"""Use-case benchmarks over data already uploaded by harp_archive_bench.py and parquet_bench.py.

Every use case is measured cold: opening the recording is part of the cost.

  uc1_window_all    all events between two times, from every register (untimestamped registers cannot take part)
  uc1_window_empty  the same query for a window after the end of the recording
  uc2_full5         load 5 whole registers (DigitalInputs, AnalogData, Counter0, PulseDO0, StartPulse) into DataFrames
  uc3_bounds_meta   earliest and latest timestamp in the recording, leaning on metadata where the format has it
  uc3_bounds_msgs   the same, plus fetching the earliest and the latest message itself

Modes: seq (one request at a time), par (8 threads, S3 only).
"""
import argparse, json, os, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median

import boto3
import numpy as np
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from botocore.config import Config
from harp.benchmarks._registers import BENCHMARK_REGISTERS
from harp.data import parse_to_dataframe

import harp_archive_bench as hab
import parquet_bench as pb

FIVE = ["DigitalInputs", "AnalogData", "Counter0", "PulseDO0", "StartPulse"]
WINDOW = (200_000, 201_000)      # rows 200000..200999 on the harness's 1 row/second ramp
EMPTY_WINDOW = (400_000.0, 401_000.0)   # the recording ends at 329,999 s


def pmap(fn, items, par):
    if not par: return [fn(i) for i in items]
    with ThreadPoolExecutor(8) as ex: return list(ex.map(fn, items))


# ------------------------------------------------------------------ .harp (folder and archive) use cases

def key(r): return f"{r.name}_{r.address}"


def h_window(store, regs, t0, t1, par):
    def one(r):
        ch = store.chunks[key(r)]
        for c in ch: c.ensure_meta()
        if not all(c.timestamped for c in ch): return None
        return hab.op_time(r, ch, t0, t1)
    return dict(zip([r.name for r in regs], pmap(one, regs, par)))


def h_full5(store, regs, par):
    def one(r):
        ch = store.chunks[key(r)]
        bufs = [c.read(0, c.nbytes) for c in ch]
        buf = bufs[0] if len(bufs) == 1 else b"".join(bytes(b) for b in bufs)
        parse_to_dataframe(r.register, buf, time_index=True)
        return buf
    return dict(zip([r.name for r in regs], pmap(one, regs, par)))


def h_bounds(store, regs, par, msgs):
    def one(r):
        ch = store.chunks[key(r)]
        for c in ch: c.ensure_meta()
        return [c for c in ch if c.tmin is not None]
    chunks = [c for cl in pmap(one, regs, par) for c in cl]
    cmin, cmax = min(chunks, key=lambda c: c.tmin), max(chunks, key=lambda c: c.tmax)
    if not msgs: return cmin.tmin, cmax.tmax, None, None
    first, last = pmap(lambda f: f(), [lambda: bytes(cmin.read(0, cmin.stride)),
                                        lambda: bytes(cmax.read((cmax.rows - 1) * cmax.stride, cmax.stride))], par)
    return cmin.tmin, cmax.tmax, first, last


# ------------------------------------------------------------------ Parquet use cases

class PqDir:
    """A directory or prefix of per-register Parquet files, opened lazily."""
    def __init__(self, kind, variant, ctx):
        self.kind, self.v, self.ctx, self.regs, self.listing = kind, variant, ctx, {}, 0
        if kind == "local": self.names = set(os.listdir(ctx["pq_dir"] / variant))
        else:
            self.names = set()
            pre = f"{ctx['pfx']}/parquet/{variant}/"
            for page in ctx["s3"].get_paginator("list_objects_v2").paginate(Bucket=ctx["bucket"], Prefix=pre):
                self.listing += 1; self.names |= {o["Key"][len(pre):] for o in page.get("Contents", [])}
    def get(self, r):
        if r.name not in self.regs:
            fn = f"{key(r)}.parquet"; assert fn in self.names
            c = self.ctx
            if self.kind == "local": self.regs[r.name] = pb.PqReg(pq.ParquetFile(c["pq_dir"] / self.v / fn), None)
            elif self.kind == "s3_counted":
                b = hab.S3Backend(c["s3"], f"{c['pfx']}/parquet/{self.v}/{fn}")
                self.regs[r.name] = pb.PqReg(pq.ParquetFile(pb.RangeFile(b), pre_buffer=True), b)
            else:
                self.regs[r.name] = pb.PqReg(pq.ParquetFile(f"{c['bucket']}/{c['pfx']}/parquet/{self.v}/{fn}",
                                                            filesystem=c["fs"], pre_buffer=True), None)
        return self.regs[r.name]
    def counters(self):
        if self.kind != "s3_counted": return None, None
        bs = [r.b for r in self.regs.values()]
        return self.listing + sum(b.requests for b in bs), sum(b.bytes for b in bs)


def p_window(d, regs, t0, t1, par):
    def one(r):
        pr = d.get(r)
        return None if pr.stats is None else pr.time(t0, t1).to_pandas()
    return dict(zip([r.name for r in regs], pmap(one, regs, par)))


def p_full5(d, regs, par):
    return dict(zip([r.name for r in regs], pmap(lambda r: d.get(r).full().to_pandas(), regs, par)))


def p_bounds(d, regs, par, msgs):
    def one(r):
        pr = d.get(r)
        if pr.stats is None: return None
        return r, pr, min(s[0] for s in pr.stats), max(s[1] for s in pr.stats)
    info = [x for x in pmap(one, regs, par) if x]
    rmin, rmax = min(info, key=lambda x: x[2]), max(info, key=lambda x: x[3])
    if not msgs: return rmin[2], rmax[3], None, None
    def first(): return float(rmin[1].pf.read_row_group(0).slice(0, 1).column("Time")[0].as_py())
    def last(): return float(rmax[1].pf.read_row_group(len(rmax[1].rg) - 1).slice(rmax[1].rg[-1] - 1, 1).column("Time")[0].as_py())
    f, l = pmap(lambda fn: fn(), [first, last], par)
    return rmin[2], rmax[3], f, l


# ------------------------------------------------------------------ verification

def load_truth(corpus, regs):
    t = {}
    for r in regs:
        raw = (corpus / r.filename).read_bytes(); stride = raw[1] + 2; rows = len(raw) // stride; ts = None
        if r.timestamped:
            a = np.frombuffer(raw, np.uint8)
            ts = (np.ndarray(rows, "<u4", buffer=a, offset=5, strides=stride)
                  + np.ndarray(rows, "<u2", buffer=a, offset=9, strides=stride) * hab.TICK)
        t[r.name] = dict(raw=raw, stride=stride, rows=rows, ts=ts)
    return t


def make_checks(truth, regs):
    a, b = WINDOW
    tsregs = [r for r in regs if r.timestamped]
    gmin = min(truth[r.name]["ts"][0] for r in tsregs); gmax = max(truth[r.name]["ts"][-1] for r in tsregs)

    def window(out, harp, empty):
        for r in regs:
            o = out[r.name]
            if not r.timestamped:
                if o is not None: return False
                continue
            t = truth[r.name]; st = t["stride"]
            if empty:
                if harp and len(o) != 0: return False
                if not harp and len(o) != 0: return False
            elif harp:
                if bytes(o) != t["raw"][a * st:b * st]: return False
            elif len(o) != b - a or float(o["Time"].iloc[0]) != float(t["ts"][a]): return False
        return True

    def full5(out, harp):
        for r in regs:
            t = truth[r.name]
            if harp and bytes(out[r.name]) != t["raw"]: return False
            if not harp and len(out[r.name]) != t["rows"]: return False
        return True

    def bounds(out, harp, msgs):
        lo, hi, first, last = out
        if (float(lo), float(hi)) != (float(gmin), float(gmax)): return False
        if not msgs: return True
        if harp: return hab.ts_of(first) == lo and hab.ts_of(last) == hi
        return first == lo and last == hi
    return window, full5, bounds


# ------------------------------------------------------------------ main

def counters_harp(store):
    r, b = store.counters()
    return r + (store.open_info["requests"] if store.label == "folder" else 0), b


def close_harp(store):
    for b in store.backends:
        if hasattr(b, "f"):
            try:
                if b.mm is not None: b.mm.close()
            except Exception: pass
            try: b.f.close()
            except Exception: pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--harp-dir", type=Path, required=True, help="out dir of harp_archive_bench.py (holds folder/ and *.harp)")
    ap.add_argument("--parquet-dir", type=Path, required=True, help="<out>/parquet from parquet_bench.py")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--local-runs", type=int, default=3)
    ap.add_argument("--remote-runs", type=int, default=2)
    ap.add_argument("--only-uc", nargs="+")
    ap.add_argument("--only-layout", nargs="+", help="restrict to these layouts, e.g. deflate1_chunked parquet_zstd_rg50k")
    a = ap.parse_args()
    hab.BUCKET = a.bucket
    a.out.mkdir(parents=True, exist_ok=True)
    regs = list(BENCHMARK_REGISTERS)
    five = [r for r in regs if r.name in FIVE]
    truth = load_truth(a.corpus, regs)
    c_window, c_full5, c_bounds = make_checks(truth, regs)
    s3 = boto3.client("s3", region_name=a.region, config=Config(max_pool_connections=32, retries={"max_attempts": 3}))
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    fs = pafs.S3FileSystem(access_key=cr.access_key, secret_key=cr.secret_key, session_token=cr.token, region=a.region)
    pfx = f"{a.prefix.strip('/')}/{a.run_id}"
    ctx = dict(s3=s3, fs=fs, bucket=a.bucket, pfx=pfx, pq_dir=a.parquet_dir)
    s3.head_object(Bucket=a.bucket, Key=f"{pfx}/stored.harp")

    t0w, t1w = float(truth[FIVE[0]]["ts"][WINDOW[0]]), float(truth[FIVE[0]]["ts"][WINDOW[1]])
    UCS = {
        "uc1_window_all": (lambda h: (lambda s, par: h_window(s, regs, t0w, t1w, par)) if h else (lambda s, par: p_window(s, regs, t0w, t1w, par)),
                           lambda o, h: c_window(o, h, False)),
        "uc1_window_empty": (lambda h: (lambda s, par: h_window(s, regs, *EMPTY_WINDOW, par)) if h else (lambda s, par: p_window(s, regs, *EMPTY_WINDOW, par)),
                             lambda o, h: c_window(o, h, True)),
        "uc2_full5": (lambda h: (lambda s, par: h_full5(s, five, par)) if h else (lambda s, par: p_full5(s, five, par)),
                      lambda o, h: _full5(o, five, truth, h)),
        "uc3_bounds_meta": (lambda h: (lambda s, par: h_bounds(s, regs, par, False)) if h else (lambda s, par: p_bounds(s, regs, par, False)),
                            lambda o, h: c_bounds(o, h, False)),
        "uc3_bounds_msgs": (lambda h: (lambda s, par: h_bounds(s, regs, par, True)) if h else (lambda s, par: p_bounds(s, regs, par, True)),
                            lambda o, h: c_bounds(o, h, True)),
    }
    log = []

    def run(loc, layout, harp, open_fn, counters, close):
        if a.only_layout and layout not in a.only_layout: return
        remote = loc != "local"
        for uc, (mk, check) in UCS.items():
            if a.only_uc and uc not in a.only_uc: continue
            for mode in (["seq", "par"] if remote else ["seq"]):
                op = mk(harp); samples = []; req = byt = None; ok = False; err = None
                try:
                    for _ in range(a.remote_runs if remote else a.local_runs):
                        t0 = time.perf_counter(); store = open_fn(); out = op(store, mode == "par")
                        samples.append((time.perf_counter() - t0) * 1e3)
                        req, byt = counters(store)
                        ok = bool(check(out, harp)); close(store)
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                log.append(dict(loc=loc, layout=layout, uc=uc, mode=mode, ms=median(samples) if samples else None,
                                ms_min=min(samples) if samples else None, requests=req, bytes=byt, ok=ok, err=err))
        print("done", loc, layout, flush=True)

    folder_dir = a.harp_dir / "folder"
    for layout in ["folder", "stored", "deflate1", "stored_chunked", "deflate1_chunked"]:
        for loc in ["local", "s3"]:
            if layout == "folder":
                of = (lambda: hab.open_folder_local(str(folder_dir))) if loc == "local" else (lambda: hab.open_folder_s3(s3, f"{pfx}/folder/"))
            elif loc == "local":
                of = lambda L=layout: hab.open_archive(hab.LocalBackend(str(a.harp_dir / f"{L}.harp"), use_mmap=not L.startswith("deflate")))
            else:
                of = lambda L=layout: hab.open_archive(hab.S3Backend(s3, f"{pfx}/{L}.harp"))
            run(loc, layout, True, of, counters_harp, close_harp)
    for v in pb.VARIANTS:
        for kind in pb.KINDS:
            run(kind, "parquet_" + v, False, lambda k=kind, vv=v: PqDir(k, vv, ctx), lambda d: d.counters(), lambda d: None)

    (a.out / "usecases_results.json").write_text(json.dumps(dict(log=log, window=[t0w, t1w], empty_window=EMPTY_WINDOW, five=FIVE), indent=1, default=str))
    print("done all", flush=True)


def _full5(o, five, truth, harp):
    for r in five:
        t = truth[r.name]
        if harp and bytes(o[r.name]) != t["raw"]: return False
        if not harp and len(o[r.name]) != t["rows"]: return False
    return True


if __name__ == "__main__":
    main()
