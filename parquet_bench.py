"""Parquet counterpart of harp_archive_bench.py, over the same harness corpus and the same operations.

Each register becomes one Parquet file (decoded columns via harp's parse_to_dataframe, time as a "Time" column),
written with row groups of 50k rows (the analogue of the .harp time chunks) or a single row group.
Read modes:
  local        pyarrow native file path
  s3_counted   RangeFile over boto3 ranged GETs, so requests and bytes are counted
  s3_native    pyarrow.fs.S3FileSystem (its own request coalescing), timing only
"""
import argparse, io, json, subprocess, time
from pathlib import Path
from statistics import median

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from botocore.config import Config
from harp.benchmarks._registers import BENCHMARK_REGISTERS
from harp.benchmarks.generate import ensure_corpus
from harp.data import parse_to_dataframe
from pandas.testing import assert_frame_equal

import harp_archive_bench as hab

VARIANTS = {"snappy_rg50k": ("snappy", 50_000), "zstd_rg50k": ("zstd", 50_000), "zstd_rgall": ("zstd", None)}
KINDS = ["local", "s3_counted", "s3_native"]


class RangeFile(io.RawIOBase):
    """Seekable file over a hab.Backend, footer region prefetched, every other read is one ranged request."""
    def __init__(self, backend, tail_bytes=hab.TAIL):
        tail, size = backend.fetch_tail(tail_bytes)
        self.b, self.tail, self.size = backend, tail, size
        self.toff = size - len(tail); self.pos = 0
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.pos
    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        return self.pos
    def readinto(self, buf):
        n = max(0, min(len(buf), self.size - self.pos))
        if n == 0: return 0
        d = self.tail[self.pos - self.toff:self.pos - self.toff + n] if self.pos >= self.toff else bytes(self.b.read(self.pos, n))
        buf[:len(d)] = d; self.pos += len(d); return len(d)


def to_table(df, timestamped):
    """Returns (table, dataframe actually written, whether object columns had to be stringified)."""
    if timestamped: df = df.reset_index()
    try: return pa.Table.from_pandas(df, preserve_index=False), df, False
    except Exception:
        df = df.copy()
        for c in df.columns:
            if df[c].dtype == object or isinstance(df[c].dtype, pd.CategoricalDtype): df[c] = df[c].astype(str)
        return pa.Table.from_pandas(df, preserve_index=False), df, True


class PqReg:
    def __init__(self, pf, backend):
        self.pf, self.b = pf, backend
        md = pf.metadata; self.n = md.num_rows
        self.rg = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
        names = [md.row_group(0).column(j).path_in_schema for j in range(md.num_columns)]
        self.stats = None
        if "Time" in names:
            j = names.index("Time")
            self.stats = [(md.row_group(i).column(j).statistics.min, md.row_group(i).column(j).statistics.max)
                          for i in range(len(self.rg))]
    def full(self): return self.pf.read()
    def rows(self, a, b):
        out, base = [], 0
        for i, n in enumerate(self.rg):
            lo, hi = max(a, base), min(b, base + n)
            if lo < hi: out.append(self.pf.read_row_group(i).slice(lo - base, hi - lo))
            base += n
        return pa.concat_tables(out) if out else self.pf.schema_arrow.empty_table()
    def time(self, t0, t1):
        out = []
        for i in range(len(self.rg)):
            mn, mx = self.stats[i]
            if mx < t0 or mn >= t1: continue
            t = self.pf.read_row_group(i); a = t.column("Time").to_numpy(); m = (a >= t0) & (a < t1)
            if m.any(): out.append(t.filter(pa.array(m)))
        return pa.concat_tables(out) if out else self.pf.schema_arrow.empty_table()


def same(got, exp):
    try:
        assert_frame_equal(got.reset_index(drop=True), exp.reset_index(drop=True), check_dtype=False, check_categorical=False)
        return True
    except AssertionError: return False


def measure(fn, runs, backend):
    s = []
    for _ in range(runs):
        if backend: backend.reset()
        t0 = time.perf_counter(); out = fn(); s.append((time.perf_counter() - t0) * 1e3)
    return out, median(s), min(s), backend.requests if backend else None, backend.bytes if backend else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path, required=True, help="directory holding the harness corpus (generated if missing)")
    ap.add_argument("--entries", type=int, default=330_000)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--only", nargs="+")
    ap.add_argument("--stage", nargs="+", help="re-measure only these kind/variant stages (e.g. s3_native/snappy_rg50k) and merge them into the existing parquet_results.json; conversion and upload are skipped")
    ap.add_argument("--local-runs", type=int, default=5)
    ap.add_argument("--remote-runs", type=int, default=3)
    a = ap.parse_args()
    hab.BUCKET = a.bucket
    a.out.mkdir(parents=True, exist_ok=True)
    regs = [r for r in BENCHMARK_REGISTERS if r.timestamped and (not a.only or r.name in a.only)]
    for r in regs: ensure_corpus(r, a.entries, data_dir=a.corpus)

    # ---- convert
    src, info = {}, {v: {"files": {}} for v in VARIANTS}
    decode_ms = {}
    for r in regs:
        raw = (a.corpus / r.filename).read_bytes()
        t0 = time.perf_counter(); df = parse_to_dataframe(r.register, raw, time_index=r.timestamped)
        decode_ms[r.name] = (time.perf_counter() - t0) * 1e3
        tbl, written, stringified = to_table(df, r.timestamped); src[r.name] = written
        for v, (codec, rg) in VARIANTS.items():
            d = a.out / "parquet" / v; d.mkdir(parents=True, exist_ok=True)
            t0 = time.perf_counter()
            pq.write_table(tbl, d / f"{r.name}_{r.address}.parquet", compression=codec, row_group_size=rg or len(tbl),
                           write_statistics=True)
            info[v]["files"][r.name] = dict(bytes=(d / f"{r.name}_{r.address}.parquet").stat().st_size,
                                            write_s=time.perf_counter() - t0, stringified=stringified)
        print("converted", r.name, f"decode {decode_ms[r.name]:.0f} ms", flush=True)
    raw_total = sum((a.corpus / r.filename).stat().st_size for r in regs)
    for v in VARIANTS: info[v]["bytes"] = sum(f["bytes"] for f in info[v]["files"].values())

    # ---- upload
    s3 = boto3.client("s3", region_name=a.region, config=Config(max_pool_connections=16, retries={"max_attempts": 3}))
    pfx = f"{a.prefix.strip('/')}/{a.run_id}"
    for v in ([] if a.stage else VARIANTS):
        t, rc, err = hab.sh(["aws", "s3", "cp", str(a.out / "parquet" / v), f"s3://{a.bucket}/{pfx}/parquet/{v}/",
                             "--recursive", "--only-show-errors"])
        info[v].update(upload_s=t, upload_rc=rc, upload_err=err, upload_objects=len(regs)); print("upload", v, t, rc, err, flush=True)

    cr = boto3.Session().get_credentials().get_frozen_credentials()
    fs = pafs.S3FileSystem(access_key=cr.access_key, secret_key=cr.secret_key, session_token=cr.token, region=a.region)

    def open_reg(kind, v, r):
        fn = f"{r.name}_{r.address}.parquet"
        if kind == "local": return PqReg(pq.ParquetFile(a.out / "parquet" / v / fn), None)
        key = f"{pfx}/parquet/{v}/{fn}"
        if kind == "s3_counted":
            b = hab.S3Backend(s3, key); return PqReg(pq.ParquetFile(RangeFile(b), pre_buffer=True), b)
        return PqReg(pq.ParquetFile(f"{a.bucket}/{key}", filesystem=fs, pre_buffer=True), None)

    log, opens = [], {}
    s3.head_object(Bucket=a.bucket, Key=f"{pfx}/parquet/{next(iter(VARIANTS))}/{regs[0].name}_{regs[0].address}.parquet")
    for kind in KINDS:
        for v in VARIANTS:
            if a.stage and f"{kind}/{v}" not in a.stage: continue
            t_open, req_open = [], 0
            t0_all = time.perf_counter()
            for r in regs:
                runs_open = []
                for _ in range(3 if kind != "local" else 5):
                    t0 = time.perf_counter(); pr = open_reg(kind, v, r); runs_open.append((time.perf_counter() - t0) * 1e3)
                t_open.append(median(runs_open)); req_open += pr.b.requests if pr.b else 0
            opens[f"{kind}/{v}"] = dict(ms_per_register=median(t_open), ms_all=sum(t_open), requests_all=req_open if kind == "s3_counted" else None)
            runs = dict(full=a.local_runs if kind == "local" else 1, op=a.local_runs if kind == "local" else a.remote_runs)
            for r in regs:
                pr = open_reg(kind, v, r); df = src[r.name]; n = len(df)
                ops = [("full", lambda: pr.full(), lambda o: o.num_rows == n, runs["full"]),
                       ("full_df", lambda: pr.full().to_pandas(), lambda o: same(o, df), runs["full"]),
                       ("slice", lambda: pr.rows(100_000, 101_000).to_pandas(), lambda o: same(o, df.iloc[100_000:101_000]), runs["op"]),
                       ("slice_x", lambda: pr.rows(49_500, 50_500).to_pandas(), lambda o: same(o, df.iloc[49_500:50_500]), runs["op"])]
                if r.timestamped:
                    t0, t1 = float(df["Time"].iloc[200_000]), float(df["Time"].iloc[201_000])
                    ops.append(("timeq", lambda: pr.time(t0, t1).to_pandas(), lambda o: same(o, df.iloc[200_000:201_000]), runs["op"]))
                for name, fn, check, nr in ops:
                    try:
                        out, med, mn, req, byt = measure(fn, nr, pr.b)
                        ok, err = bool(check(out)), None
                    except Exception as e:
                        med = mn = req = byt = None; ok = False; err = f"{type(e).__name__}: {e}"
                    log.append(dict(loc=kind, layout=v, reg=r.name, timestamped=r.timestamped, op=name, ms=med, ms_min=mn,
                                    requests=req, bytes=byt, ok=ok, err=err))
            print(f"measured {kind}/{v} in {time.perf_counter() - t0_all:.0f}s", flush=True)

    res_path = a.out / "parquet_results.json"
    if a.stage:
        old = json.loads(res_path.read_text())
        redo = {tuple(x.split("/")) for x in a.stage}
        old["log"] = [r for r in old["log"] if (r["loc"], r["layout"]) not in redo] + log
        old["opens"].update(opens)
        old["remeasured"] = sorted(set(old.get("remeasured", [])) | set(a.stage))
        res_path.write_text(json.dumps(old, indent=1, default=str)); print("merged", a.stage, flush=True); return
    res_path.write_text(json.dumps(
        dict(info=info, opens=opens, log=log, decode_ms=decode_ms, raw_bytes=raw_total, entries=a.entries,
             prefix=f"s3://{a.bucket}/{pfx}/parquet/"), indent=1, default=str))
    print("done", flush=True)


if __name__ == "__main__":
    main()
