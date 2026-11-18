"""Benchmark of the issue #251 packed .harp layouts, local vs S3, over every harness register.

Built on harp.benchmarks (corpus generation + parse_bulk / parse_to_dataframe).
Readers use one code path for both locations; only the byte backend differs:
  LocalBackend (seek/read or mmap)  |  S3Backend (boto3 ranged GetObject)  |  HttpBackend (presigned URL).
"""
import argparse, io, json, mmap, os, re, struct, subprocess, sys, threading, time, urllib.request, zipfile, zlib
from pathlib import Path
from statistics import median

import boto3
import numpy as np
from botocore.config import Config
from harp.benchmarks._registers import BENCHMARK_REGISTERS
from harp.benchmarks.generate import ensure_corpus
from harp.data import parse_to_dataframe
from harp.protocol._register import _TIMESTAMP_FLAG

TICK = 32e-6
PAGE = 64 * 1024          # probe page for bisection on remote stored members
TAIL = 256 * 1024         # tail prefetch for the central directory
NAME_RE = re.compile(r"^(?P<base>.+?_\d+)(?:_(?P<k>\d+))?\.bin$")
BUCKET = None       # set from --bucket
ROOT_PREFIX = None  # set from --prefix

# ---------------------------------------------------------------- backends

class Backend:
    local = False
    def __init__(self): self.requests = 0; self.bytes = 0; self._lock = threading.Lock()
    def reset(self): self.requests = 0; self.bytes = 0
    def _add(self, n):
        with self._lock: self.requests += 1; self.bytes += n

class LocalBackend(Backend):
    local = True
    def __init__(self, path, use_mmap):
        super().__init__()
        self.size = os.path.getsize(path)
        self.f = open(path, "rb")
        self.mm = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ) if use_mmap and self.size else None
    def read(self, off, n):
        self._add(n)
        if self.mm is not None: return memoryview(self.mm)[off:off + n]
        self.f.seek(off); return self.f.read(n)
    def fetch_tail(self, n):
        n = min(n, self.size); return bytes(self.read(self.size - n, n)), self.size

class S3Backend(Backend):
    def __init__(self, client, key, size=None):
        super().__init__(); self.c = client; self.key = key; self.size = size
    def read(self, off, n):
        r = self.c.get_object(Bucket=BUCKET, Key=self.key, Range=f"bytes={off}-{off + n - 1}")
        d = r["Body"].read(); self._add(len(d)); return d
    def fetch_tail(self, n):
        r = self.c.get_object(Bucket=BUCKET, Key=self.key, Range=f"bytes=-{n}")
        d = r["Body"].read(); self._add(len(d))
        self.size = int(r["ContentRange"].split("/")[1]); return d, self.size

class HttpBackend(Backend):
    def __init__(self, url): super().__init__(); self.url = url; self.size = None
    def _get(self, rng):
        req = urllib.request.Request(self.url, headers={"Range": rng})
        with urllib.request.urlopen(req) as r:
            d = r.read(); cr = r.headers.get("Content-Range")
        self._add(len(d)); return d, cr
    def read(self, off, n): return self._get(f"bytes={off}-{off + n - 1}")[0]
    def fetch_tail(self, n):
        d, cr = self._get(f"bytes=-{n}"); self.size = int(cr.split("/")[1]); return d, self.size

class BackendFile:
    """Minimal seekable file over a Backend with the archive tail prefetched."""
    def __init__(self, b, tail, size):
        self.b = b; self.tail = tail; self.size = size; self.toff = size - len(tail); self.pos = 0
    def seekable(self): return True
    def tell(self): return self.pos
    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        return self.pos
    def read(self, n=-1):
        if n is None or n < 0: n = self.size - self.pos
        n = max(0, min(n, self.size - self.pos))
        if n == 0: return b""
        if self.pos >= self.toff: d = self.tail[self.pos - self.toff:self.pos - self.toff + n]
        else: d = bytes(self.b.read(self.pos, n))
        self.pos += len(d); return d
    def close(self): pass

# ---------------------------------------------------------------- chunks and ops

def ts_of(frame11):
    s, us = struct.unpack("<IH", bytes(frame11[5:11])); return s + us * TICK

class Chunk:
    def __init__(self, bk, name, nbytes, *, zi=None, meta=None, folder=False):
        self.bk, self.name, self.nbytes, self.zi, self.folder = bk, name, nbytes, zi, folder
        self.compressed = bool(zi and zi.compress_type == zipfile.ZIP_DEFLATED)
        self.from_comment = meta is not None
        self._meta0 = meta
        self.reset()
    def reset(self):
        m = self._meta0
        self.stride = m["stride"] if m else None
        self.rows = m["rows"] if m else None
        self.tmin = m["tmin"] if m else None
        self.tmax = m["tmax"] if m else None
        self.monotonic = m["monotonic"] if m else None
        self.timestamped = (m["tmin"] is not None) if m else None
        self.base_off = 0 if self.folder else None
        self.cache = None; self.pages = {}
    def _resolve(self):
        if self.base_off is None:
            zi = self.zi
            hdr = bytes(self.bk.read(zi.header_offset, 30 + len(zi.filename.encode()) + 64))
            fn, ex = struct.unpack("<HH", hdr[26:30]); self.base_off = zi.header_offset + 30 + fn + ex
        return self.base_off
    def read(self, off, n):
        n = min(n, self.nbytes - off)
        if self.compressed:
            if self.cache is None:
                raw = self.bk.read(self._resolve(), self.zi.compress_size)
                self.cache = zlib.decompressobj(-15).decompress(bytes(raw))
            return self.cache[off:off + n]
        return self.bk.read(self._resolve() + off, n)
    def small(self, off, n):
        if self.compressed or self.bk.local: return bytes(self.read(off, n))
        out = b""
        while n > 0:
            p = off // PAGE
            if p not in self.pages: self.pages[p] = bytes(self.read(p * PAGE, PAGE))
            pg = self.pages[p]; a = off - p * PAGE; take = min(n, len(pg) - a)
            out += pg[a:a + take]; off += take; n -= take
        return out
    def ensure_stride(self):
        if self.stride is None:
            self.stride = bytes(self.read(0, 2))[1] + 2; self.rows = self.nbytes // self.stride
    def ensure_meta(self):
        if self.tmin is None and not self.from_comment:
            self.ensure_stride()
            first = bytes(self.read(0, 11)); last = bytes(self.read((self.rows - 1) * self.stride, 11))
            self.timestamped = bool(first[4] & _TIMESTAMP_FLAG)
            if self.timestamped: self.tmin, self.tmax = ts_of(first), ts_of(last)
    def ts_at(self, row): return ts_of(b"\0" * 5 + self.small(row * self.stride + 5, 6))

def lower_bound(c, t, lo=0):
    hi = c.rows
    while lo < hi:
        mid = (lo + hi) // 2
        if c.ts_at(mid) < t: lo = mid + 1
        else: hi = mid
    return lo

def op_layout(reg, chunks):
    for c in chunks: c.ensure_meta()
    return sum(c.rows for c in chunks)

def op_full(reg, chunks):
    bufs = [c.read(0, c.nbytes) for c in chunks]
    buf = bufs[0] if len(bufs) == 1 else b"".join(bytes(b) for b in bufs)
    _, ts, _, _ = reg.register.parse_bulk(buf, parse_timestamp=True)
    if ts is not None: np.asarray(ts)
    else: int(np.frombuffer(buf, np.uint8)[::4096].sum())
    return buf

def _rows(chunks, a, b):
    for c in chunks: c.ensure_stride()
    out, base = [], 0
    for c in chunks:
        lo, hi = max(a, base), min(b, base + c.rows)
        if lo < hi: out.append(bytes(c.read((lo - base) * c.stride, (hi - lo) * c.stride)))
        base += c.rows
    return b"".join(out)

def op_slice(reg, chunks, a=100_000, b=101_000):
    buf = _rows(chunks, a, b)
    parse_to_dataframe(reg.register, buf, time_index=reg.timestamped)
    return buf

def op_slice_x(reg, chunks): return op_slice(reg, chunks, 49_500, 50_500)

def op_time(reg, chunks, t0, t1, parse=True):
    for c in chunks: c.ensure_meta()
    out = []
    for c in chunks:
        if c.monotonic:
            if c.tmax < t0 or c.tmin >= t1: continue
            lo = lower_bound(c, t0); hi = lower_bound(c, t1, lo)
            if lo < hi: out.append(bytes(c.read(lo * c.stride, (hi - lo) * c.stride)))
        else:  # no metadata guarantee: every timestamp must be checked
            data = np.frombuffer(bytes(c.read(0, c.nbytes)), np.uint8)
            s = np.ndarray(c.rows, "<u4", buffer=data, offset=5, strides=c.stride)
            u = np.ndarray(c.rows, "<u2", buffer=data, offset=9, strides=c.stride)
            t = s + u * TICK; idx = np.nonzero((t >= t0) & (t < t1))[0]
            if len(idx): out.append(data.reshape(-1, c.stride)[idx].tobytes())
    buf = b"".join(out)
    if parse and buf: parse_to_dataframe(reg.register, buf, time_index=True)
    return buf

# ---------------------------------------------------------------- stores

class Store:
    def __init__(self, label, backends, chunks, open_info):
        self.label, self.backends, self.chunks, self.open_info = label, backends, chunks, open_info
    def reset(self):
        for bk in self.backends: bk.reset()
        for cl in self.chunks.values():
            for c in cl: c.reset()
    def counters(self): return sum(b.requests for b in self.backends), sum(b.bytes for b in self.backends)

def open_archive(backend):
    t0 = time.perf_counter()
    tail, size = backend.fetch_tail(TAIL)
    zf = zipfile.ZipFile(BackendFile(backend, tail, size))
    chunks = {}
    for zi in zf.infolist():
        m = NAME_RE.match(zi.filename)
        if not m: continue
        meta = json.loads(zi.comment) if zi.comment else None
        chunks.setdefault(m["base"], []).append((int(m["k"] or -1), Chunk(backend, zi.filename, zi.file_size, zi=zi, meta=meta)))
    chunks = {k: [c for _, c in sorted(v, key=lambda x: x[0])] for k, v in chunks.items()}
    return Store("archive", [backend], chunks, dict(ms=(time.perf_counter() - t0) * 1e3, requests=backend.requests,
                 bytes=backend.bytes, members=len(zf.infolist()), version=json.loads(zf.comment)))

def open_folder_local(path):
    t0 = time.perf_counter(); bks, chunks = [], {}
    for fn in sorted(os.listdir(path)):
        m = NAME_RE.match(fn)
        if not m: continue
        b = LocalBackend(os.path.join(path, fn), use_mmap=False); bks.append(b)
        chunks.setdefault(m["base"], []).append(Chunk(b, fn, b.size, folder=True))
    return Store("folder", bks, chunks, dict(ms=(time.perf_counter() - t0) * 1e3, requests=1, bytes=0, members=len(bks)))

def open_folder_s3(client, prefix):
    t0 = time.perf_counter(); bks, chunks, reqs = [], {}, 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        reqs += 1
        for o in page.get("Contents", []):
            fn = o["Key"].rsplit("/", 1)[1]; m = NAME_RE.match(fn)
            if not m: continue
            b = S3Backend(client, o["Key"], o["Size"]); bks.append(b)
            chunks.setdefault(m["base"], []).append(Chunk(b, fn, o["Size"], folder=True))
    return Store("folder", bks, chunks, dict(ms=(time.perf_counter() - t0) * 1e3, requests=reqs, bytes=0, members=len(bks)))

# ---------------------------------------------------------------- packing

def frames_meta(buf, stride, timestamped):
    rows = len(buf) // stride
    m = dict(stride=stride, rows=rows, tmin=None, tmax=None, monotonic=None)
    if timestamped:
        a = np.frombuffer(buf, np.uint8)
        t = (np.ndarray(rows, "<u4", buffer=a, offset=5, strides=stride)
             + np.ndarray(rows, "<u2", buffer=a, offset=9, strides=stride) * TICK)
        m.update(tmin=float(t.min()), tmax=float(t.max()), monotonic=bool(np.all(np.diff(t) >= 0)))
    return m

def pack(path, regs, raws, compress, level, chunk_rows):
    t0 = time.perf_counter()
    with zipfile.ZipFile(path, "w", allowZip64=True) as zf:
        zf.comment = json.dumps({"harp": 1}, separators=(",", ":")).encode()
        zf.writestr(zipfile.ZipInfo("device.yml", (2026, 1, 1, 0, 0, 0)), "# synthetic benchmark device\n", zipfile.ZIP_STORED)
        for reg in regs:
            raw = raws[reg.name]; stride = raw[1] + 2
            step = (chunk_rows or len(raw) // stride) * stride
            for k, off in enumerate(range(0, len(raw), step)):
                piece = raw[off:off + step]
                name = f"{reg.name}_{reg.address}" + (f"_{k:03d}" if chunk_rows else "") + ".bin"
                zi = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
                zi.comment = json.dumps(frames_meta(piece, stride, reg.timestamped), separators=(",", ":")).encode()
                zf.writestr(zi, piece, compress_type=compress, compresslevel=level if compress == zipfile.ZIP_DEFLATED else None)
    pack_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    with zipfile.ZipFile(path) as zf: bad = zf.testzip()
    return dict(pack_s=pack_s, verify_s=time.perf_counter() - t0, bad=bad, bytes=os.path.getsize(path))

# ---------------------------------------------------------------- measurement

def measure(store, fn, runs):
    samples = []
    for _ in range(runs):
        store.reset(); t0 = time.perf_counter(); out = fn(); samples.append((time.perf_counter() - t0) * 1e3)
        req, byt = store.counters()
    return out, median(samples), min(samples), req, byt

def run_layout(loc, layout, store, regs, raws, truth, runs, rows, log):
    for reg in regs:
        base = f"{reg.name}_{reg.address}"; chunks = store.chunks[base]; raw = raws[reg.name]
        stride, n = truth[reg.name]["stride"], truth[reg.name]["rows"]; ts = truth[reg.name]["ts"]
        ops = [("layout", lambda: op_layout(reg, chunks), lambda o: o == n, 1),
               ("full", lambda: op_full(reg, chunks), lambda o: bytes(o) == raw, runs["full"]),
               ("slice", lambda: op_slice(reg, chunks), lambda o: o == raw[100_000 * stride:101_000 * stride], runs["op"]),
               ("slice_x", lambda: op_slice_x(reg, chunks), lambda o: o == raw[49_500 * stride:50_500 * stride], runs["op"])]
        if reg.timestamped:
            t0, t1 = float(ts[200_000]), float(ts[201_000])
            ops.append(("timeq", lambda: op_time(reg, chunks, t0, t1), lambda o: o == raw[200_000 * stride:201_000 * stride], runs["op"]))
        for name, fn, check, r in ops:
            try:
                out, med, mn, req, byt = measure(store, fn, r if name != "layout" else 3)
                ok = bool(check(out)); err = None
            except Exception as e:  # record, do not abort
                med = mn = req = byt = None; ok = False; err = f"{type(e).__name__}: {e}"
            log.append(dict(loc=loc, layout=layout, reg=reg.name, timestamped=reg.timestamped, op=name, ms=med, ms_min=mn,
                            requests=req, bytes=byt, ok=ok, err=err))

def sh(cmd):
    t0 = time.perf_counter(); r = subprocess.run(cmd, capture_output=True, text=True)
    return time.perf_counter() - t0, r.returncode, (r.stderr or "")[-300:]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entries", type=int, default=330_000)
    ap.add_argument("--chunk-rows", type=int, default=50_000)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True, help="key prefix under which <run-id>/ is written")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--only", nargs="+")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--local-runs", type=int, default=5)
    ap.add_argument("--remote-runs", type=int, default=3)
    a = ap.parse_args()
    global BUCKET, ROOT_PREFIX
    BUCKET, ROOT_PREFIX = a.bucket, a.prefix.strip("/")
    a.out.mkdir(parents=True, exist_ok=True)
    regs = [r for r in BENCHMARK_REGISTERS if r.timestamped and (not a.only or r.name in a.only)]  # issue #251: every message carries a timestamp
    corpus = a.out / "corpus"
    for r in regs: ensure_corpus(r, a.entries, data_dir=corpus)
    raws = {r.name: (corpus / r.filename).read_bytes() for r in regs}
    truth = {}
    for r in regs:
        raw = raws[r.name]; stride = raw[1] + 2; rows = len(raw) // stride; ts = None
        if r.timestamped:
            arr = np.frombuffer(raw, np.uint8)
            ts = (np.ndarray(rows, "<u4", buffer=arr, offset=5, strides=stride)
                  + np.ndarray(rows, "<u2", buffer=arr, offset=9, strides=stride) * TICK)
        truth[r.name] = dict(stride=stride, rows=rows, ts=ts)
    print(f"corpus: {len(regs)} registers, {sum(len(v) for v in raws.values()) / 1e6:.0f} MB raw", flush=True)

    # ---- build layouts locally
    layouts = {"stored": (zipfile.ZIP_STORED, None, None), "deflate1": (zipfile.ZIP_DEFLATED, 1, None),
               "stored_chunked": (zipfile.ZIP_STORED, None, a.chunk_rows), "deflate1_chunked": (zipfile.ZIP_DEFLATED, 1, a.chunk_rows)}
    folder_dir = a.out / "folder"; folder_dir.mkdir(exist_ok=True)
    for r in regs: (folder_dir / r.filename).write_bytes(raws[r.name])
    info = {"folder": dict(bytes=sum(len(v) for v in raws.values()), pack_s=0.0, verify_s=0.0)}
    for name, (ct, lv, ch) in layouts.items():
        info[name] = pack(a.out / f"{name}.harp", regs, raws, ct, lv, ch); print("packed", name, info[name], flush=True)

    # ---- upload
    s3 = boto3.client("s3", region_name=a.region, config=Config(max_pool_connections=16, retries={"max_attempts": 3}))
    pfx = f"{ROOT_PREFIX}/{a.run_id}"
    if not a.no_upload:
        t, rc, err = sh(["aws", "s3", "cp", str(folder_dir), f"s3://{BUCKET}/{pfx}/folder/", "--recursive", "--only-show-errors"])
        info["folder"].update(upload_s=t, upload_rc=rc, upload_err=err, upload_objects=len(regs)); print("upload folder", t, rc, err, flush=True)
        for name in layouts:
            t, rc, err = sh(["aws", "s3", "cp", str(a.out / f"{name}.harp"), f"s3://{BUCKET}/{pfx}/{name}.harp", "--only-show-errors"])
            info[name].update(upload_s=t, upload_rc=rc, upload_err=err, upload_objects=1); print("upload", name, t, rc, err, flush=True)

    # ---- open stores, measure
    log, opens, extras = [], {}, {}
    s3.head_object(Bucket=BUCKET, Key=f"{pfx}/stored.harp")  # warm connection
    for layout in ["folder", *layouts]:
        for loc in ["local", "s3"]:
            if layout == "folder":
                mk = (lambda: open_folder_local(str(folder_dir))) if loc == "local" else (lambda: open_folder_s3(s3, f"{pfx}/folder/"))
            elif loc == "local":
                mk = lambda: open_archive(LocalBackend(str(a.out / f"{layout}.harp"), use_mmap=not layouts[layout][0] == zipfile.ZIP_DEFLATED))
            else:
                mk = lambda: open_archive(S3Backend(s3, f"{pfx}/{layout}.harp"))
            samples = [mk().open_info for _ in range(5 if loc == "local" else 3)]
            store = mk(); opens[f"{loc}/{layout}"] = dict(ms=median(s["ms"] for s in samples), requests=samples[-1]["requests"],
                                                         bytes=samples[-1]["bytes"], members=samples[-1]["members"])
            runs = dict(full=a.local_runs if loc == "local" else 1, op=a.local_runs if loc == "local" else a.remote_runs)
            t0 = time.perf_counter()
            run_layout(loc, layout, store, regs, raws, truth, runs, a.entries, log)
            print(f"measured {loc}/{layout} in {time.perf_counter() - t0:.0f}s", flush=True)

    # ---- tool checks (remote): presigned URL, s3fs, standard zip tools
    probe = regs[0]; base = f"{probe.name}_{probe.address}"; stride = truth[probe.name]["stride"]
    want = raws[probe.name][100_000 * stride:101_000 * stride]
    try:
        url = s3.generate_presigned_url("get_object", Params=dict(Bucket=BUCKET, Key=f"{pfx}/stored.harp"), ExpiresIn=900)
        t0 = time.perf_counter(); st = open_archive(HttpBackend(url)); got = _rows(st.chunks[base], 100_000, 101_000)
        extras["presigned_http"] = dict(ok=got == want, ms=(time.perf_counter() - t0) * 1e3, requests=st.backends[0].requests)
    except Exception as e: extras["presigned_http"] = dict(ok=False, err=f"{type(e).__name__}: {e}")
    try:
        import s3fs
        for ct in ["readahead", "none"]:
            fs = s3fs.S3FileSystem(); t0 = time.perf_counter()
            with fs.open(f"{BUCKET}/{pfx}/stored.harp", "rb", cache_type=ct) as f, zipfile.ZipFile(f) as zf:
                with zf.open(f"{base}.bin") as m: m.seek(100_000 * stride); got = m.read(1000 * stride)
            extras[f"s3fs_{ct}"] = dict(ok=got == want, ms=(time.perf_counter() - t0) * 1e3)
    except Exception as e: extras["s3fs"] = dict(ok=False, err=f"{type(e).__name__}: {e}")
    extras["tar_list_local"] = dict(rc=sh(["tar", "-tf", str(a.out / "stored.harp")])[1])
    extras["zipfile_cli_test_local"] = dict(rc=sh([sys.executable, "-I", "-m", "zipfile", "-t", str(a.out / "stored.harp")])[1])

    (a.out / "results.json").write_text(json.dumps(dict(info=info, opens=opens, log=log, extras=extras, entries=a.entries,
                                                        chunk_rows=a.chunk_rows, prefix=f"s3://{BUCKET}/{pfx}/"), indent=1, default=str))
    print("done", flush=True)

if __name__ == "__main__":
    main()
