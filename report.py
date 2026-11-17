"""Build report.md from the results of harp_archive_bench.py, parquet_bench.py and usecases_bench.py.

usage: python report.py results/harp_archive_results.json results/parquet_results.json results/usecases_results.json report.md
"""
import json, sys
from collections import defaultdict
from pathlib import Path
from statistics import median

H = json.load(open(sys.argv[1]))
P = json.load(open(sys.argv[2]))
U = json.load(open(sys.argv[3]))
OUT = Path(sys.argv[4] if len(sys.argv) > 4 else "report.md")
MB = 1e6
HL = ["folder", "stored", "deflate1", "stored_chunked", "deflate1_chunked"]
PV = list(P["info"])


def f(x, p=1): return "-" if x is None else f"{x:,.{p}f}"


def agg(log, loc, layout, op, key, ts_only=False):
    v = [r[key] for r in log if r["loc"] == loc and r["layout"] == layout and r["op"] == op and r[key] is not None
         and (not ts_only or r["timestamped"])]
    return median(v) if v else None


out = []
w = out.append
hlog, plog, ulog = H["log"], P["log"], U["log"]
regs = sorted({r["reg"] for r in hlog})
n_ts = sum("NoTimestamp" not in r for r in regs)
bad = [r for r in hlog + plog + ulog if not r["ok"]]
total = len(hlog) + len(plog) + len(ulog)

w("# Packed `.harp` (harp-tech/protocol#251) vs folder vs Parquet, local and S3\n")
w(f"{len(regs)} registers from `harp.benchmarks` ({n_ts} timestamped, {len(regs) - n_ts} untimestamped twins), "
  f"{H['entries']:,} frames each, **synthetic random payloads** from the harness generator, on a 1 row/second timestamp ramp. "
  f"Data: `{H['prefix']}`, `{P['prefix']}`.\n")
w(f"**Correctness:** {total - len(bad):,}/{total:,} timed operations returned data identical to the source"
  f"{'' if not bad else f' ({len(bad)} failed, listed at the end)'}.\n")

w("## What is measured\n")
w("**Layouts.** `folder` is the current format, one `.bin` per register. `stored` and `deflate1` are one `.harp` zip archive holding every register, "
  "members stored or deflated (level 1). The `_chunked` variants split each register into time chunks of "
  f"{H['chunk_rows']:,} rows, one zip member per chunk (`Name_addr_000.bin`, `_001`, ...). Each member's zip comment holds "
  "`{stride, rows, tmin, tmax, monotonic}`, as in the issue. "
  "Parquet writes one file per register with decoded columns and a `Time` column: `snappy_rg50k` and `zstd_rg50k` use row groups of 50,000 rows "
  "(the analogue of a chunk), `zstd_rgall` uses a single row group.\n")
w("**Locations.** `local` reads from disk (`stored` archives use `mmap`, the folder uses seek/read). `S3` reads with ranged `GetObject` requests "
  "from the bucket's region, through one code path shared with local, so requests and bytes are counted. "
  "For Parquet, `S3 native` uses `pyarrow.fs.S3FileSystem` (its own request coalescing, timing only).\n")
w("**Part A, use cases (section 1).** End-to-end tasks, each measured cold: opening the recording is part of the cost.\n")
w("| Use case | Task |\n|-|-|")
w("| `uc1_window_all` | All events between two times (1,000 rows per register) from every register. Untimestamped registers cannot take part. |")
w("| `uc1_window_empty` | The same query for a window after the end of the recording. Can metadata alone answer it? |")
w("| `uc2_full5` | Load five whole registers (DigitalInputs, AnalogData, Counter0, PulseDO0, StartPulse) into DataFrames. |")
w("| `uc3_bounds_meta` | Earliest and latest timestamp in the recording, leaning on metadata where the format has it. |")
w("| `uc3_bounds_msgs` | The same, plus fetching the earliest and the latest message itself. |")
w("\nModes: `seq` issues one request at a time, `par` uses 8 threads (S3 only).\n")
w("**Part B, per-register operations (sections 2-4).** Micro-benchmarks, each verified against the source:\n")
w("| Op | What it does |\n|-|-|")
w("| `layout` | Rows, stride and time range of a register. Archive: from the zip comments already loaded at open. Folder: 3 reads (first 2 bytes, first frame, last frame). Parquet: from the file footer, read at open. |")
w("| `full` | Reads a whole register. `.harp`: bytes + `parse_bulk` + realizing the timestamp column (no decoding). Parquet: file to an Arrow table (decoded columns). |")
w("| `full_df` | Parquet only: `full` plus `to_pandas()`. Compare against `.harp` `full` plus the decode cost in section 4. |")
w("| `slice` | Rows [100000:101000] by index, then `parse_to_dataframe` (`.harp`) or `to_pandas` (Parquet). Tests fixed-offset row access. |")
w("| `slice_x` | Rows [49500:50500], which straddle the boundary between chunk/row group 0 (rows 0-49,999) and 1 (rows 50,000+). Tests stitching two pieces together. Layouts without a boundary run an ordinary slice, as a control. |")
w("| `timeq` | The 1,000 rows between two timestamps (rows 200,000-201,000). Skips chunks or row groups whose time range does not overlap, bisects the rest where the metadata says timestamps are monotonic, and otherwise checks every timestamp (folder). Timestamped registers only. |")
w("")

# ------------------------------------------------------------------ use cases
w("## 1. Use cases\n")
LAY = [(L, L) for L in HL] + [(f"parquet_{v}", f"parquet {v}") for v in PV]
by = {(r["loc"], r["layout"], r["uc"], r["mode"]): r for r in ulog}
UCT = [("uc1_window_all", "All events in a time window, from all registers"),
       ("uc1_window_empty", "The same, for a window past the end of the recording"),
       ("uc2_full5", "Load 5 whole registers into DataFrames"),
       ("uc3_bounds_meta", "Earliest and latest timestamp in the recording"),
       ("uc3_bounds_msgs", "Earliest and latest timestamp, plus the two messages")]
for uc, title in UCT:
    w(f"### {title}\n")
    w("| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |")
    w("|-|-:|-:|-:|-:|-:|-:|-:|")
    for key, label in LAY:
        pq_ = key.startswith("parquet")
        s3 = "s3_counted" if pq_ else "s3"
        g = lambda loc, mode: by.get((loc, key, uc, mode))
        loc_, sq, pr = g("local", "seq"), g(s3, "seq"), g(s3, "par")
        ns, npar = (g("s3_native", "seq"), g("s3_native", "par")) if pq_ else (None, None)
        w(f"| {label} | {f(loc_ and loc_['ms'], 1)} | {f(sq and sq['ms'], 0)} | {f(pr and pr['ms'], 0)} | "
          f"{f(sq and sq['requests'], 0)} | {f(sq and sq['bytes'] / MB, 2)} | {f(ns and ns['ms'], 0)} | {f(npar and npar['ms'], 0)} |")
    w("")

# ------------------------------------------------------------------ part B
w("## 2. Size and upload\n")
w("| Layout | Size (MB) | vs raw | Pack/convert + verify (s) | Upload (s) | Objects |")
w("|-|-:|-:|-:|-:|-:|")
raw = H["info"]["folder"]["bytes"]
for L in HL:
    i = H["info"][L]
    w(f"| {L} | {i['bytes'] / MB:,.1f} | {i['bytes'] / raw:.2f}x | {f(i['pack_s'] + i['verify_s']) if L != 'folder' else '-'} | {f(i.get('upload_s'))} | {i.get('upload_objects', '-')} |")
for v in PV:
    i = P["info"][v]
    w(f"| parquet {v} | {i['bytes'] / MB:,.1f} | {i['bytes'] / raw:.2f}x | {f(sum(x['write_s'] for x in i['files'].values()))} | {f(i.get('upload_s'))} | {i.get('upload_objects', '-')} |")
w("\nPayloads are uniformly random bytes, so compression ratios here are pessimistic. The issue measured about 3x for deflate on real Behavior data. "
  "Parquet size also depends on the stringified object columns noted at the end.\n")

w("## 3. Open and discover, then learn the layout of every register\n")
w("What it costs, once per recording, to know every register's row count and time range.\n")
w("| Layout | Local ms | S3 ms | S3 requests | S3 KB fetched |")
w("|-|-:|-:|-:|-:|")
for L in HL:
    o_l, o_s = H["opens"][f"local/{L}"], H["opens"][f"s3/{L}"]
    lay_l = [r for r in hlog if r["loc"] == "local" and r["layout"] == L and r["op"] == "layout"]
    lay_s = [r for r in hlog if r["loc"] == "s3" and r["layout"] == L and r["op"] == "layout"]
    ml = o_l["ms"] + sum(r["ms"] for r in lay_l); ms = o_s["ms"] + sum(r["ms"] for r in lay_s)
    rq = o_s["requests"] + sum(r["requests"] for r in lay_s); by_ = o_s["bytes"] + sum(r["bytes"] for r in lay_s)
    w(f"| {L} | {ml:.1f} | {ms:,.0f} | {rq} | {by_ / 1e3:,.0f} |")
for v in PV:
    ol, oc, on = P["opens"][f"local/{v}"], P["opens"][f"s3_counted/{v}"], P["opens"][f"s3_native/{v}"]
    w(f"| parquet {v} | {ol['ms_all']:.1f} | {oc['ms_all']:,.0f} (native {on['ms_all']:,.0f}) | {oc['requests_all']} | - |")
w("\nThe folder needs a listing plus 3 reads per register. The archive needs one tail request, which carries the central directory "
  "and every register's metadata. Parquet needs one footer read per file, so one request per register.\n")


def op_table(title, op, tso, note=""):
    w(f"### {title}\n")
    if note: w(note + "\n")
    w("| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |")
    w("|-|-:|-:|-:|-:|-:|")
    for L in HL:
        w(f"| {L} | {f(agg(hlog, 'local', L, op, 'ms', tso), 2)} | {f(agg(hlog, 's3', L, op, 'ms', tso), 0)} | "
          f"{f(agg(hlog, 's3', L, op, 'requests', tso), 0)} | {f((agg(hlog, 's3', L, op, 'bytes', tso) or 0) / MB, 3)} | - |")
    for v in PV:
        w(f"| parquet {v} | {f(agg(plog, 'local', v, op, 'ms', tso), 2)} | {f(agg(plog, 's3_counted', v, op, 'ms', tso), 0)} | "
          f"{f(agg(plog, 's3_counted', v, op, 'requests', tso), 0)} | {f((agg(plog, 's3_counted', v, op, 'bytes', tso) or 0) / MB, 3)} | "
          f"{f(agg(plog, 's3_native', v, op, 'ms', tso), 0)} |")
    w("")


w("## 4. Per-register operations, local vs S3\n")
w("Median over registers of each register's median time. Requests and bytes are for one operation on an already-opened store.\n")
op_table("Full register read", "full", False, "`.harp` rows stop before decoding; Parquet rows are already decoded columns. See section 5 for a like-for-like comparison.")
op_table("Slice, rows [100000:101000]", "slice", False)
op_table("Slice across a chunk or row-group boundary, rows [49500:50500]", "slice_x", False)
op_table("Time range, 1,000 rows", "timeq", True, "Timestamped registers only.")

w("## 5. Like-for-like full read: `.harp` plus decode vs Parquet to pandas\n")
dec = P["decode_ms"]
w("`.harp` stores raw Harp frames, so getting a DataFrame costs the read plus `parse_to_dataframe`. The decode cost (location-independent, measured once per register on the harness corpus) "
  f"has median {median(dec.values()):.0f} ms and maximum {max(dec.values()):,.0f} ms, dominated by registers with Python-loop converters (e.g. `Version`).\n")
w("| Layout | Local ms | S3 ms |")
w("|-|-:|-:|")
for L in ["folder", "stored", "deflate1"]:
    def tot(loc):
        v = [r["ms"] + dec[r["reg"]] for r in hlog if r["loc"] == loc and r["layout"] == L and r["op"] == "full" and r["ms"] is not None]
        return median(v)
    w(f"| {L} + decode | {tot('local'):.1f} | {tot('s3'):,.0f} |")
for v in PV:
    w(f"| parquet {v} | {f(agg(plog, 'local', v, 'full_df', 'ms'))} | {f(agg(plog, 's3_counted', v, 'full_df', 'ms'), 0)} |")
w("")

w("## 6. Per-register detail (local ms / S3 ms)\n")
w("Stored archive, and Parquet `zstd_rg50k`.\n")
w("| Register | harp full | harp slice | harp slice_x | harp timeq | pq full_df | pq slice | pq timeq |")
w("|-|-:|-:|-:|-:|-:|-:|-:|")
byreg = defaultdict(dict)
for r in hlog:
    if r["layout"] == "stored": byreg[r["reg"]][("h", r["loc"], r["op"])] = r
for r in plog:
    if r["layout"] == "zstd_rg50k": byreg[r["reg"]][("p", r["loc"], r["op"])] = r
for reg in regs:
    row = byreg[reg]
    def cell(kind, op, locs):
        a, b = row.get((kind, locs[0], op)), row.get((kind, locs[1], op))
        return "n/a" if not a else f"{f(a['ms'], 1)} / {f(b['ms'], 0) if b else '-'}"
    w(f"| {reg} | " + " | ".join([cell("h", "full", ("local", "s3")), cell("h", "slice", ("local", "s3")), cell("h", "slice_x", ("local", "s3")),
                                  cell("h", "timeq", ("local", "s3")), cell("p", "full_df", ("local", "s3_counted")),
                                  cell("p", "slice", ("local", "s3_counted")), cell("p", "timeq", ("local", "s3_counted"))]) + " |")

w("\n## 7. Tool checks (`.harp`, S3)\n")
w("| Check | Result |\n|-|-|")
for k, v in H["extras"].items(): w(f"| {k} | {json.dumps(v)} |")

w("\n## Notes and caveats\n")
w("- Payloads are random bytes from the harness generator. Compression ratios, and Parquet encodings in particular, will differ on real recordings.")
w("- Every register spans the same 0-329,999 s ramp, so register-level pruning by time range (skipping a whole register) is never exercised, only chunk-level pruning. The empty-window use case shows the all-registers-skipped case.")
n_str = sum(1 for x in P["info"][PV[0]]["files"].values() if x["stringified"])
w(f"- {n_str} registers have object or categorical columns (custom version types, enums with undefined random values) that Arrow rejected; those columns were stringified before writing Parquet.")
w("- The folder cannot promise monotonic timestamps, so time queries on it scan every file. A reader that trusted the first and last frame would be cheaper but not safe.")
w("- Remote per-register operations are sequential; use-case runs also show an 8-thread mode. Remote `full` reads ran once per register (local five times), so they are noisier than the slice and time-range numbers.")
w("- `mmap` has no remote equivalent. Remotely, every access is a ranged request with a round-trip latency floor.")
w("- Slices of stored members make no CRC check, locally or remotely.")
w("- Use cases: the first pass showed an isolated 15.8 s stall for `deflate1_chunked` loading 5 registers on S3 (same request count as `stored_chunked` at 4.3 s). Both chunked layouts were re-run in isolation and those rows replace the first-pass ones (see `remeasured` in `results/usecases_results.json`). Other S3 numbers are single passes of 2 runs, so expect similar occasional stalls.")
w("- S3 numbers vary a lot between sessions. A first run of the per-register `.harp` benchmark, on a slower connection, measured a full register read at 1,573 ms against 220 ms in the final run. The per-register `.harp` figures here were re-measured in the same session as the Parquet and use-case runs (against the same S3 objects), so compare rows only within this report. The `.harp` upload times come from the first session, so they are not comparable with the Parquet upload times.")
if bad:
    w("\n## Failures\n")
    for r in bad[:40]: w(f"- {r['loc']}/{r['layout']}/{r.get('reg', r.get('uc'))}/{r.get('op', r.get('mode'))}: {r['err']}")
OUT.write_text("\n".join(out), encoding="utf-8")
print("wrote", OUT, len(out), "lines")
