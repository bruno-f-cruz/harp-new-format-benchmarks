# Packed `.harp` (harp-tech/protocol#251) vs folder vs Parquet, local and S3

15 registers from `harp.benchmarks` (timestamped only, as the issue requires), 330,000 frames each, **synthetic random payloads** from the harness generator, on a 1 row/second timestamp ramp. Data: `s3://aind-scratch-data/bruno.cruz/harp-tests/bench-ts-20261117/`, `s3://aind-scratch-data/bruno.cruz/harp-tests/bench-ts-20261117/parquet/`.

**Correctness:** 1,575/1,575 timed operations returned data identical to the source.

## What is measured

**Layouts** are the ways the same recording is stored. Names used in every table:

| Name | What it is on disk / S3 | Compression | Metadata |
|-|-|-|-|
| `folder` | The current format: a directory with one `.bin` file per register (raw Harp frames). On S3, one object per register. | none | none; a reader must read the file to learn its row count and time range |
| `stored` | One `.harp` zip archive holding every register as one member each. "Stored" is the zip term for no compression, so members are byte-for-byte the `.bin` files. | none | zip comment per member: `{stride, rows, tmin, tmax, monotonic}`, as in the issue |
| `deflate1` | Same single `.harp` archive, members compressed with zip deflate at level 1 (fastest). | deflate, level 1 | same |
| `stored_chunked` | Same as `stored`, but each register is split into time chunks of 50,000 rows, one zip member per chunk (`Name_addr_000.bin`, `_001`, ...). | none | per-chunk comment, so a reader can skip chunks by time |
| `deflate1_chunked` | Chunked, and each chunk deflated (level 1). | deflate, level 1 | per-chunk comment |
| `snappy_rg50k`, `zstd_rg50k` | Not a Harp format: one Parquet file per register, decoded columns plus a `Time` column, in row groups of 50,000 rows (the analogue of a chunk). | snappy or zstd | row-group statistics in the file footer |
| `zstd_rgall` | Parquet, zstd, one row group for the whole register. | zstd | footer |

The `layout` operation in the per-register tables is the metadata lookup (row count, frame stride, first and last timestamp) a reader needs before any slice or time query. Section 3 measures it.

**Locations.** `local` reads from disk (`stored` archives use `mmap`, the folder uses seek/read). `S3` reads with ranged `GetObject` requests from the bucket's region, through one code path shared with local, so requests and bytes are counted. For Parquet, `S3 native` uses `pyarrow.fs.S3FileSystem` (its own request coalescing, timing only).

**Part A, use cases (section 1).** End-to-end tasks, each measured cold: opening the recording is part of the cost.

| Use case | Task |
|-|-|
| `uc1_window_all` | All events between two times (1,000 rows per register) from every register. |
| `uc1_window_empty` | The same query for a window after the end of the recording. Can metadata alone answer it? |
| `uc2_full5` | Load five whole registers (DigitalInputs, AnalogData, Counter0, PulseDO0, StartPulse) into DataFrames. |
| `uc3_bounds_meta` | Earliest and latest timestamp in the recording, leaning on metadata where the format has it. |
| `uc3_bounds_msgs` | The same, plus fetching the earliest and the latest message itself. |

Modes: `seq` issues one request at a time, `par` uses 8 threads (S3 only).

**Part B, per-register operations (sections 2-4).** Micro-benchmarks, each verified against the source:

| Op | What it does |
|-|-|
| `layout` | Rows, stride and time range of a register. Archive: from the zip comments already loaded at open. Folder: 3 reads (first 2 bytes, first frame, last frame). Parquet: from the file footer, read at open. |
| `full` | Reads a whole register. `.harp`: bytes + `parse_bulk` + realizing the timestamp column (no decoding). Parquet: file to an Arrow table (decoded columns). |
| `full_df` | Parquet only: `full` plus `to_pandas()`. Compare against `.harp` `full` plus the decode cost in section 4. |
| `slice` | Rows [100000:101000] by index, then `parse_to_dataframe` (`.harp`) or `to_pandas` (Parquet). Tests fixed-offset row access. |
| `slice_x` | Rows [49500:50500], which straddle the boundary between chunk/row group 0 (rows 0-49,999) and 1 (rows 50,000+). Tests stitching two pieces together. Layouts without a boundary run an ordinary slice, as a control. |
| `timeq` | The 1,000 rows between two timestamps (rows 200,000-201,000). Skips chunks or row groups whose time range does not overlap, bisects the rest where the metadata says timestamps are monotonic, and otherwise checks every timestamp (folder). Timestamped registers only. |

## 1. Use cases

### All events in a time window, from all registers

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 121.0 | 30,640 | 12,351 | 61 | 98.34 | - | - |
| stored | 11.8 | 10,126 | 2,009 | 193 | 11.18 | - | - |
| deflate1 | 371.0 | 17,442 | 9,619 | 31 | 56.74 | - | - |
| stored_chunked | 13.2 | 7,508 | 1,155 | 97 | 4.89 | - | - |
| deflate1_chunked | 139.5 | 4,357 | 1,663 | 31 | 8.87 | - | - |
| parquet snappy_rg50k | 91.9 | 8,641 | 3,029 | 31 | 17.66 | 5,010 | 2,516 |
| parquet zstd_rg50k | 82.2 | 5,832 | 2,259 | 31 | 13.23 | 6,032 | 1,586 |
| parquet zstd_rgall | 290.3 | 17,544 | 8,160 | 31 | 56.65 | 17,114 | 8,925 |

### The same, for a window past the end of the recording

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 93.5 | 30,960 | 13,597 | 61 | 98.34 | - | - |
| stored | 0.7 | 150 | 127 | 1 | 0.26 | - | - |
| deflate1 | 0.7 | 115 | 93 | 1 | 0.26 | - | - |
| stored_chunked | 1.5 | 127 | 124 | 1 | 0.26 | - | - |
| deflate1_chunked | 1.4 | 118 | 111 | 1 | 0.26 | - | - |
| parquet snappy_rg50k | 17.0 | 1,937 | 729 | 16 | 3.93 | 1,391 | 276 |
| parquet zstd_rg50k | 19.4 | 1,627 | 662 | 16 | 3.93 | 1,768 | 281 |
| parquet zstd_rgall | 18.2 | 1,929 | 652 | 16 | 3.93 | 1,477 | 310 |

### Load 5 whole registers into DataFrames

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 31.9 | 8,394 | 5,151 | 6 | 30.69 | - | - |
| stored | 49.7 | 7,949 | 6,082 | 11 | 30.95 | - | - |
| deflate1 | 116.2 | 6,379 | 3,920 | 11 | 17.09 | - | - |
| stored_chunked | 60.6 | 12,057 | 6,095 | 71 | 30.96 | - | - |
| deflate1_chunked | 174.6 | 7,730 | 4,058 | 71 | 17.09 | - | - |
| parquet snappy_rg50k | 83.7 | 8,647 | 5,298 | 11 | 26.55 | 7,963 | 4,986 |
| parquet zstd_rg50k | 100.9 | 6,538 | 4,221 | 11 | 19.68 | 5,633 | 3,953 |
| parquet zstd_rgall | 82.7 | 6,417 | 4,026 | 11 | 17.20 | 5,320 | 3,548 |

### Earliest and latest timestamp in the recording

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 1.1 | 2,137 | 446 | 46 | 0.00 | - | - |
| stored | 0.7 | 152 | 125 | 1 | 0.26 | - | - |
| deflate1 | 0.9 | 179 | 197 | 1 | 0.26 | - | - |
| stored_chunked | 2.4 | 117 | 169 | 1 | 0.26 | - | - |
| deflate1_chunked | 1.3 | 126 | 159 | 1 | 0.26 | - | - |
| parquet snappy_rg50k | 7.1 | 1,515 | 693 | 16 | 3.93 | 1,361 | 278 |
| parquet zstd_rg50k | 7.4 | 1,644 | 686 | 16 | 3.93 | 1,328 | 271 |
| parquet zstd_rgall | 5.0 | 1,891 | 707 | 16 | 3.93 | 1,392 | 314 |

### Earliest and latest timestamp, plus the two messages

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 1.0 | 2,389 | 383 | 48 | 0.00 | - | - |
| stored | 0.4 | 219 | 201 | 4 | 0.26 | - | - |
| deflate1 | 12.3 | 780 | 858 | 3 | 1.84 | - | - |
| stored_chunked | 1.8 | 332 | 246 | 5 | 0.26 | - | - |
| deflate1_chunked | 4.1 | 500 | 309 | 5 | 0.64 | - | - |
| parquet snappy_rg50k | 14.0 | 2,308 | 911 | 17 | 4.26 | 1,756 | 541 |
| parquet zstd_rg50k | 11.4 | 1,676 | 787 | 17 | 4.12 | 1,494 | 397 |
| parquet zstd_rgall | 24.1 | 2,063 | 1,313 | 18 | 5.55 | 2,142 | 605 |

## 2. Size and upload

| Layout | Size (MB) | vs raw | Pack/convert + verify (s) | Upload (s) | Objects |
|-|-:|-:|-:|-:|-:|
| folder | 98.3 | 1.00x | - | 14.3 | 15 |
| stored | 98.3 | 1.00x | 0.2 | 13.6 | 1 |
| deflate1 | 56.5 | 0.57x | 2.4 | 8.9 | 1 |
| stored_chunked | 98.4 | 1.00x | 0.2 | 14.5 | 1 |
| deflate1_chunked | 56.5 | 0.57x | 3.7 | 9.4 | 1 |
| parquet snappy_rg50k | 90.6 | 0.92x | 1.9 | 14.7 | 15 |
| parquet zstd_rg50k | 61.7 | 0.63x | 2.0 | 10.8 | 15 |
| parquet zstd_rgall | 52.8 | 0.54x | 1.9 | 9.9 | 15 |

Payloads are uniformly random bytes, so compression ratios here are pessimistic. The issue measured about 3x for deflate on real Behavior data. Parquet size also depends on the stringified object columns noted at the end.

## 3. Open and discover, then learn the layout of every register

What it costs, once per recording, to know every register's row count and time range.

| Layout | Local ms | S3 ms | S3 requests | S3 KB fetched |
|-|-:|-:|-:|-:|
| folder | 1.7 | 2,206 | 46 | 0 |
| stored | 0.4 | 87 | 1 | 262 |
| deflate1 | 0.3 | 84 | 1 | 262 |
| stored_chunked | 2.0 | 300 | 1 | 262 |
| deflate1_chunked | 1.0 | 111 | 1 | 262 |
| parquet snappy_rg50k | 5.9 | 1,215 (native 1,381) | 15 | - |
| parquet zstd_rg50k | 5.8 | 1,346 (native 1,513) | 15 | - |
| parquet zstd_rgall | 4.6 | 2,123 (native 1,571) | 15 | - |

The folder needs a listing plus 3 reads per register. The archive needs one tail request, which carries the central directory and every register's metadata. Parquet needs one footer read per file, so one request per register.

## 4. Per-register operations, local vs S3

Median over registers of each register's median time. Requests and bytes are for one operation on an already-opened store.

### Full register read

`.harp` rows stop before decoding; Parquet rows are already decoded columns. See section 5 for a like-for-like comparison.

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 4.11 | 1,410 | 1 | 4.950 | - |
| stored | 1.48 | 1,026 | 2 | 4.950 | - |
| deflate1 | 24.30 | 899 | 2 | 2.250 | - |
| stored_chunked | 7.81 | 1,606 | 14 | 4.951 | - |
| deflate1_chunked | 24.05 | 1,094 | 14 | 2.251 | - |
| parquet snappy_rg50k | 9.36 | 803 | 1 | 3.004 | 850 |
| parquet zstd_rg50k | 12.02 | 602 | 1 | 1.925 | 582 |
| parquet zstd_rgall | 10.73 | 538 | 1 | 1.426 | 455 |

### Slice, rows [100000:101000]

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 0.46 | 86 | 2 | 0.015 | - |
| stored | 0.43 | 89 | 2 | 0.015 | - |
| deflate1 | 22.71 | 719 | 2 | 2.250 | - |
| stored_chunked | 0.38 | 89 | 2 | 0.015 | - |
| deflate1_chunked | 3.16 | 162 | 2 | 0.340 | - |
| parquet snappy_rg50k | 1.86 | 150 | 1 | 0.456 | 194 |
| parquet zstd_rg50k | 2.16 | 147 | 1 | 0.289 | 122 |
| parquet zstd_rgall | 11.23 | 484 | 1 | 1.426 | 398 |

### Slice across a chunk or row-group boundary, rows [49500:50500]

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 0.42 | 85 | 2 | 0.015 | - |
| stored | 0.38 | 86 | 2 | 0.015 | - |
| deflate1 | 22.71 | 841 | 2 | 2.250 | - |
| stored_chunked | 0.46 | 168 | 4 | 0.015 | - |
| deflate1_chunked | 6.04 | 311 | 4 | 0.680 | - |
| parquet snappy_rg50k | 3.01 | 305 | 2 | 0.903 | 418 |
| parquet zstd_rg50k | 3.98 | 261 | 2 | 0.587 | 227 |
| parquet zstd_rgall | 11.09 | 346 | 1 | 1.426 | 354 |

### Time range, 1,000 rows

Timestamped registers only.

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 5.93 | 1,613 | 4 | 4.950 | - |
| stored | 0.47 | 685 | 12 | 0.670 | - |
| deflate1 | 23.58 | 848 | 2 | 2.250 | - |
| stored_chunked | 0.48 | 313 | 6 | 0.277 | - |
| deflate1_chunked | 3.14 | 143 | 2 | 0.345 | - |
| parquet snappy_rg50k | 1.86 | 144 | 1 | 0.457 | 213 |
| parquet zstd_rg50k | 2.44 | 135 | 1 | 0.290 | 130 |
| parquet zstd_rgall | 11.85 | 483 | 1 | 1.426 | 360 |

## 5. Like-for-like full read: `.harp` plus decode vs Parquet to pandas

`.harp` stores raw Harp frames, so getting a DataFrame costs the read plus `parse_to_dataframe`. The decode cost (location-independent, measured once per register on the harness corpus) has median 9 ms and maximum 983 ms, dominated by registers with Python-loop converters (e.g. `Version`).

| Layout | Local ms | S3 ms |
|-|-:|-:|
| folder + decode | 15.8 | 1,424 |
| stored + decode | 10.4 | 1,030 |
| deflate1 + decode | 32.9 | 902 |
| parquet snappy_rg50k | 12.3 | 776 |
| parquet zstd_rg50k | 14.4 | 671 |
| parquet zstd_rgall | 14.7 | 505 |

## 6. Per-register detail (local ms / S3 ms)

Stored archive, and Parquet `zstd_rg50k`.

| Register | harp full | harp slice | harp slice_x | harp timeq | pq full_df | pq slice | pq timeq |
|-|-:|-:|-:|-:|-:|-:|-:|
| AnalogData | 3.8 / 3,072 | 0.6 / 103 | 0.4 / 91 | 0.5 / 673 | 15.8 / 1,961 | 2.3 / 484 | 2.6 / 347 |
| BitmaskSplitter | 1.4 / 792 | 0.2 / 93 | 0.2 / 83 | 0.3 / 521 | 11.5 / 671 | 1.9 / 132 | 2.1 / 88 |
| ComplexConfiguration | 3.8 / 1,012 | 0.5 / 86 | 0.6 / 87 | 0.5 / 829 | 17.5 / 1,208 | 2.8 / 178 | 2.9 / 210 |
| Counter0 | 1.3 / 1,200 | 0.2 / 77 | 0.2 / 92 | 0.3 / 681 | 13.8 / 946 | 1.9 / 234 | 2.0 / 284 |
| CustomMemberConverter | 1.3 / 815 | 0.3 / 126 | 0.3 / 75 | 0.3 / 724 | 11.8 / 345 | 1.9 / 124 | 2.0 / 145 |
| CustomPayload | 1.6 / 1,768 | 0.9 / 94 | 0.8 / 78 | 0.9 / 706 | 29.7 / 2,283 | 5.6 / 264 | 5.1 / 231 |
| CustomRawPayload | 2.0 / 1,099 | 1.0 / 94 | 0.8 / 90 | 1.0 / 678 | 29.0 / 1,685 | 4.6 / 379 | 4.9 / 153 |
| DigitalInputs | 1.5 / 1,204 | 0.3 / 86 | 0.3 / 95 | 0.4 / 695 | 12.0 / 254 | 1.8 / 69 | 2.1 / 77 |
| EncoderMode | 1.7 / 1,039 | 0.6 / 96 | 0.5 / 77 | 0.6 / 799 | 14.4 / 572 | 2.2 / 106 | 2.4 / 111 |
| PortDIOSet | 1.3 / 498 | 0.2 / 95 | 0.2 / 88 | 0.2 / 560 | 11.8 / 462 | 1.9 / 89 | 2.0 / 98 |
| PulseDO0 | 1.2 / 955 | 0.2 / 87 | 0.1 / 86 | 0.2 / 604 | 11.6 / 648 | 1.8 / 148 | 2.0 / 94 |
| PulseDOPort0 | 1.2 / 1,026 | 0.2 / 82 | 0.1 / 83 | 0.2 / 685 | 11.6 / 686 | 1.9 / 147 | 2.0 / 135 |
| StartPulse | 1.5 / 939 | 0.4 / 74 | 0.4 / 72 | 0.5 / 676 | 15.6 / 533 | 2.5 / 95 | 2.7 / 64 |
| StartPulseTrain | 1.4 / 636 | 0.5 / 89 | 0.4 / 82 | 0.8 / 740 | 15.3 / 314 | 3.4 / 86 | 2.7 / 135 |
| Version | 4.7 / 4,265 | 3.2 / 88 | 2.9 / 96 | 2.8 / 708 | 44.4 / 4,073 | 7.7 / 586 | 8.1 / 529 |

## 7. Tool checks (`.harp`, S3)

| Check | Result |
|-|-|
| presigned_http | {"ok": true, "ms": 619.2964000074426, "requests": 3} |
| s3fs_readahead | {"ok": true, "ms": 2869.602800012217} |
| s3fs_none | {"ok": true, "ms": 326.6505999927176} |
| tar_list_local | {"rc": 0} |
| zipfile_cli_test_local | {"rc": 0} |

## Notes and caveats

- Payloads are random bytes from the harness generator. Compression ratios, and Parquet encodings in particular, will differ on real recordings.
- Every register spans the same 0-329,999 s ramp, so register-level pruning by time range (skipping a whole register) is never exercised, only chunk-level pruning. The empty-window use case shows the all-registers-skipped case.
- 7 registers have object or categorical columns (custom version types, enums with undefined random values) that Arrow rejected; those columns were stringified before writing Parquet.
- The folder cannot promise monotonic timestamps, so time queries on it scan every file. A reader that trusted the first and last frame would be cheaper but not safe.
- Remote per-register operations are sequential; use-case runs also show an 8-thread mode. Remote `full` reads ran once per register (local five times), so they are noisier than the slice and time-range numbers.
- `mmap` has no remote equivalent. Remotely, every access is a ranged request with a round-trip latency floor.
- Slices of stored members make no CRC check, locally or remotely.
- All three benchmarks ran in one session against the same objects. S3 speed varies a lot between sessions (an earlier session measured the folder time-window query at roughly 3x less), so compare rows only within this report.
- One pyarrow native-S3 call (`CustomMemberConverter`, `snappy_rg50k`) hung for about three hours in the first pass, probably after the credentials expired. The whole `s3_native/snappy_rg50k` stage was re-measured afterwards and replaces the first-pass rows (see `remeasured` in `results/parquet_results.json`).

## 8. Conclusions

These are the author's reading of the tables above, on synthetic data (see the caveats). They are a take, not a measurement.

1. **The packed `.harp` format earns its place through metadata, not through the zip container.** The biggest and most robust wins are the questions a reader can answer from the central directory and the zip comments alone: earliest and latest timestamp, row counts, and "is there anything in this window?". These take one or two requests against dozens for a folder, and an empty window costs almost no data. A folder cannot promise monotonic timestamps or a time range without reading the files, which is exactly what the issue's comment fields fix.

2. **Chunking is worth having for remote use.** An unchunked, stored archive needs one bisection round trip per register and per step, so on a slow link its sequential time-window query is several times slower than the chunked variants (it is still faster than the folder, which scans everything). Splitting registers into time chunks lets a reader skip chunks from metadata and bisect only inside one, and it makes parallel reads natural. The cost is more members and a naming convention. I would keep chunking in the proposal, with a recommended chunk size rather than a fixed one.

3. **Deflate level 1 is a reasonable default, with a caveat.** It roughly halves the bytes moved on S3 for these payloads, which helps when bandwidth is the limit. It costs CPU, forces whole-member reads (no cheap bisection or slicing without chunks), and the compression ratio here is pessimistic because the payloads are random. Real recordings compress better (the issue measured about 3x). Deflate is best combined with chunking, so a slice only decompresses one chunk.

4. **Loading whole registers is bandwidth-bound and does not discriminate much between formats.** If most reads are "give me everything", the layout matters less than the compression choice and the network.

5. **Parquet is faster for single-register slices and time ranges on S3, and that is not a reason to switch.** It returns decoded columns in one request, which is genuinely better for analysis. But it is a different data model: it needs a conversion step, it loses the raw frames that Harp tooling and the existing readers expect, and it needed workarounds for registers with object or categorical columns. `.harp` keeps the raw bytes and the existing register files, so it is an incremental change to the current ecosystem. The remaining cost is a decode step on the reader side.

6. **Local behaviour is close between formats, so the case rests on remote access and metadata.** If recordings are only ever read from local disk, the gain is smaller and mostly about a single self-describing file and fast metadata queries.

7. **What these results do not show.** There is no real recording, so compression and the value of skipping whole registers by time are untested. Variable-length registers (#220) do not fit the stride-based layout. Writers still record into a folder and pack at the end, which is not measured here. S3 timings are noisy between sessions, so compare rows within this report only.

**Recommendation:** proceed with the `.harp` proposal as written (zip, per-member comments, optional chunking), and recommend chunking plus deflate for archives that will live on object storage. The next evidence to collect is a real recording, to check compression and per-register time ranges, and a comparison against the production reader as the folder baseline.
