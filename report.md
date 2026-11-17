# Packed `.harp` (harp-tech/protocol#251) vs folder vs Parquet, local and S3

30 registers from `harp.benchmarks` (15 timestamped, 15 untimestamped twins), 330,000 frames each, **synthetic random payloads** from the harness generator, on a 1 row/second timestamp ramp. Data: `s3://aind-scratch-data/bruno.cruz/harp-tests/bench-20261116/`, `s3://aind-scratch-data/bruno.cruz/harp-tests/bench-20261116/parquet/`.

**Correctness:** 2,715/2,715 timed operations returned data identical to the source.

## What is measured

**Layouts.** `folder` is the current format, one `.bin` per register. `stored` and `deflate1` are one `.harp` zip archive holding every register, members stored or deflated (level 1). The `_chunked` variants split each register into time chunks of 50,000 rows, one zip member per chunk (`Name_addr_000.bin`, `_001`, ...). Each member's zip comment holds `{stride, rows, tmin, tmax, monotonic}`, as in the issue. Parquet writes one file per register with decoded columns and a `Time` column: `snappy_rg50k` and `zstd_rg50k` use row groups of 50,000 rows (the analogue of a chunk), `zstd_rgall` uses a single row group.

**Locations.** `local` reads from disk (`stored` archives use `mmap`, the folder uses seek/read). `S3` reads with ranged `GetObject` requests from the bucket's region, through one code path shared with local, so requests and bytes are counted. For Parquet, `S3 native` uses `pyarrow.fs.S3FileSystem` (its own request coalescing, timing only).

**Part A, use cases (section 1).** End-to-end tasks, each measured cold: opening the recording is part of the cost.

| Use case | Task |
|-|-|
| `uc1_window_all` | All events between two times (1,000 rows per register) from every register. Untimestamped registers cannot take part. |
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
| folder | 117.2 | 12,563 | 3,818 | 106 | 98.34 | - | - |
| stored | 16.8 | 17,802 | 1,527 | 193 | 11.18 | - | - |
| deflate1 | 514.9 | 5,140 | 1,920 | 31 | 56.74 | - | - |
| stored_chunked | 9.2 | 6,181 | 779 | 97 | 4.89 | - | - |
| deflate1_chunked | 70.1 | 1,957 | 453 | 31 | 8.87 | - | - |
| parquet snappy_rg50k | 102.5 | 5,449 | 966 | 46 | 21.60 | 3,806 | 704 |
| parquet zstd_rg50k | 93.0 | 6,436 | 800 | 46 | 17.17 | 3,346 | 613 |
| parquet zstd_rgall | 239.8 | 6,519 | 2,141 | 46 | 60.58 | 4,919 | 2,099 |

### The same, for a window past the end of the recording

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 59.2 | 9,969 | 3,831 | 106 | 98.34 | - | - |
| stored | 1.5 | 74 | 80 | 1 | 0.26 | - | - |
| deflate1 | 1.1 | 55 | 58 | 1 | 0.26 | - | - |
| stored_chunked | 1.6 | 51 | 64 | 1 | 0.26 | - | - |
| deflate1_chunked | 1.8 | 70 | 60 | 1 | 0.26 | - | - |
| parquet snappy_rg50k | 39.9 | 2,191 | 397 | 31 | 7.86 | 2,985 | 419 |
| parquet zstd_rg50k | 38.2 | 1,762 | 408 | 31 | 7.86 | 2,100 | 349 |
| parquet zstd_rgall | 20.9 | 1,688 | 358 | 31 | 7.86 | 2,119 | 319 |

### Load 5 whole registers into DataFrames

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 33.6 | 2,347 | 1,836 | 6 | 30.69 | - | - |
| stored | 77.9 | 1,721 | 1,374 | 11 | 30.95 | - | - |
| deflate1 | 170.9 | 1,492 | 1,024 | 11 | 17.09 | - | - |
| stored_chunked | 52.4 | 4,167 | 1,684 | 71 | 30.96 | - | - |
| deflate1_chunked | 163.7 | 3,956 | 1,276 | 71 | 17.09 | - | - |
| parquet snappy_rg50k | 98.0 | 2,136 | 1,415 | 11 | 26.55 | 2,950 | 1,103 |
| parquet zstd_rg50k | 90.9 | 1,927 | 1,313 | 11 | 19.68 | 1,630 | 1,276 |
| parquet zstd_rgall | 66.5 | 1,610 | 996 | 11 | 17.20 | 2,051 | 1,051 |

### Earliest and latest timestamp in the recording

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 3.2 | 3,961 | 646 | 91 | 0.00 | - | - |
| stored | 2.0 | 58 | 55 | 1 | 0.26 | - | - |
| deflate1 | 2.7 | 95 | 57 | 1 | 0.26 | - | - |
| stored_chunked | 2.6 | 52 | 61 | 1 | 0.26 | - | - |
| deflate1_chunked | 2.5 | 54 | 60 | 1 | 0.26 | - | - |
| parquet snappy_rg50k | 14.7 | 1,831 | 412 | 31 | 7.86 | 2,150 | 335 |
| parquet zstd_rg50k | 13.6 | 1,713 | 374 | 31 | 7.86 | 2,154 | 324 |
| parquet zstd_rgall | 7.9 | 1,572 | 363 | 31 | 7.86 | 1,957 | 313 |

### Earliest and latest timestamp, plus the two messages

| Layout | Local ms | S3 seq ms | S3 par ms | S3 requests (seq) | S3 MB fetched (seq) | S3 native seq ms | S3 native par ms |
|-|-:|-:|-:|-:|-:|-:|-:|
| folder | 3.5 | 4,042 | 739 | 93 | 0.00 | - | - |
| stored | 1.0 | 174 | 145 | 4 | 0.26 | - | - |
| deflate1 | 17.5 | 244 | 298 | 3 | 1.84 | - | - |
| stored_chunked | 1.8 | 203 | 155 | 5 | 0.26 | - | - |
| deflate1_chunked | 5.1 | 229 | 162 | 5 | 0.64 | - | - |
| parquet snappy_rg50k | 16.4 | 2,597 | 543 | 32 | 8.20 | 2,343 | 472 |
| parquet zstd_rg50k | 18.3 | 2,103 | 474 | 32 | 8.05 | 2,026 | 446 |
| parquet zstd_rgall | 24.0 | 1,795 | 632 | 33 | 9.48 | 2,168 | 500 |

## 2. Size and upload

| Layout | Size (MB) | vs raw | Pack/convert + verify (s) | Upload (s) | Objects |
|-|-:|-:|-:|-:|-:|
| folder | 167.0 | 1.00x | - | 19.7 | 30 |
| stored | 167.0 | 1.00x | 0.3 | 19.2 | 1 |
| deflate1 | 100.7 | 0.60x | 4.1 | 12.8 | 1 |
| stored_chunked | 167.0 | 1.00x | 0.4 | 20.1 | 1 |
| deflate1_chunked | 100.7 | 0.60x | 4.2 | 12.2 | 1 |
| parquet snappy_rg50k | 152.0 | 0.91x | 2.3 | 5.8 | 30 |
| parquet zstd_rg50k | 110.3 | 0.66x | 2.4 | 5.5 | 30 |
| parquet zstd_rgall | 97.7 | 0.59x | 2.2 | 4.8 | 30 |

Payloads are uniformly random bytes, so compression ratios here are pessimistic. The issue measured about 3x for deflate on real Behavior data. Parquet size also depends on the stringified object columns noted at the end.

## 3. Open and discover, then learn the layout of every register

What it costs, once per recording, to know every register's row count and time range.

| Layout | Local ms | S3 ms | S3 requests | S3 KB fetched |
|-|-:|-:|-:|-:|
| folder | 2.4 | 4,277 | 91 | 1 |
| stored | 0.5 | 88 | 1 | 262 |
| deflate1 | 0.3 | 64 | 1 | 262 |
| stored_chunked | 1.5 | 109 | 1 | 262 |
| deflate1_chunked | 1.9 | 64 | 1 | 262 |
| parquet snappy_rg50k | 6.5 | 1,893 (native 2,283) | 30 | - |
| parquet zstd_rg50k | 6.3 | 2,079 (native 2,267) | 30 | - |
| parquet zstd_rgall | 5.6 | 1,911 (native 2,566) | 30 | - |

The folder needs a listing plus 3 reads per register. The archive needs one tail request, which carries the central directory and every register's metadata. Parquet needs one footer read per file, so one request per register.

## 4. Per-register operations, local vs S3

Median over registers of each register's median time. Requests and bytes are for one operation on an already-opened store.

### Full register read

`.harp` rows stop before decoding; Parquet rows are already decoded columns. See section 5 for a like-for-like comparison.

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 3.52 | 220 | 1 | 4.620 | - |
| stored | 0.65 | 280 | 2 | 4.620 | - |
| deflate1 | 13.31 | 201 | 2 | 1.932 | - |
| stored_chunked | 5.47 | 821 | 14 | 4.621 | - |
| deflate1_chunked | 14.59 | 758 | 14 | 1.932 | - |
| parquet snappy_rg50k | 5.79 | 158 | 1 | 2.683 | 187 |
| parquet zstd_rg50k | 6.68 | 108 | 1 | 1.696 | 143 |
| parquet zstd_rgall | 6.11 | 102 | 1 | 1.135 | 131 |

### Slice, rows [100000:101000]

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 0.18 | 77 | 2 | 0.014 | - |
| stored | 0.25 | 77 | 2 | 0.014 | - |
| deflate1 | 12.15 | 158 | 2 | 1.932 | - |
| stored_chunked | 0.19 | 77 | 2 | 0.014 | - |
| deflate1_chunked | 1.89 | 104 | 2 | 0.292 | - |
| parquet snappy_rg50k | 1.68 | 52 | 1 | 0.406 | 65 |
| parquet zstd_rg50k | 1.60 | 49 | 1 | 0.253 | 59 |
| parquet zstd_rgall | 7.04 | 105 | 1 | 1.135 | 150 |

### Slice across a chunk or row-group boundary, rows [49500:50500]

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 0.17 | 77 | 2 | 0.014 | - |
| stored | 0.21 | 76 | 2 | 0.014 | - |
| deflate1 | 12.15 | 160 | 2 | 1.932 | - |
| stored_chunked | 0.18 | 152 | 4 | 0.014 | - |
| deflate1_chunked | 3.54 | 218 | 4 | 0.585 | - |
| parquet snappy_rg50k | 2.19 | 98 | 2 | 0.803 | 120 |
| parquet zstd_rg50k | 2.58 | 94 | 2 | 0.514 | 111 |
| parquet zstd_rgall | 7.00 | 120 | 1 | 1.135 | 135 |

### Time range, 1,000 rows

Timestamped registers only.

| Layout | Local ms | S3 ms | S3 requests | S3 MB fetched | S3 native ms |
|-|-:|-:|-:|-:|-:|
| folder | 5.80 | 385 | 4 | 4.950 | - |
| stored | 0.32 | 495 | 12 | 0.670 | - |
| deflate1 | 12.81 | 178 | 2 | 2.250 | - |
| stored_chunked | 0.20 | 270 | 6 | 0.277 | - |
| deflate1_chunked | 2.23 | 106 | 2 | 0.345 | - |
| parquet snappy_rg50k | 1.58 | 56 | 1 | 0.457 | 67 |
| parquet zstd_rg50k | 2.21 | 48 | 1 | 0.290 | 65 |
| parquet zstd_rgall | 8.12 | 127 | 1 | 1.426 | 161 |

## 5. Like-for-like full read: `.harp` plus decode vs Parquet to pandas

`.harp` stores raw Harp frames, so getting a DataFrame costs the read plus `parse_to_dataframe`. The decode cost (location-independent, measured once per register on the harness corpus) has median 7 ms and maximum 1,248 ms, dominated by registers with Python-loop converters (e.g. `Version`).

| Layout | Local ms | S3 ms |
|-|-:|-:|
| folder + decode | 9.9 | 231 |
| stored + decode | 8.2 | 305 |
| deflate1 + decode | 20.3 | 213 |
| parquet snappy_rg50k | 7.3 | 153 |
| parquet zstd_rg50k | 8.3 | 109 |
| parquet zstd_rgall | 8.3 | 108 |

## 6. Per-register detail (local ms / S3 ms)

Stored archive, and Parquet `zstd_rg50k`.

| Register | harp full | harp slice | harp slice_x | harp timeq | pq full_df | pq slice | pq timeq |
|-|-:|-:|-:|-:|-:|-:|-:|
| AnalogData | 3.2 / 573 | 0.2 / 73 | 0.2 / 86 | 0.3 / 575 | 11.1 / 442 | 1.7 / 109 | 2.4 / 100 |
| AnalogDataNoTimestamp | 0.0 / 594 | 0.2 / 81 | 0.2 / 76 | n/a | 8.0 / 430 | 1.4 / 84 | n/a |
| BitmaskSplitter | 1.4 / 341 | 0.2 / 79 | 0.2 / 85 | 0.2 / 516 | 8.8 / 101 | 1.9 / 42 | 2.3 / 43 |
| BitmaskSplitterNoTimestamp | 0.0 / 174 | 0.1 / 68 | 0.1 / 83 | n/a | 2.0 / 50 | 0.5 / 1 | n/a |
| ComplexConfiguration | 4.8 / 688 | 0.5 / 85 | 0.5 / 131 | 0.5 / 629 | 11.6 / 285 | 2.2 / 67 | 2.5 / 77 |
| ComplexConfigurationNoTimestamp | 0.0 / 527 | 1.3 / 77 | 0.6 / 74 | n/a | 11.1 / 292 | 2.1 / 71 | n/a |
| Counter0 | 1.8 / 355 | 0.1 / 92 | 0.1 / 74 | 0.2 / 472 | 8.3 / 150 | 1.3 / 56 | 1.4 / 48 |
| Counter0NoTimestamp | 0.0 / 184 | 0.1 / 73 | 0.1 / 81 | n/a | 3.2 / 99 | 0.7 / 35 | n/a |
| CustomMemberConverter | 1.4 / 328 | 0.2 / 72 | 0.1 / 70 | 0.2 / 496 | 9.0 / 134 | 1.4 / 46 | 1.8 / 48 |
| CustomMemberConverterNoTimestamp | 0.0 / 243 | 0.1 / 83 | 0.1 / 75 | n/a | 2.4 / 81 | 1.0 / 45 | n/a |
| CustomPayload | 1.4 / 520 | 0.7 / 139 | 0.7 / 154 | 0.7 / 1,204 | 23.0 / 283 | 3.9 / 70 | 3.7 / 80 |
| CustomPayloadNoTimestamp | 0.0 / 340 | 0.6 / 110 | 0.6 / 116 | n/a | 20.6 / 290 | 4.2 / 65 | n/a |
| CustomRawPayload | 1.7 / 509 | 0.7 / 81 | 0.7 / 95 | 0.7 / 1,317 | 26.1 / 283 | 4.1 / 76 | 3.7 / 70 |
| CustomRawPayloadNoTimestamp | 0.0 / 284 | 0.6 / 72 | 0.6 / 72 | n/a | 18.8 / 261 | 4.1 / 71 | n/a |
| DigitalInputs | 1.7 / 322 | 0.2 / 81 | 0.1 / 79 | 0.2 / 470 | 8.3 / 69 | 1.1 / 52 | 1.6 / 41 |
| DigitalInputsNoTimestamp | 0.0 / 258 | 0.1 / 78 | 0.1 / 69 | n/a | 1.5 / 39 | 0.7 / 1 | n/a |
| EncoderMode | 1.4 / 437 | 0.3 / 76 | 0.3 / 83 | 0.3 / 458 | 9.3 / 85 | 2.4 / 45 | 1.8 / 43 |
| EncoderModeNoTimestamp | 0.0 / 150 | 0.4 / 75 | 0.3 / 72 | n/a | 7.6 / 41 | 1.9 / 2 | n/a |
| PortDIOSet | 1.2 / 223 | 0.2 / 82 | 0.1 / 71 | 0.2 / 495 | 8.1 / 84 | 1.5 / 44 | 1.7 / 39 |
| PortDIOSetNoTimestamp | 0.0 / 140 | 0.1 / 72 | 0.1 / 71 | n/a | 1.4 / 41 | 0.5 / 1 | n/a |
| PulseDO0 | 1.4 / 209 | 0.2 / 73 | 0.2 / 76 | 0.3 / 470 | 7.3 / 147 | 1.3 / 52 | 1.3 / 50 |
| PulseDO0NoTimestamp | 0.0 / 276 | 0.1 / 69 | 0.1 / 67 | n/a | 1.5 / 71 | 0.6 / 40 | n/a |
| PulseDOPort0 | 1.3 / 203 | 0.1 / 72 | 0.1 / 75 | 0.2 / 467 | 8.2 / 109 | 2.0 / 46 | 1.4 / 40 |
| PulseDOPort0NoTimestamp | 0.0 / 179 | 0.1 / 72 | 0.1 / 71 | n/a | 2.0 / 78 | 0.8 / 40 | n/a |
| StartPulse | 2.0 / 242 | 0.5 / 74 | 0.6 / 74 | 0.4 / 466 | 9.8 / 108 | 2.2 / 41 | 2.2 / 46 |
| StartPulseNoTimestamp | 0.0 / 252 | 0.4 / 73 | 0.4 / 77 | n/a | 7.8 / 57 | 1.3 / 39 | n/a |
| StartPulseTrain | 1.5 / 259 | 0.8 / 81 | 0.5 / 76 | 0.4 / 462 | 9.6 / 128 | 2.1 / 56 | 2.6 / 46 |
| StartPulseTrainNoTimestamp | 0.0 / 227 | 0.3 / 85 | 0.3 / 81 | n/a | 7.9 / 78 | 1.5 / 52 | n/a |
| Version | 5.5 / 848 | 2.6 / 92 | 2.2 / 78 | 2.3 / 812 | 28.6 / 697 | 5.0 / 122 | 6.2 / 143 |
| VersionNoTimestamp | 0.1 / 777 | 2.1 / 201 | 2.0 / 175 | n/a | 25.2 / 558 | 5.3 / 131 | n/a |

## 7. Tool checks (`.harp`, S3)

| Check | Result |
|-|-|
| presigned_http | {"ok": true, "ms": 547.1230999974068, "requests": 3} |
| s3fs_readahead | {"ok": true, "ms": 3120.629800003371} |
| s3fs_none | {"ok": true, "ms": 427.22300000605173} |
| tar_list_local | {"rc": 0} |
| zipfile_cli_test_local | {"rc": 0} |

## Notes and caveats

- Payloads are random bytes from the harness generator. Compression ratios, and Parquet encodings in particular, will differ on real recordings.
- Every register spans the same 0-329,999 s ramp, so register-level pruning by time range (skipping a whole register) is never exercised, only chunk-level pruning. The empty-window use case shows the all-registers-skipped case.
- 14 registers have object or categorical columns (custom version types, enums with undefined random values) that Arrow rejected; those columns were stringified before writing Parquet.
- The folder cannot promise monotonic timestamps, so time queries on it scan every file. A reader that trusted the first and last frame would be cheaper but not safe.
- Remote per-register operations are sequential; use-case runs also show an 8-thread mode. Remote `full` reads ran once per register (local five times), so they are noisier than the slice and time-range numbers.
- `mmap` has no remote equivalent. Remotely, every access is a ranged request with a round-trip latency floor.
- Slices of stored members make no CRC check, locally or remotely.
- Use cases: the first pass showed an isolated 15.8 s stall for `deflate1_chunked` loading 5 registers on S3 (same request count as `stored_chunked` at 4.3 s). Both chunked layouts were re-run in isolation and those rows replace the first-pass ones (see `remeasured` in `results/usecases_results.json`). Other S3 numbers are single passes of 2 runs, so expect similar occasional stalls.
- S3 numbers vary a lot between sessions. A first run of the per-register `.harp` benchmark, on a slower connection, measured a full register read at 1,573 ms against 220 ms in the final run. The per-register `.harp` figures here were re-measured in the same session as the Parquet and use-case runs (against the same S3 objects), so compare rows only within this report. The `.harp` upload times come from the first session, so they are not comparable with the Parquet upload times.