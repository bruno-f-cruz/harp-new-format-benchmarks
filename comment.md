I benchmarked the packed `.harp` format proposed here against the current folder format and against Parquet, reading locally and from S3. The full results are in the attached `report.html`. The code and raw results are at commit `d7833d5`.

## Setup

- 15 timestamped registers from the `harp.benchmarks` harness, 330,000 frames each, with **synthetic random payloads**. No real recording was available.
- Layouts:
  - `folder`, the current format.
  - `stored` and `deflate1`: one `.harp` zip archive, members uncompressed or deflated at level 1.
  - `stored_chunked` and `deflate1_chunked`: the same, with each register split into 50,000-row chunks.
  - Parquet (snappy or zstd, 50k-row row groups or one row group) as an outside reference.
- Each archive member's zip comment holds `{stride, rows, tmin, tmax, monotonic}` as proposed, and the archive comment holds `{"harp":1}`.
- S3 reads use ranged `GetObject` through the same reader code as local reads, so requests and bytes are counted. Every result was checked against the source, and 1,575 of 1,575 operations matched.
- All formats ran in one session. S3 timings vary between sessions, so the ratios matter more than the absolute numbers.

## Results (S3, sequential, seconds)

| Use case | folder | stored | deflate1 chunked | Parquet zstd 50k |
|-|-:|-:|-:|-:|
| 1,000 rows from every register in a time window | 30.6 | 10.1 | 4.4 | 5.8 |
| Same, 8 threads | 12.4 | 2.0 | 1.7 | 2.3 |
| Window past the end of the recording | 31.0 | 0.15 | 0.12 | 1.6 |
| Earliest and latest timestamp | 2.1 | 0.15 | 0.13 | 1.6 |
| Load 5 whole registers | 8.4 | 8.0 | 7.7 | 6.5 |

- **Opening and learning every register's row count and time range** takes 1 request (87 ms) for the archive. The folder needs 46 requests (2.2 s) and Parquet 15 (1.3 s).
- **An empty window** is answered from the central directory (0.26 MB) without touching data. The folder reads 98 MB to find out there is nothing there.
- **Single-register slices and time ranges** (median over registers):
  - Time range: Parquet 135 ms, `deflate1_chunked` 143 ms, `stored_chunked` 313 ms, unchunked `stored` 685 ms, folder 1.6 s.
  - Row slices on stored members are one ranged read (about 90 ms).
- **A whole register to a DataFrame:** stored plus decode takes 1.0 s against 0.5 to 0.8 s for Parquet. The decode itself has a median of 9 ms and a maximum of 1 s (`Version`).
- **Size relative to the folder:** deflate1 is 0.57x, and Parquet zstd 0.54 to 0.63x. Random payloads make this pessimistic; the issue measured about 3x on real data.
- **Locally,** every format answers these queries in milliseconds.

## My take

1. **The proposal earns its place through the metadata, not the zip container.** Time ranges, row counts and "is there anything in this window?" come from the central directory in one request. A folder has to read every file because it cannot promise a time range or monotonic order.
2. **Chunking matters for remote reads.**
   - An unchunked stored archive does one bisection round trip per register and step, so a single-register time query takes 685 ms against 143 to 313 ms for the chunked layouts.
   - A deflated member is read whole, so without chunks every slice downloads the entire register.
   - Chunks let a reader skip data from metadata and read in parallel.
3. **Deflate level 1 is a good default when combined with chunking.** It halves the bytes moved here, and more on real data.
4. **Parquet is faster for single-register slices on S3, but that is not an argument to switch.** It is a different data model: it needs a conversion step, it drops the raw frames, and it needed workarounds for object and categorical columns. `.harp` keeps the existing register files, and the only extra cost on the reader side is decoding.
5. **Loading whole registers is bandwidth-bound** and doesn't separate the formats much.

## On the open question of chunk size

If chunks are sized in rows (or in bytes rounded to whole frames), each chunk covers a different time span. A few extra comment fields would then help:

| Field | Why |
|-|-|
| `row0`, the register-wide index of the chunk's first row | Row slices go straight to the right chunk, and a missing chunk can be detected |
| Chunk count, or a last-chunk flag | Tells a complete archive from a truncated one |
| Tick-exact `tmin` / `tmax` (seconds plus 32 µs ticks, as integers) | Avoids float rounding when skipping chunks at a boundary |
| Optional: median row interval and maximum gap | Enables interpolation search, about 2 to 3 reads instead of about 16 for bisection |
| Optional: a sparse time index (every Nth timestamp) | One range read per time query; must fit the 65,535-byte comment limit |
| `chunk_rows` in the archive comment | A hint for readers and tools |

Size, CRC, name and address are already in the zip entry and the filename.

**Readers don't need bisection to start with.** With chunks, a time query skips non-overlapping chunks and reads the one or two that remain in full. `deflate1_chunked` already works that way, since it cannot bisect, and it had the best archive time-range result. Bisection, interpolation and a sparse index are reader-side optimisations that can come later without breaking existing files, as long as readers ignore unknown comment keys.

What is hard to change later is the chunk granularity, the per-chunk `monotonic` flag, the time encoding and `row0`. The thing I would avoid is making chunking optional with no default size, because unchunked archives are where skipping bisection hurts.

## Not covered

- Real recordings: compression ratios, and skipping whole registers by time range (here every register spans the same time range).
- Variable-length registers (#220), which don't fit the stride-based layout.
- The writer's flush and pack step.
- A comparison against the production harp-python reader as the folder baseline.
