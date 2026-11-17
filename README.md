# harp-new-format-benchmarks

Benchmarks of the packed `.harp` recording format proposed in [harp-tech/protocol#251](https://github.com/harp-tech/protocol/issues/251),
read locally and from S3, against the current folder format and against Parquet.

The data comes from the `harp.benchmarks` harness in [harp-tech/python](https://github.com/harp-tech/python) (30 register fixtures,
15 timestamped and 15 untimestamped twins, 330,000 frames each, synthetic random payloads). The full results are in
[`report.md`](report.md), the raw results in [`results/`](results/).

## Layouts compared

| Layout | What it is |
|-|-|
| `folder` | Current format: one `.bin` per register |
| `stored`, `deflate1` | One `.harp` zip archive with every register, members stored or deflated (level 1) |
| `stored_chunked`, `deflate1_chunked` | The same, with each register split into 50,000-row time chunks, one zip member per chunk |
| `parquet snappy_rg50k`, `zstd_rg50k`, `zstd_rgall` | One Parquet file per register, decoded columns plus a `Time` column; row groups of 50k rows, or one row group |

Each `.harp` member's zip comment holds `{stride, rows, tmin, tmax, monotonic}`, the archive comment holds `{"harp":1}`, as in the issue.

## What is measured

**Use cases** (`usecases_bench.py`), each cold, opening the recording included:

- all events between two times from every register, and the same for a window past the end of the recording;
- loading five whole registers into DataFrames;
- the earliest and latest timestamp in the recording, from metadata only, and with the two messages fetched.

**Per-register operations** (`harp_archive_bench.py`, `parquet_bench.py`): layout lookup, full read, a 1,000-row slice, a slice across
a chunk or row-group boundary, and a 1,000-row time range. Every result is verified against the source bytes or values.

Streaming uses ranged S3 `GetObject` requests through one reader shared by local and remote (so requests and bytes are counted), a presigned
URL, `s3fs`, and for Parquet also `pyarrow.fs.S3FileSystem`.

## Running

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), the AWS CLI with working credentials, and a bucket and prefix you can write to.

```bash
uv sync
B=<bucket>; P=<key/prefix>; RUN=bench-1

# 1. folder and .harp layouts: generate corpus, pack, upload, measure, local and S3
uv run python harp_archive_bench.py --out out/harp --run-id $RUN --bucket $B --prefix $P

# 2. Parquet: convert the same corpus, upload, measure
uv run python parquet_bench.py --corpus out/harp/corpus --out out/pq --run-id $RUN --bucket $B --prefix $P

# 3. use cases, over the data uploaded by the two steps above
uv run python usecases_bench.py --corpus out/harp/corpus --harp-dir out/harp --parquet-dir out/pq/parquet \
    --out out/uc --run-id $RUN --bucket $B --prefix $P

# 4. report
python report.py out/harp/results.json out/pq/parquet_results.json out/uc/usecases_results.json report.md
```

Use `--only <register names>` on the first two scripts for a quick trial, and `--only-uc` / `--only-layout` on the third.

## Caveats

- The payloads are random, so compression ratios do not represent real recordings.
- Every register covers the same time range, so skipping a whole register by time range is only exercised by the empty-window use case.
- S3 timings vary between sessions and even within one (one stall had to be re-measured, see `report.md`). Compare rows only within a report.
- Remote reads are sequential unless a mode says otherwise.
