# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "matplotlib>=3.8",
#     "pandas>=2.0",
# ]
# ///
"""Benchmark report for the packed .harp format (harp-tech/protocol#251), as a marimo notebook.

Reads results/*.json (written by harp_archive_bench.py, parquet_bench.py, usecases_bench.py).

    uvx marimo edit --sandbox report.py                         # interactive
    uvx marimo export html --sandbox report.py -o report.html  # static file to share
"""

import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium", app_title="Packed .harp benchmarks")


@app.cell
def _():
    import json
    import subprocess
    from statistics import median

    import marimo as mo
    import matplotlib.pyplot as plt
    import pandas as pd
    return json, median, mo, pd, plt, subprocess


@app.cell
def _(json, mo, subprocess):
    root = mo.notebook_dir()
    H = json.loads((root / "results" / "harp_archive_results.json").read_text())
    P = json.loads((root / "results" / "parquet_results.json").read_text())
    U = json.loads((root / "results" / "usecases_results.json").read_text())
    commit = subprocess.run(["git", "-C", str(root), "log", "-1", "--format=%h", "--", "results"],
                            capture_output=True, text=True).stdout.strip() or "unknown"
    return H, P, U, commit


@app.cell
def _(H, P, plt):
    # Layout order, labels and colours. Colour encodes the format family only;
    # the axis label carries each layout's identity.
    HL = ["folder", "stored", "deflate1", "stored_chunked", "deflate1_chunked"]
    PV = list(P["info"])
    LAYOUTS = HL + [f"parquet {v}" for v in PV]
    FAMILY_COLOR = {"folder": "#8a8985", ".harp": "#2a78d6", "Parquet": "#eb6834"}

    def family(layout):
        return "folder" if layout == "folder" else "Parquet" if layout.startswith("parquet") else ".harp"

    TEXT, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "axes.titlecolor": TEXT,
        "axes.titlesize": 11, "axes.titleweight": "semibold", "axes.titlelocation": "left",
        "axes.spines.top": False, "axes.spines.right": False,
        "xtick.color": MUTED, "ytick.color": TEXT, "xtick.labelsize": 9, "ytick.labelsize": 9,
        "axes.grid": True, "axes.grid.axis": "x", "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.axisbelow": True, "font.size": 10, "legend.frameon": False, "legend.fontsize": 9,
    })
    chunk_rows = H["chunk_rows"]
    return FAMILY_COLOR, HL, LAYOUTS, MUTED, PV, TEXT, chunk_rows, family


@app.cell
def _(FAMILY_COLOR, LAYOUTS, MUTED, TEXT, family, plt):
    import math

    from matplotlib.patches import Patch
    from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

    def fmt(v):
        if v is None or v != v:
            return "-"
        if v >= 100:
            return f"{v:,.0f}"
        if v >= 10:
            return f"{v:.1f}"
        return f"{v:.2g}" if v < 1 else f"{v:.2f}"

    def bars(ax, values, title, xlabel, log=True, marks=None, layouts=None, show_labels=True):
        """Horizontal bars, one per layout, value printed at the bar end. marks: optional second value drawn as a tick."""
        layouts = layouts or [l for l in LAYOUTS if l in values]
        ys = list(range(len(layouts)))[::-1]
        vals = [values.get(l) for l in layouts]
        pos = [v for v in vals if v]
        # On a log axis, bars start one decade below the smallest value so every bar is visible.
        base = 10 ** math.floor(math.log10(min(pos))) if (log and pos) else 0
        ax.barh(ys, [(v - base) if v else 0 for v in vals], left=base, height=0.62,
                color=[FAMILY_COLOR[family(l)] for l in layouts], edgecolor="white", linewidth=1.5)
        if marks:
            mx = [(marks.get(l), y) for l, y in zip(layouts, ys) if marks.get(l) is not None]
            ax.scatter([m for m, _ in mx], [y for _, y in mx], marker="|", s=260, linewidths=2.2, color=TEXT, zorder=3)
        if log:
            ax.set_xscale("log")
        ax.set_yticks(ys, layouts if show_labels else [""] * len(layouts))
        ax.tick_params(axis="y", length=0)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        for v, y in zip(vals, ys):
            if v is not None:
                ax.annotate(fmt(v), (v, y), xytext=(4, 0), textcoords="offset points", va="center", fontsize=8.5, color=MUTED)
        if log and pos:
            top = max(pos + [m for m in (marks or {}).values() if m])
            ax.set_xlim(base, 10 ** math.ceil(math.log10(top) + 0.45))
            ax.xaxis.set_major_locator(LogLocator(base=10))
            ax.xaxis.set_minor_formatter(NullFormatter())
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}" if x >= 1 else f"{x:g}"))
        else:
            ax.margins(x=0.22)

    def family_legend(fig, extra=()):
        handles = [Patch(color=c, label=k) for k, c in FAMILY_COLOR.items()] + list(extra)
        fig.legend(handles=handles, loc="outside upper right", ncol=len(handles))

    def figure(rows, cols, h_per_row=3.3, w=15):
        return plt.subplots(rows, cols, figsize=(w, h_per_row * rows + 0.6), squeeze=False, layout="constrained")
    return bars, family_legend, figure


@app.cell
def _(H, P, U, commit, mo):
    _all = H["log"] + P["log"] + U["log"]
    bad = [r for r in _all if not r["ok"]]
    n_regs = len({r["reg"] for r in H["log"]})
    mo.md(f"""
    # Packed `.harp` vs folder vs Parquet, local and S3

    Benchmarks for the packed recording format proposed in
    [harp-tech/protocol#251](https://github.com/harp-tech/protocol/issues/251).

    - **Data:** {n_regs} timestamped registers from `harp.benchmarks`, {H['entries']:,} frames each,
      **synthetic random payloads** on a 1 row/second timestamp ramp.
    - **Correctness:** {len(_all) - len(bad):,}/{len(_all):,} timed operations returned data identical to the source.
    - **Code and raw results:** commit `{commit}`. Data on S3: `{H['prefix']}`.
    - **How to read the charts:** every chart uses a log scale unless stated, and colour shows the format family
      (grey folder, blue `.harp`, orange Parquet). Each section has a table with the exact numbers.
      S3 timings vary between sessions.
    """)
    return (bad,)


@app.cell
def _(chunk_rows, mo):
    mo.accordion({
        "What the layout names mean": mo.md(f"""
    | Name | What it is on disk / S3 | Compression | Metadata |
    |-|-|-|-|
    | `folder` | The current format: a directory with one `.bin` per register (raw Harp frames); on S3, one object per register | none | none; a reader must read the file to learn its row count and time range |
    | `stored` | One `.harp` zip archive, one member per register. "Stored" is zip's term for no compression, so members are byte-identical to the `.bin` files | none | zip comment per member: `{{stride, rows, tmin, tmax, monotonic}}` |
    | `deflate1` | The same archive, members deflated at level 1 (fastest) | deflate | same |
    | `stored_chunked` | As `stored`, each register split into {chunk_rows:,}-row time chunks, one member per chunk (`Name_addr_000.bin`, ...) | none | comment per chunk, so a reader can skip chunks by time |
    | `deflate1_chunked` | Chunked and deflated | deflate | comment per chunk |
    | `parquet snappy_rg50k`, `parquet zstd_rg50k` | Not a Harp format: one Parquet file per register, decoded columns plus `Time`, 50,000-row row groups | snappy / zstd | row-group statistics in the footer |
    | `parquet zstd_rgall` | Parquet, zstd, one row group per register | zstd | footer |
    """),
        "Locations and modes": mo.md("""
    - **local:** reads from disk. Stored archives use `mmap`, the folder uses seek/read.
    - **S3:** ranged `GetObject` requests through the same reader code as local, so requests and bytes are counted.
      For Parquet, **S3 native** uses `pyarrow.fs.S3FileSystem` (its own request coalescing, timing only).
    - **sequential / 8 threads:** use cases on S3 run once with one request at a time and once with 8 threads.
    """),
        "Use cases and per-register operations": mo.md("""
    | Use case | Task (measured cold: opening the recording is part of the cost) |
    |-|-|
    | Time window, all registers | 1,000 rows between two times, from every register |
    | Window past the end | The same query after the end of the recording. Can metadata alone answer it? |
    | Load 5 registers | DigitalInputs, AnalogData, Counter0, PulseDO0, StartPulse into DataFrames |
    | First and last timestamp | Earliest and latest timestamp in the recording, from metadata where the format has it |
    | Same, plus the messages | The same, plus fetching the two messages |

    | Operation | What it does |
    |-|-|
    | `layout` | Metadata lookup: row count, frame stride, first and last timestamp. Archive: zip comments, already loaded at open. Folder: 3 reads per file. Parquet: footer |
    | `full` | A whole register. `.harp`: bytes plus frame parsing, no decoding. Parquet: an Arrow table of decoded columns |
    | `slice` | Rows 100,000-101,000 by index, to a DataFrame |
    | `slice_x` | Rows 49,500-50,500, across the boundary between chunk or row group 0 and 1 |
    | `timeq` | The 1,000 rows between two timestamps: skip chunks by time range, bisect where monotonic, otherwise scan (folder) |
    """),
    })
    return


@app.cell
def _(HL, PV, U, pd):
    # Use-case results as one tidy table. "s3" means the counted reader for every layout.
    _rows = []
    for r in U["log"]:
        layout = r["layout"].replace("parquet_", "parquet ")
        loc = {"s3_counted": "s3"}.get(r["loc"], r["loc"])
        _rows.append(dict(uc=r["uc"], layout=layout, loc=loc, mode=r["mode"], ms=r["ms"],
                          requests=r.get("requests"), mb=(r.get("bytes") or 0) / 1e6 if r.get("bytes") is not None else None))
    uc = pd.DataFrame(_rows)
    UCT = {"uc1_window_all": "Time window, all registers",
           "uc1_window_empty": "Window past the end",
           "uc2_full5": "Load 5 whole registers",
           "uc3_bounds_meta": "First and last timestamp",
           "uc3_bounds_msgs": "Same, plus the two messages"}
    LAY_ORDER = HL + [f"parquet {v}" for v in PV]

    def ucv(loc, mode, col, name):
        d = uc[(uc.uc == name) & (uc["loc"] == loc) & (uc["mode"] == mode)]
        return {r.layout: r[col] for _, r in d.iterrows() if r[col] is not None and r[col] == r[col]}
    return LAY_ORDER, UCT, uc, ucv


@app.cell
def _(mo):
    mo.md("""
    ## 1. Use cases on S3

    End-to-end tasks, opening the recording included. Bars are sequential requests; the black tick is the same task with 8 threads.
    """)
    return


@app.cell
def _(Line2D_, TEXT, UCT, bars, family_legend, figure, ucv):
    fig1, ax1 = figure(1, 5, h_per_row=3.6)
    for _i, (_name, _title) in enumerate(UCT.items()):
        bars(ax1[0, _i], {k: v / 1e3 for k, v in ucv("s3", "seq", "ms", _name).items()}, _title, "seconds",
             marks={k: v / 1e3 for k, v in ucv("s3", "par", "ms", _name).items()}, show_labels=_i == 0)
    family_legend(fig1, [Line2D_([], [], marker="|", ls="", color=TEXT, markersize=12, markeredgewidth=2, label="8 threads")])
    fig1
    return


@app.cell
def _():
    from matplotlib.lines import Line2D as Line2D_
    return (Line2D_,)


@app.cell
def _(mo):
    mo.md("""
    ### Requests and bytes per use case (S3, sequential)
    """)
    return


@app.cell
def _(UCT, bars, family_legend, figure, ucv):
    fig2, ax2 = figure(2, 5, h_per_row=3.3)
    for _i, (_name, _title) in enumerate(UCT.items()):
        bars(ax2[0, _i], ucv("s3", "seq", "requests", _name), _title, "requests", show_labels=_i == 0)
        bars(ax2[1, _i], ucv("s3", "seq", "mb", _name), "", "MB fetched", show_labels=_i == 0)
    family_legend(fig2)
    fig2
    return


@app.cell
def _(mo):
    mo.md("""
    ### The same use cases, local disk
    """)
    return


@app.cell
def _(UCT, bars, family_legend, figure, ucv):
    fig3, ax3 = figure(1, 5, h_per_row=3.6)
    for _i, (_name, _title) in enumerate(UCT.items()):
        bars(ax3[0, _i], ucv("local", "seq", "ms", _name), _title, "milliseconds", show_labels=_i == 0)
    family_legend(fig3)
    fig3
    return


@app.cell
def _(LAY_ORDER, UCT, mo, pd, uc):
    _t = uc.copy()
    _t["use case"] = _t.uc.map(UCT)
    _t["column"] = _t["loc"] + " " + _t["mode"]
    _wide = _t.pivot_table(index=["use case", "layout"], columns="column", values="ms", aggfunc="first")
    _s3 = _t[(_t["loc"] == "s3") & (_t["mode"] == "seq")].set_index(["use case", "layout"])[["requests", "mb"]]
    _wide = _wide.join(_s3.rename(columns={"requests": "S3 requests (seq)", "mb": "S3 MB (seq)"}))
    _wide = _wide.reset_index()
    _wide["layout"] = pd.Categorical(_wide["layout"], LAY_ORDER, ordered=True)
    _wide["use case"] = pd.Categorical(_wide["use case"], list(UCT.values()), ordered=True)
    uc_table = _wide.sort_values(["use case", "layout"]).round(1)
    mo.accordion({"Use-case numbers (ms)": mo.ui.table(uc_table, selection=None, page_size=40)})
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Size and upload

    Payloads are uniformly random bytes (see the notes).
    """)
    return


@app.cell
def _(H, HL, P, PV, pd):
    _raw = H["info"]["folder"]["bytes"]
    _rows = []
    for _l in HL:
        _i = H["info"][_l]
        _rows.append(dict(layout=_l, MB=_i["bytes"] / 1e6, vs_raw=_i["bytes"] / _raw,
                          pack_s=None if _l == "folder" else _i["pack_s"] + _i["verify_s"],
                          upload_s=_i.get("upload_s"), objects=_i.get("upload_objects")))
    for _v in PV:
        _i = P["info"][_v]
        _rows.append(dict(layout=f"parquet {_v}", MB=_i["bytes"] / 1e6, vs_raw=_i["bytes"] / _raw,
                          pack_s=sum(x["write_s"] for x in _i["files"].values()),
                          upload_s=_i.get("upload_s"), objects=_i.get("upload_objects")))
    size = pd.DataFrame(_rows)
    return (size,)


@app.cell
def _(bars, family_legend, figure, mo, size):
    fig4, ax4 = figure(1, 2, h_per_row=3.4, w=12)
    bars(ax4[0, 0], dict(zip(size.layout, size.vs_raw)), "Size relative to the folder", "x of raw", log=False)
    bars(ax4[0, 1], dict(zip(size.layout, size.upload_s)), "Upload time (aws s3 cp)", "seconds", log=False, show_labels=False)
    family_legend(fig4)
    mo.vstack([fig4, mo.accordion({"Size numbers": mo.ui.table(size.round(2), selection=None)})])
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. Open the recording and learn every register's layout

    What it costs, once per recording, to know every register's row count and time range.
    The folder needs a listing plus 3 reads per register. The archive needs one tail request carrying
    the central directory and all the metadata. Parquet needs one footer read per file.
    """)
    return


@app.cell
def _(H, HL, P, PV, pd):
    _rows = []
    for _l in HL:
        _ol, _os = H["opens"][f"local/{_l}"], H["opens"][f"s3/{_l}"]
        _ll = [r for r in H["log"] if r["loc"] == "local" and r["layout"] == _l and r["op"] == "layout"]
        _ls = [r for r in H["log"] if r["loc"] == "s3" and r["layout"] == _l and r["op"] == "layout"]
        _rows.append(dict(layout=_l, local_ms=_ol["ms"] + sum(r["ms"] for r in _ll),
                          s3_ms=_os["ms"] + sum(r["ms"] for r in _ls),
                          s3_requests=_os["requests"] + sum(r["requests"] for r in _ls),
                          s3_kb=(_os["bytes"] + sum(r["bytes"] for r in _ls)) / 1e3))
    for _v in PV:
        _ol, _oc, _on = P["opens"][f"local/{_v}"], P["opens"][f"s3_counted/{_v}"], P["opens"][f"s3_native/{_v}"]
        _rows.append(dict(layout=f"parquet {_v}", local_ms=_ol["ms_all"], s3_ms=_oc["ms_all"],
                          s3_native_ms=_on["ms_all"], s3_requests=_oc["requests_all"], s3_kb=None))
    opens = pd.DataFrame(_rows)
    return (opens,)


@app.cell
def _(bars, family_legend, figure, mo, opens):
    fig5, ax5 = figure(1, 3, h_per_row=3.4)
    bars(ax5[0, 0], dict(zip(opens.layout, opens.local_ms)), "Local", "milliseconds")
    bars(ax5[0, 1], dict(zip(opens.layout, opens.s3_ms)), "S3", "milliseconds", show_labels=False)
    bars(ax5[0, 2], dict(zip(opens.layout, opens.s3_requests)), "S3 requests", "requests", show_labels=False)
    family_legend(fig5)
    mo.vstack([fig5, mo.accordion({"Open and layout numbers": mo.ui.table(opens.round(1), selection=None)})])
    return


@app.cell
def _(H, HL, P, PV, median, pd):
    def agg(log, loc, layout, op, key, ts_only=False):
        v = [r[key] for r in log if r["loc"] == loc and r["layout"] == layout and r["op"] == op
             and r[key] is not None and (not ts_only or r["timestamped"])]
        return median(v) if v else None

    OPS = {"full": "Full register", "slice": "Slice, 1,000 rows", "slice_x": "Slice across a chunk boundary",
           "timeq": "Time range, 1,000 rows"}
    _rows = []
    for _op in OPS:
        for _l in HL:
            _b = agg(H["log"], "s3", _l, _op, "bytes")
            _rows.append(dict(op=_op, layout=_l, local_ms=agg(H["log"], "local", _l, _op, "ms"),
                              s3_ms=agg(H["log"], "s3", _l, _op, "ms"), s3_requests=agg(H["log"], "s3", _l, _op, "requests"),
                              s3_mb=None if _b is None else _b / 1e6, s3_native_ms=None))
        for _v in PV:
            _b = agg(P["log"], "s3_counted", _v, _op, "bytes")
            _rows.append(dict(op=_op, layout=f"parquet {_v}", local_ms=agg(P["log"], "local", _v, _op, "ms"),
                              s3_ms=agg(P["log"], "s3_counted", _v, _op, "ms"),
                              s3_requests=agg(P["log"], "s3_counted", _v, _op, "requests"),
                              s3_mb=None if _b is None else _b / 1e6, s3_native_ms=agg(P["log"], "s3_native", _v, _op, "ms")))
    ops = pd.DataFrame(_rows)
    return OPS, ops


@app.cell
def _(mo):
    mo.md("""
    ## 4. Per-register operations

    Median over the registers of each register's median time, on an already-opened store.
    `.harp` full reads stop before decoding; Parquet returns decoded columns. Section 5 adds the decode step.
    """)
    return


@app.cell
def _(OPS, bars, family_legend, figure, mo, ops):
    fig6, ax6 = figure(3, 4, h_per_row=3.2)
    for _j, (_op, _title) in enumerate(OPS.items()):
        _d = ops[ops.op == _op]
        bars(ax6[0, _j], dict(zip(_d.layout, _d.local_ms)), f"{_title}\nlocal", "milliseconds", show_labels=_j == 0)
        bars(ax6[1, _j], dict(zip(_d.layout, _d.s3_ms)), "S3", "milliseconds", show_labels=_j == 0)
        bars(ax6[2, _j], dict(zip(_d.layout, _d.s3_mb)), "S3 MB fetched", "MB", show_labels=_j == 0)
    family_legend(fig6)
    mo.vstack([fig6, mo.accordion({"Per-register operation numbers": mo.ui.table(ops.round(3), selection=None, page_size=40)})])
    return


@app.cell
def _(H, P, PV, median, mo, pd):
    dec = P["decode_ms"]

    def harp_df(loc, layout):
        return median(r["ms"] + dec[r["reg"]] for r in H["log"]
                      if r["loc"] == loc and r["layout"] == layout and r["op"] == "full" and r["ms"] is not None)

    def pq_df(loc, v):
        return median(r["ms"] for r in P["log"] if r["loc"] == loc and r["layout"] == v and r["op"] == "full_df" and r["ms"] is not None)

    _rows = [dict(layout=l, local_ms=harp_df("local", l), s3_ms=harp_df("s3", l)) for l in ["folder", "stored", "deflate1"]]
    _rows += [dict(layout=f"parquet {v}", local_ms=pq_df("local", v), s3_ms=pq_df("s3_counted", v)) for v in PV]
    lfl = pd.DataFrame(_rows)
    mo.md(f"""
    ## 5. Like for like: whole register to a DataFrame

    `.harp` keeps raw frames, so a DataFrame costs the read plus `parse_to_dataframe`. The decode is location-independent,
    with median {median(dec.values()):.0f} ms and maximum {max(dec.values()):,.0f} ms over registers.
    """)
    return (lfl,)


@app.cell
def _(bars, family_legend, figure, lfl, mo):
    fig7, ax7 = figure(1, 2, h_per_row=3.0, w=12)
    bars(ax7[0, 0], dict(zip(lfl.layout, lfl.local_ms)), "Local (read + decode)", "milliseconds", layouts=list(lfl.layout))
    bars(ax7[0, 1], dict(zip(lfl.layout, lfl.s3_ms)), "S3 (read + decode)", "milliseconds", layouts=list(lfl.layout), show_labels=False)
    family_legend(fig7)
    mo.vstack([fig7, mo.accordion({"Like-for-like numbers": mo.ui.table(lfl.round(1), selection=None)})])
    return


@app.cell
def _(H, P, mo, pd):
    _rows = {}
    for _r in H["log"]:
        if _r["layout"] == "stored" and _r["op"] != "layout":
            _rows.setdefault(_r["reg"], {})[f"harp {_r['op']} {_r['loc']}"] = _r["ms"]
    for _r in P["log"]:
        if _r["layout"] == "zstd_rg50k" and _r["op"] in ("full_df", "slice", "timeq"):
            _rows.setdefault(_r["reg"], {})[f"pq {_r['op']} {_r['loc'].replace('s3_counted', 's3')}"] = _r["ms"]
    detail = pd.DataFrame.from_dict(_rows, orient="index")
    detail = detail[sorted(detail.columns)].round(1).rename_axis("register").reset_index()
    mo.vstack([
        mo.md("## 6. Per-register detail\n\nStored archive and Parquet `zstd_rg50k`, milliseconds."),
        mo.ui.table(detail, selection=None, page_size=20),
    ])
    return


@app.cell
def _(H, json, mo):
    _rows = "\n".join(f"| `{k}` | `{json.dumps(v)}` |" for k, v in H["extras"].items())
    mo.md("## 7. Tool checks (`.harp` on S3)\n\n| Check | Result |\n|-|-|\n" + _rows)
    return


@app.cell
def _(P, PV, mo):
    n_str = sum(1 for x in P["info"][PV[0]]["files"].values() if x["stringified"])
    mo.md(f"""
    ## Notes and caveats

    - Payloads are random bytes, not a real recording.
    - Every register spans the same time range, so skipping a whole register by time is only exercised by the
      window-past-the-end use case.
    - {n_str} registers have object or categorical columns that Arrow rejected; those columns were stringified for Parquet.
    - The folder cannot promise monotonic timestamps, so time queries on it scan every file.
    - Remote per-register operations are sequential. Remote `full` reads ran once per register, so they are noisier.
    - `mmap` is local only; remotely every access is a ranged request with a round-trip latency floor.
    - Slices of stored members make no CRC check.
    - All three benchmarks ran in one session against the same objects. An earlier session measured the folder
      time-window query at about a third of this session's time.
    - One pyarrow native-S3 call hung for about three hours in the first pass (probably expired credentials);
      the `s3_native/snappy_rg50k` stage was re-measured and replaces it.
    """)
    return


@app.cell
def _(bad, mo):
    _out = mo.md("## Failures\n\n" + "\n".join(f"- {r['loc']}/{r['layout']}/{r.get('reg', r.get('uc'))}: {r['err']}" for r in bad[:40])) if bad else mo.md("")
    _out
    return


if __name__ == "__main__":
    app.run()
