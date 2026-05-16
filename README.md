# diffman

**Difference manager for simulation pipelines.**

Tracks the graph of how pipelines are *forked* from one another and
shows the parameter differences at each fork. Browses runs produced by
those pipelines via a web UI with SRW-aware previews of `.h5`/`.dat`
artifacts.

diffman does **not** launch runs. Pipelines run themselves (your script,
your scheduler); diffman observes the resulting run directories and
explains how variants differ.

```
       pipe_a (root)
       ├── variants: base, jitter
       └── pipe_b (fork)
           ├── parent=pipe_a
           ├── variants: base, jitter_renamed (forks_of='jitter'), extra
           └── pipe_c (fork-of-fork)
               └── parent=pipe_b
```

Spun out from `srwl_uti_diffman.py` in
[SRW](../xpp_nnl_dataset_gpu/smp_to_det/SRW); that single-file
stdlib-only version still ships there. This package is the richer
evolution with heavier deps.

## Install

### pixi (recommended)

Provisions a project-local conda+pypi env including `srwpy`, so SRW
previews work out of the box:

```bash
pixi install
pixi run scan
pixi run serve              # http://127.0.0.1:8765
pixi shell                  # or drop in and use the `diffman` CLI
```

### pip

```bash
pip install -e ".[all]"     # core + h5py + plotly
pip install srwpy           # optional: SRW-aware previews of .h5 / .dat
```

## Concepts

- **Pipeline** — `dm.Pipeline('myname', stages, parent='other_pipeline')`.
  The `name` becomes the top-level run-directory name. The optional
  `parent` is the name of the pipeline this one was forked from; it's
  the entire fork-graph mechanism.
- **Variant** — `dm.register('jitter', base='base', probe=dict(...))`.
  A named config layer with optional inheritance. The variant registry
  is keyed on `(module, name)` so two pipelines can each register a
  variant called `base`.
- **Fork** — a hand-written pipeline `.py` that declares another
  pipeline as its `parent`. diffman computes the per-variant config
  diff against that parent.
- **`forks_of=`** — on `dm.register`, declares that a variant is a
  renamed descendant of one in the parent pipeline. Used when the diff
  should line up despite a rename. Unresolved targets are flagged in
  the UI rather than silently falling back to name-match.
- **Run** — produced by your code calling `Pipeline.run(variant,
  RunRegistry(...))`. diffman discovers the directory and serves it.

## Writing a pipeline

```python
import diffman as dm

dm.register('base',   scan=dict(width=5e-6, step=1e-7))
dm.register('jitter', base='base', probe=dict(jitter=True, amp=0.1))

def _sim(ctx):
    cfg = ctx.variant.config
    # ... do work; write artifacts under ctx.artifact('sim', 'out.npy')
    return {'out': ctx.artifact('sim', 'out.npy')}

PIPELINE = dm.Pipeline('mysim', [
    dm.Stage('sim', _sim, config_keys=('scan', 'probe')),
])
```

A fork of the above:

```python
import diffman as dm

dm.register('base',   scan=dict(width=8e-6, step=1e-7))     # width tweaked
dm.register('jitter', base='base', probe=dict(jitter=True, amp=0.5))
# New variant unique to the fork:
dm.register('hires',  base='jitter', scan=dict(step=1e-8))

PIPELINE = dm.Pipeline('mysim_v2', [
    dm.Stage('sim', _sim, config_keys=('scan', 'probe')),
], parent='mysim')
```

To run a variant, *you* invoke it (diffman doesn't):

```python
import diffman as dm
import mysim_v2

rr = dm.RunRegistry(root='runs')
mysim_v2.PIPELINE.run(dm.registry.get('mysim_v2', 'jitter'), rr)
```

## Web UI

`diffman serve --root runs --scan-root .` starts the read-only viewer.

- **Sidebar** — the fork forest. Indentation reflects parent→child;
  orphans (pipelines whose `parent=` doesn't match anything in the scan)
  are flagged.
- **Pipeline page** — variants list, plus a "Differences vs parent"
  table when a parent is declared. Per-variant entries are one of
  `matches` / `differs` / `only_in_child` / `only_in_parent`, with the
  exact config keys that changed. Buttons:
  - **Source diff vs parent** — unified text diff of the two `.py` files.
  - **Compare across pipelines** — pick N pipelines and a variant name;
    get a per-key table comparing the merged configs side-by-side.
- **Variant page** — what this variant adds on top of its inheritance
  base, plus its resolved config.
- **Run page** — stage status, per-stage artifacts. Each artifact has a
  **Diff vs…** button that opens a `(pipeline → variant → run)` picker
  and renders a numerical or text diff against the chosen run's
  matching artifact (numpy stats + delta heatmap, JSON structural diff,
  or unified text diff).
- **Find** — a fingerprint-prefix search box (sidebar) that maps from
  a `short_fp` back to its variant and runs.

The UI auto-refreshes when you edit a pipeline `.py` (filesystem watcher
on `--scan-root`) or when new runs appear (watcher on `--root`).

## CLI

```
diffman scan [root]                 # discover pipeline modules
diffman list <module>               # variant names registered by that module
diffman describe <module> <variant> # resolved config + fingerprint
diffman serve [--root DIR] [--port N] [--scan-root DIR] [--bind ADDR]
```

`diffman --help` and `diffman <cmd> --help` have the full text.

## REST endpoints

```
GET  /api/pipelines                             → fork forest
GET  /api/variants?module=<name>                → variant names
GET  /api/describe?module=&variant=             → variant config + fp
GET  /api/variant_overrides?module=&variant=    → vs the variant's base
GET  /api/diff?module=<name>                    → variant-by-variant vs parent
GET  /api/source_diff?module=<name>             → unified diff vs parent .py
GET  /api/compare?modules=a,b,c&variant=v       → N-way per-key compare
GET  /api/find?q=<fp-prefix>                    → variants/runs by fp
GET  /api/artifact_diff?path_a=&path_b=         → numerical/text/json diff
GET  /api/runs                                  → all run records
GET  /api/run/{p}/{v}/{fp}                      → single run + stages
GET  /api/stage/{p}/{v}/{fp}/{stage}            → stage detail + artifacts
GET  /api/render?path=                          → renderer payload
GET  /api/srw_preview?path=                     → SRW heatmap + h/v cuts
GET  /artifact/{p}/{v}/{fp}/{rest}              → raw artifact download
WS   /ws                                        → run_changed / pipelines_changed
```

## On-disk layout

```
<runs-root>/
    <pipeline>/<variant>/<short-fp>/
        run.json            # RunRecord — all state is JSON
        config.json         # resolved variant config
        run.log
        stages/<stage>/
            outputs/...     # artifacts (any format)
            _meta.json
            _key            # cache key string
    _scripts/.git/          # auto-snapshotted pipeline .py per run
```

All app-managed state is JSON or git — text-friendly, mergeable, and
inspectable without a database.

## Tests

```
pixi run test               # pytest tests/
```
