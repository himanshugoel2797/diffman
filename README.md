# diffman

**Difference manager for simulation pipelines.**

[![PyPI version](https://img.shields.io/pypi/v/diffman.svg)](https://pypi.org/project/diffman/)
[![Python versions](https://img.shields.io/pypi/pyversions/diffman.svg)](https://pypi.org/project/diffman/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

diffman tracks the graph of how simulation pipelines are *forked* from
one another and shows the parameter differences at each fork. It
discovers runs produced by those pipelines and serves them through a
read-only web UI with previews for common artifact formats
(`.npy`, `.json`, `.h5`, images, plain text).

diffman does **not** launch runs. Your script (or scheduler) runs the
pipeline; diffman observes the resulting run directories and explains
how variants relate to one another.

## Highlights

- **Fork-aware** — declare `parent=` on a pipeline and get per-variant
  config diffs against the parent, plus a unified source diff of the
  two pipeline `.py` files.
- **Variant inheritance** — `dm.register(name, base=..., ...)` layered
  config with cross-pipeline rename tracking (`forks_of=`).
- **Live UI** — file-watcher driven, no reloads, with N-way
  cross-pipeline compare and fingerprint-prefix search.
- **Artifact diffs** — pick any artifact and diff it against the
  matching artifact in another run: numpy stats + delta heatmap, JSON
  structural diff, or unified text diff.
- **No database** — all state is JSON-on-disk plus a snapshot git repo
  of pipeline `.py` files per run. Inspectable with `cat` and `git
  log`.

```
       pipe_a (root)
       ├── variants: base, jitter
       └── pipe_b (fork)
           ├── parent=pipe_a
           ├── variants: base, jitter_renamed (forks_of='jitter'), extra
           └── pipe_c (fork-of-fork)
               └── parent=pipe_b
```

## Install

```bash
pip install diffman              # core
pip install "diffman[all]"       # + h5py + plotly previews
```

### From source

```bash
git clone https://github.com/dotnet00/diffman
cd diffman
pip install -e ".[all,dev]"
```

### pixi

A `pixi` workspace is included for users who prefer a project-local
conda+pypi environment:

```bash
pixi install
pixi run serve              # http://127.0.0.1:8765
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
    # Run your tool wherever it writes naturally:
    tmp = '/tmp/out.npy'
    # ... produce `tmp` ...
    # Register it under stages/sim/outputs/out.npy (symlink or copy):
    ctx.artifact('sim', 'out.npy', tmp)

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

## Chaining pipelines

A `Chain` declares a DAG of pipelines and the upstream/downstream
edges between them. A `Variation` picks a coherent tuple of variants
across the chain — one per step — so `baseline`, `jitter_low`,
`algo_dm` etc. each refer to a consistent slice through the chain.
Diffman doesn't execute the chain; calling
`chain.variations[name].run(rr)` loops over the steps and threads each
upstream `RunRecord` into the next pipeline's `.run()`. Per-stage
caching plus upstream-aware run fingerprints make re-invocation
idempotent: shared upstream variants reuse the same run directory,
and downstream runs invalidate automatically when their upstream
changes.

```python
# chains/ptycho.py
import diffman as dm
import forward_sim, ptyd_convert, recon, analysis

CHAIN = dm.Chain('ptycho', steps=[
    dm.ChainStep('forward_sim',  forward_sim.PIPELINE),
    dm.ChainStep('ptyd_convert', ptyd_convert.PIPELINE,
                 consumes=('forward_sim',)),
    dm.ChainStep('recon',        recon.PIPELINE,
                 consumes=('ptyd_convert',)),
    dm.ChainStep('analysis',     analysis.PIPELINE,
                 consumes=('recon', 'forward_sim')),   # FRC needs both
])

CHAIN.variation('baseline',
    forward_sim='base', ptyd_convert='default',
    recon='ePIE', analysis='standard')
CHAIN.variation('jitter_low',  base='baseline', forward_sim='jitter_low')
CHAIN.variation('jitter_high', base='jitter_low', forward_sim='jitter_high')
CHAIN.variation('algo_dm',     base='baseline', recon='DM')
```

```python
# run_ablation.py
import diffman as dm
from chains.ptycho import CHAIN

rr = dm.RunRegistry('runs')
for name in ['baseline', 'jitter_low', 'jitter_high', 'algo_dm']:
    CHAIN.variations[name].run(rr)
```

Downstream stages read upstream artifacts via the run context:

```python
def _frc(ctx):
    recon_path = ctx.upstream_artifact('recon',
                                       'stages/recon/outputs/result.npy')
    truth_path = ctx.upstream_artifact('forward_sim',
                                       'stages/sim/outputs/field.npy')
    # ...
```

Each run's `run.json` records `chain`, `variation`, and
`upstream={step: fingerprint}` — chain progress is reconstructible
from those edges alone, no separate state file.

Stages opt into the scoreboard view by writing scalars during
execution:

```python
def _frc(ctx):
    score = compute_frc(...)
    ctx.metric('frc', 'score_at_half_period', score)
    ctx.metric('frc', 'half_period_nm', hp_nm)
```

These land in `stages/<stage>/metrics.json` and the scoreboard
aggregates them across all of a chain's variations into one table.

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
  or unified text diff). For runs that are part of a chain, the picker
  also offers a sibling-variation quick-pick.
- **Chains** — a separate sidebar section listing the chain fork
  forest. The chain page shows the variation tree, a variation selector,
  and a DAG of steps colored by status (`done` / `cached` / `failed` /
  `pending`); failed steps surface their traceback inline. Buttons:
  - **Scoreboard** — a variation × metric table aggregated from every
    stage's `metrics.json` (populated via `ctx.metric()`).
  - **Compare variations** — pick N variations and get a per-step
    parameter diff across their resolved configs.
  - **Source diff vs parent** — unified text diff of two chain `.py`
    files (chain forks declared via `Chain(..., parent='other')`).
- **Find** — a fingerprint-prefix search box (sidebar) that maps from
  a `short_fp` back to its variant and runs.

The UI auto-refreshes when you edit a pipeline `.py` (filesystem watcher
on `--scan-root`) or when new runs appear (watcher on `--root`). A small
dot next to the **diffman** title in the sidebar reflects the WebSocket
state — green for connected, red for transport errors, grey while
reconnecting.

Every drill-in page (variant, stage, source diff, run diff, compare,
why-re-run picker) carries a back link beneath its heading, since the
UI has no URL routing and the sidebar only navigates between top-level
pipelines, chains, and runs.

## CLI

```
diffman scan [root]                   # discover pipeline modules
diffman list <module>                 # variant names registered by that module
diffman describe <module> <variant>   # resolved config + fingerprint
diffman chains                        # list every discovered chain
diffman chain <name>                  # one chain's steps + variations (JSON)
diffman progress <chain> <variation>  # per-step status from disk
diffman scoreboard <chain> [-b VAR]   # variation × metric table; -b for deltas
diffman serve [--root DIR] [--port N] [--scan-root DIR] [--bind ADDR] [--no-scan]
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
GET  /api/chains                                → chain fork forest
GET  /api/chain/{name}                          → chain metadata + variations
GET  /api/chain_progress/{name}/{variation}     → per-step status
GET  /api/chain_source_diff?chain=<name>        → diff vs parent chain .py
GET  /api/chain_variation_diff?chain=&variations=a,b  → per-step config diff
GET  /api/chain_diff?chain=<name>               → variation diff vs parent chain
GET  /api/scoreboard/{name}[?baseline=<var>]    → variation × metric table
GET  /api/run_diff?pipeline=&variant=&a=&b=     → explain why two runs differ
GET  /api/disk_usage                            → bytes per pipeline/variant/run
GET  /api/runs                                  → all run records
GET  /api/run/{p}/{v}/{fp}                      → single run + stages
GET  /api/stage/{p}/{v}/{fp}/{stage}            → stage detail + artifacts
GET  /api/render?path=                          → renderer payload
GET  /api/render_dataset?path=&dataset=         → h5 dataset preview
GET  /api/srw_preview?path=                     → SRW heatmap + h/v cuts (requires srwpy)
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
            metrics.json    # scalars written via ctx.metric() (optional)
            _meta.json
            _key            # cache key string
    _scripts/.git/          # auto-snapshotted pipeline + chain .py per run
```

All app-managed state is JSON or git — text-friendly, mergeable, and
inspectable without a database.

## Tests

```bash
pixi run test               # pytest tests/
# or
pytest tests/
```

## Releasing

Tagged commits trigger the
[`publish.yml`](.github/workflows/publish.yml) workflow, which builds
sdist + wheel and publishes to PyPI via trusted publishing (OIDC).

```bash
# bump version in pyproject.toml, commit, then:
git tag v0.2.1
git push origin v0.2.1
```

## License

MIT — see [LICENSE](LICENSE) if present, or the `license` field in
[pyproject.toml](pyproject.toml).
