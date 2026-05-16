# Agent notes for `diffman`

Orientation doc for coding agents (Claude Code, etc.). Read this **before**
making changes. Pair with [README.md](README.md) for user-facing usage.

## What this is

A *difference manager* for simulation pipelines. It tracks the graph of
how pipelines are related (forks), shows what parameters changed at each
fork, and serves a read-only web UI for browsing both the fork graph and
the existing run directories those pipelines produced.

**diffman does not launch runs.** Users invoke their own pipeline
modules directly; diffman discovers the resulting run directories under
`<runs-root>/` and renders them. Forks are created and edited manually
by the user — a fork is just a pipeline module that declares a
`parent=` in its `Pipeline(...)` constructor.

Spun out from `srwl_uti_diffman.py` in
[/global/cfs/cdirs/m2173/hgoel/xpp_nnl_dataset_gpu/smp_to_det/SRW](../xpp_nnl_dataset_gpu/smp_to_det/SRW).
That single-file stdlib-only version still ships with SRW; this package
is the richer evolution with heavier deps. Keep both functional —
porting a change usually means touching both.

## Code map

| File | What lives here |
|------|-----------------|
| [src/diffman/core.py](src/diffman/core.py) | `Config`, `Variant`, `VariantRegistry`, `Stage`, `Pipeline`, `Chain`, `ChainStep`, `Variation`, `RunRegistry`, `RunContext`, `RunRecord`, `fingerprint()`, the module-level `register()` helper, and the singleton `registry`. |
| [src/diffman/discovery.py](src/diffman/discovery.py) | Grep-based `discover()` of pipeline modules; `load_module()`. |
| [src/diffman/git_backup.py](src/diffman/git_backup.py) | Per-run snapshot of pipeline source into `runs/_scripts/.git/`. Best-effort. |
| [src/diffman/renderers.py](src/diffman/renderers.py) | Generic artifact renderers; SRW files route to `kind: 'srw'`. |
| [src/diffman/srw_loaders.py](src/diffman/srw_loaders.py) | SRW file detection + projection (intensity/amplitude/phase/...), downsampling, cuts. |
| [src/diffman/server.py](src/diffman/server.py) | FastAPI app factory, REST endpoints, `/ws` WebSocket, watchdog filesystem push. Read-only. |
| [src/diffman/cli.py](src/diffman/cli.py) | Subcommands: `scan`, `list`, `describe`, `serve`, `chains`, `chain`, `progress`, `scoreboard`. |
| [src/diffman/ui/](src/diffman/ui/) | `index.html`, `style.css`, `app.js` — vanilla JS + Plotly via CDN. |
| [pyproject.toml](pyproject.toml) | pip extras (`[all]` = h5py + plotly) and `[tool.pixi.*]` config (incl. `srwpy` from conda-forge). |

## Core concepts

- **Variant** = a named `Config` (nested dict, deep-merge) with optional
  inheritance. Pipeline modules call `dm.register('name', base='parent',
  **overrides)` to build a tree. Each Variant records the `__name__` of
  the module that registered it; the registry is keyed on `(module,
  name)`, so two pipelines can each have a `base` variant without
  colliding.
- **Pipeline** = `dm.Pipeline(name, [stages...], parent='other_pipeline')`.
  The `name` becomes the top-level run-directory name; `parent` (optional)
  is the name of the pipeline this one was forked from. `parent` is what
  builds the fork graph in the UI.
- **Fork** = a pipeline whose `parent=...` points at another pipeline.
  Created by hand: copy a `.py`, change the `Pipeline` name, set
  `parent=`, edit the variants. The UI shows the forest of pipelines
  rooted at parent-less pipelines and computes a per-variant diff vs the
  parent (`/api/diff?module=...`).
- **Stage** = `(name, fn, inputs, config_keys)`. Cache key is sha256 of
  `(name, source-of-fn, restricted-config, upstream-keys)`. `config_keys`
  restricts which top-level subtrees feed the key.
- **Run directory** (created by user code via `Pipeline.run`):
  ```
  <runs-root>/
      <pipeline>/<variant>/<short-fp>/
          run.json           # RunRecord — ALL state is JSON for git
          config.json
          run.log
          stages/<stage>/
              outputs/...    # artifacts (any format)
              _meta.json
              _key           # cache key string
      _scripts/.git/         # auto-snapshotted pipeline source files
  ```
  Many endpoints parse this layout — grep for `parts[-4]`, `parts[-3]`,
  `'stages'`, `'outputs'`, `'_scripts'` before changing it.

## Adding things

### A new pipeline module (user-facing)

```python
import diffman as dm

dm.register('base',   scan=dict(width=5e-6, step=1e-7))
dm.register('jitter', base='base', probe=dict(jitter=True))

def _stage_sim(ctx):
    cfg = ctx.variant.config
    tmp = '/tmp/out.npy'              # or wherever the tool wrote it
    np.save(tmp, …)
    ctx.artifact('sim', 'out.npy', tmp)   # symlink or copy into the run

PIPELINE = dm.Pipeline('myname', [
    dm.Stage('sim', _stage_sim, config_keys=('scan', 'probe')),
])
```

A fork of the above:

```python
import diffman as dm

dm.register('base',   scan=dict(width=8e-6, step=1e-7))  # note: width tweaked
dm.register('jitter', base='base', probe=dict(jitter=True))

# ... stages ...

PIPELINE = dm.Pipeline('myname_v2', [...], parent='myname')
```

The UI will show `myname_v2` as a child of `myname` and report
`scan.width` as the only difference on the `base` variant.

### A chain of pipelines

A `Chain` declares a DAG of pipelines plus `Variation`s — coherent
tuples of (chain step -> variant name). Diffman doesn't execute; the
chain just iterates its steps, threads upstream `RunRecord`s through,
and lets the per-stage cache short-circuit unchanged work.

```python
import diffman as dm
import forward_sim, ptyd_convert, recon

CHAIN = dm.Chain('ptycho', steps=[
    dm.ChainStep('forward_sim', forward_sim.PIPELINE),
    dm.ChainStep('recon', recon.PIPELINE, consumes=('forward_sim',)),
])
CHAIN.variation('baseline',  forward_sim='base',   recon='ePIE')
CHAIN.variation('jitter',    base='baseline', forward_sim='jitter')
```

Downstream stages access upstream output via
`ctx.upstream_artifact('forward_sim', 'stages/sim/outputs/data.npy')`.
Each run records `chain`/`variation`/`upstream` in its `run.json` so
chain progress is reconstructible from disk.

A chain step can be targeted individually via
`CHAIN.variations[v].run(rr, step='recon')` (or `step=2` /
`step='2'`). The targeted step runs normally; upstream steps are
re-entered with `assume_cached=True`, which raises if any of their
stages aren't already cached. Downstream steps are skipped. This is
how a single entry-point script can be invoked once per `srun`
geometry — e.g., the forward sim under `-n 16 -N 4`, the reconstruction
under `-n 4 -N 1` — without merging both into one launch. The
canonical wiring reads `STEP` from the environment alongside
`VARIATION`.

Chains may declare `parent='other_chain'` to fork; the UI groups child
chains under their parent and offers a source diff just like pipeline
forks.

Stages can write scoreboard metrics via
`ctx.metric('<stage>', '<name>', value)`. These persist to
`stages/<stage>/metrics.json` and are aggregated across a chain's
variations by the `/api/scoreboard/{chain}` endpoint.

### A new artifact renderer

Extend `renderers.render()` in
[src/diffman/renderers.py](src/diffman/renderers.py) with a new extension
branch returning `{'kind': '<your-kind>', 'data': …, 'meta': …}`. Add a
matching branch in `App.renderArtifact()` in
[src/diffman/ui/app.js](src/diffman/ui/app.js). Optional deps (h5py,
plotly) must be import-guarded.

### A new UI page / view

The UI is a single-page app rooted at `App` in
[src/diffman/ui/app.js](src/diffman/ui/app.js). New views are methods on
`App` that clear `#main` and append the content. A few conventions hold
across them:

- Set `this.current = {kind: '<page>', ...identifiers}` at the top of
  every entry-point method. The sidebar's auto-refresh consults
  `this.current` to keep the active highlight in sync, and
  `handleRunChanged` uses it to decide whether a websocket event should
  re-render the current page.
- Include a back link as the first element after the `<h2>` heading
  when the view was reached from another page (variant → pipeline,
  stage → run, source-diff → pipeline, etc.). The UI has no URL
  routing, so without an explicit back link the user is stranded.
- Build DOM with the `el(tag, props, kids)` helper. It treats `class`,
  `onclick`, `html`, `text` specially; everything else is a flat
  attribute. **Null / undefined values in `props` are skipped** — so
  `title: maybeNull` is safe — and boolean values are set as HTML
  attribute presence (so `disabled: true` works, `disabled: false`
  omits the attribute).
- Numeric formatting goes through `fmt()` (smart sci/precision with
  0 / NaN / Infinity special-cased) and `fmtVal()` (everything else,
  falling back to `JSON.stringify`).

## Conventions

- **Type hints on the public surface.** Internal helpers can skip.
- **All persistent state is JSON or git.** No SQLite, pickles, or binary
  databases in the run tree.
- **Heavy deps are optional.** `h5py`, `plotly`, `srwpy` — import-guarded;
  fall back to `kind: 'binary'` / `kind: 'error'` with a clear note.
- **No emoji in source.** Plain ASCII.
- **Read-only server.** Don't add endpoints that write run state, launch
  processes, or edit pipeline source. Those concerns live outside
  diffman.

## Gotchas

- **`python -m diffman` double-import**: when invoked via `-m`, Python
  loads `__main__.py` as the script `__main__`. We deliberately import
  the CLI via a *relative* import (`from .cli import main`) so the
  package side-effect — `diffman/__init__.py` — runs once and lands in
  `sys.modules['diffman']`. Subsequent `import diffman` inside a user
  pipeline reuses that module and the shared `registry`. Don't rewrite
  `__main__.py` to use an absolute `import diffman` or invoke the file
  directly with `python src/diffman/__main__.py` — both load a second
  copy of the package with an empty registry.
- **`load_module` reuse**: `discovery.load_module(name)` is idempotent —
  it short-circuits on `name in sys.modules`. The only caches it
  touches are `sys.modules`, `discovery.PIPELINE_TO_MODULE`,
  `discovery.CHAIN_TO_MODULE`, and the variants attributed to the
  module inside the global registry. The file watcher (and any reload
  logic you add) should call `discovery.evict_module(name)` to drop all
  four atomically instead of poking `sys.modules` directly.
- **WebSocket loop ownership**: `_Broadcaster.loop` is set inside the
  FastAPI `lifespan` context manager (via `asyncio.get_running_loop()`).
  Watchdog runs in a separate thread and `_Broadcaster.schedule()` uses
  `asyncio.run_coroutine_threadsafe` to hand events back to the event
  loop. If you start broadcasting before the lifespan runs, the loop
  attribute is still `None` and the schedule call will crash.
- **`_safe_under()` path check**: every endpoint that takes a `path=`
  parameter must call `_safe_under(path, registry.root)` before reading
  the file, or you've opened a path-traversal hole.
- **Variant lookup is always module-qualified**: there's no name-only
  lookup on the registry — you must call `registry.get(module, name)`.
  Use `registry.for_module(module)` to list a module's variant names.
  Two pipelines can legitimately each register a `base` variant; the
  `(module, name)` key keeps them distinct.
- **`/api/artifact_diff` JSON has two response shapes**: when both
  files parse to dicts, the server returns `{kind: 'json_diff',
  entries: [...]}` (a list of `diff_configs` entries). For arrays /
  primitives / `null`, it returns `{kind: 'json_diff', a, b, equal}`
  instead. `renderArtifactDiff` in the UI handles both — don't drop
  the `else` branch if you refactor.
- **Two diffman codebases**: changes to core semantics should usually
  land in BOTH this package and
  [srwl_uti_diffman.py](../xpp_nnl_dataset_gpu/smp_to_det/SRW/env/python/srwpy/srwl_uti_diffman.py).
  The on-disk run layout is shared.

## Don't

- Don't reintroduce launch/submitter/script-editing endpoints. diffman
  is a viewer.
- Don't introduce binary state (SQLite, pickled caches).
- Don't make heavy deps mandatory. Core (`diffman scan/list/describe`)
  must work on python+numpy minimal env.
- Don't auto-create or auto-edit pipeline modules. Forks are
  hand-written.
- Don't add a frontend framework. Vanilla JS + Plotly via CDN.
- Don't break `python -m diffman`.
