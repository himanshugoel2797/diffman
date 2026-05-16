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
| [src/diffman/core.py](src/diffman/core.py) | `Config`, `Variant`, `VariantRegistry`, `Stage`, `Pipeline`, `RunRegistry`, `RunContext`, `RunRecord`, `fingerprint()`. |
| [src/diffman/discovery.py](src/diffman/discovery.py) | Grep-based `discover()` of pipeline modules; `load_module()`. |
| [src/diffman/git_backup.py](src/diffman/git_backup.py) | Per-run snapshot of pipeline source into `runs/_scripts/.git/`. Best-effort. |
| [src/diffman/renderers.py](src/diffman/renderers.py) | Generic artifact renderers; SRW files route to `kind: 'srw'`. |
| [src/diffman/srw_loaders.py](src/diffman/srw_loaders.py) | SRW file detection + projection (intensity/amplitude/phase/...), downsampling, cuts. |
| [src/diffman/server.py](src/diffman/server.py) | FastAPI app factory, REST endpoints, `/ws` WebSocket, watchdog filesystem push. Read-only. |
| [src/diffman/cli.py](src/diffman/cli.py) | Subcommands: `scan`, `list`, `describe`, `serve`. |
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
    out = ctx.artifact('sim', 'out.npy', tmp)   # symlink or copy into the run
    return {'out': out}

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

### A new artifact renderer

Extend `renderers.render()` in
[src/diffman/renderers.py](src/diffman/renderers.py) with a new extension
branch returning `{'kind': '<your-kind>', 'data': …, 'meta': …}`. Add a
matching branch in `App.renderArtifact()` in
[src/diffman/ui/app.js](src/diffman/ui/app.js). Optional deps (h5py,
plotly) must be import-guarded.

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

- **`python -m diffman` double-import**: when invoked via `-m`, the module
  loads as `__main__`. A pipeline module then doing `import diffman`
  would load a second copy with an empty registry. Resolved by the alias
  in `__main__.py` — see the comment there. Don't remove it.
- **`load_module` reuse**: `discovery.load_module(name)` is idempotent
  (caches by name in `_module_variants`). If you add reload logic, also
  reset `_module_variants[name]` and `sys.modules[name]`.
- **WebSocket loop ownership**: `_Broadcaster.loop` is set in the FastAPI
  `startup` event. Watchdog runs in a separate thread and uses
  `asyncio.run_coroutine_threadsafe` to push events back.
- **`_safe_under()` path check**: every endpoint that takes a `path=`
  parameter must call `_safe_under(path, registry.root)` before reading
  the file, or you've opened a path-traversal hole.
- **Variant lookup is module-qualified**: use `registry.get(module, name)`
  internally; `registry.get_any(name)` works only when exactly one module
  has registered that name and raises otherwise.
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
