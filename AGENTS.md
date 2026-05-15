# Agent notes for `diffman`

Orientation doc for coding agents (Claude Code, etc.). Read this **before**
making changes. Pair with [README.md](README.md) for user-facing usage.

## What this is

A variant/cache/run manager for simulation pipelines, plus a FastAPI +
WebSocket browser UI that previews stage inputs/outputs live (with
SRW-aware rendering for `.h5` and `.dat` files).

Spun out from `srwl_uti_diffman.py` in
[/global/cfs/cdirs/m2173/hgoel/xpp_nnl_dataset_gpu/smp_to_det/SRW](../xpp_nnl_dataset_gpu/smp_to_det/SRW).
That single-file stdlib-only version still ships with SRW; this package is
the richer evolution with heavier deps. Keep both functional — porting a
fix usually means touching both.

## Code map

| File | What lives here |
|------|-----------------|
| [src/diffman/core.py](src/diffman/core.py) | `Config`, `Variant`, `VariantRegistry`, `Stage`, `Pipeline`, `RunRegistry`, `RunContext`, `RunRecord`, `fingerprint()`. The bedrock. |
| [src/diffman/discovery.py](src/diffman/discovery.py) | Grep-based `discover()` of pipeline modules; `load_module()` that adds discovered dirs to `sys.path` and tags `PIPELINE._source_file`. |
| [src/diffman/submitters.py](src/diffman/submitters.py) | `LocalSubmitter` (subprocess), `SlurmSubmitter` (sbatch wrapper), `default_submitter('auto')`. |
| [src/diffman/git_backup.py](src/diffman/git_backup.py) | Per-run snapshot of pipeline source into `runs/_scripts/.git/`. Best-effort; failures are silent. |
| [src/diffman/renderers.py](src/diffman/renderers.py) | Generic artifact renderers (image / npy / h5 / json / text). SRW files are sniffed first and routed to the SRW panel via `kind: 'srw'`. |
| [src/diffman/srw_loaders.py](src/diffman/srw_loaders.py) | SRW file detection, load via `srwl_uti_read_*`, repr conversion (intensity/amplitude/phase/real/imag), polarization toggle, downsampling, cuts. |
| [src/diffman/server.py](src/diffman/server.py) | FastAPI app factory, REST endpoints, `/ws` WebSocket, watchdog-based filesystem push. |
| [src/diffman/cli.py](src/diffman/cli.py) | Subcommands: `scan`, `list`, `describe`, `run`, `serve`. |
| [src/diffman/ui/](src/diffman/ui/) | `index.html`, `style.css`, `app.js` — vanilla JS + Plotly via CDN. |
| [pyproject.toml](pyproject.toml) | pip extras (`[all]` = h5py + plotly) and `[tool.pixi.*]` config (incl. `srwpy` from conda-forge). |

## Core concepts

- **Variant** = named `Config` (nested dict, deep-merge) with optional
  inheritance. Pipeline modules call `dm.register('name', base='parent',
  **overrides)` to build trees. The registry is global, but each Variant
  records the `__name__` of the calling module (via stack inspection in
  `register()`); `registry.for_module(mod)` filters by that, and the UI
  uses it so one pipeline's variants don't bleed into another's listing.
- **Override variant** = inline `--var k=v` overrides synthesized at
  submit time into a Variant named `<base>+<short-fp>`. Inherits the
  base's `module` attribution. Each unique override set lives in its
  own run directory. Not a script fork.
- **Script fork** = an on-disk copy of a pipeline `.py`. Created via
  `POST /api/fork_script {parent_module, new_name}`; the server writes
  `<new_name>.py` next to the parent, rewrites the `Pipeline('<parent>',
  ...)` literal to use the new name, prefixes every variant name in the
  file with `<new_name>__` so the parent and fork can coexist in one
  process, and drops a `<new_name>.fork.json` sidecar recording the
  parent. Both files get committed into `_scripts/.git/`. The fork
  module is then editable via `GET/PUT /api/script`.
- **Stage** = `(name, fn, inputs, config_keys)`. The stage's cache key is
  a sha256 of `(name, source-of-fn, restricted-config, upstream-keys)`.
  `config_keys=('a','b')` means only those top-level subtrees feed the
  key. Anything outside leaves the cache valid.
- **Pipeline** = ordered list of stages; `pipeline.run(variant, registry,
  force=…, only=…)` dispatches with `cached / running / done / skipped /
  failed` per stage.
- **Run directory**:
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
      _jobs/<fork>_<fp>/     # per-launch job.log or slurm-*.out
  ```
  This layout is load-bearing — many endpoints and helpers parse it. If
  you change it, grep for `parts[-4]`, `parts[-3]`, `'stages'`, `'outputs'`,
  `'_jobs'`, `'_scripts'`.

## Adding things

### A new pipeline module (user-facing)

```python
import diffman as dm

dm.register('myname_base', scan=dict(width=5e-6, step=1e-7))
dm.register('myname_jitter', base='myname_base', probe=dict(jitter=True))

def _stage_sim(ctx):
    cfg = ctx.variant.config
    np.save(ctx.artifact('sim', 'out.npy'), …)
    return {'out': ctx.artifact('sim', 'out.npy')}

PIPELINE = dm.Pipeline('myname', [
    dm.Stage('sim', _stage_sim, config_keys=('scan', 'probe')),
])
```

Anything matching `*diffman*` + `*PIPELINE*` in a `.py` file under the
scan root is auto-discovered. Don't rely on import-time side effects
outside `dm.register()` calls and the `PIPELINE = …` line — the loader
imports the module just to populate the registry.

### A new artifact renderer

Extend `renderers.render()` in
[src/diffman/renderers.py](src/diffman/renderers.py) with a new extension
branch returning `{'kind': '<your-kind>', 'data': …, 'meta': …}`. Add a
matching branch in `App.renderArtifact()` in
[src/diffman/ui/app.js](src/diffman/ui/app.js). Optional deps (h5py,
plotly) must be import-guarded.

For SRW-specific previews, add a load path in
[src/diffman/srw_loaders.py](src/diffman/srw_loaders.py) and extend
`project()` — the server endpoint and UI panel will pick it up
automatically via `available`.

### A new submitter

Subclass `Submitter` in
[src/diffman/submitters.py](src/diffman/submitters.py); implement
`submit(cmd, *, cwd, env, log_dir) -> dict` returning at minimum
`{'id': str, 'kind': str}`. Wire it into `default_submitter()` and add
a `--submitter <name>` choice in the CLI.

## Conventions

- **No `SRWL`/`_uti_dfm_` prefixes**. That's the SRW-resident version's
  style. Here classes are plain (`Config`, `Pipeline`), functions are
  snake_case, params don't take leading underscores.
- **Type hints on public surface**. Internal helpers can skip.
- **All persistent state is JSON or git**. No SQLite, pickles, or binary
  databases in the run tree. The git repo at `_scripts/` is the only
  exception (and that's git's own format, mergeable by git).
- **Heavy deps are optional**. `h5py`, `plotly`, `srwpy` — import-guarded.
  Code paths that need them fall back to `kind: 'binary'` / `kind:
  'error'` with a clear `meta.note`.
- **No emoji in source**. Plain ASCII.

## Testing

```
pixi run test                 # pytest tests/
```

Smoke-test recipe (full e2e):

```bash
pixi shell
mkdir /tmp/dmtest && cd /tmp/dmtest
cp /global/cfs/cdirs/m2173/hgoel/diffman/tests/demo_pipe.py .  # or write your own
diffman run demo_pipe demo_small
diffman serve --root runs --scan-root .
# in another shell:
curl -s http://127.0.0.1:8765/api/runs
```

The SRW-aware path needs `srwpy` importable; `pixi install` brings it
from conda-forge. Without it, `/api/render` on a `.dat` file still
returns `kind: 'srw'` but `/api/srw_preview` returns
`{'kind': 'error', 'data': 'srwlib not importable; install SRW …'}` —
which is intentional and tested.

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
  `asyncio.run_coroutine_threadsafe` to push events back — don't try to
  call `bcast._send(...)` directly from the watchdog thread.
- **`_safe_under()` path check**: every endpoint that takes a `path=`
  parameter must call `_safe_under(path, registry.root)` before reading
  the file, or you've opened a path-traversal hole. The realpath check
  also blocks symlink escapes.
- **Heatmap downsampling**: `srw_loaders.downsample(arr, target_max=512)`
  is applied *before* JSON serialization. If you bump `target_max`, also
  raise the FastAPI request size limit if needed.
- **Two diffman codebases**: changes to core semantics (variant fingerprint,
  cache key, run dir layout) should usually land in BOTH this package and
  [srwl_uti_diffman.py](../xpp_nnl_dataset_gpu/smp_to_det/SRW/env/python/srwpy/srwl_uti_diffman.py).
  The on-disk layout is shared — runs created by one are readable by
  the other.

## Don't

- Don't introduce binary state (SQLite, pickled caches). Keep everything
  git-mergeable text.
- Don't make heavy deps mandatory. Core (`diffman list/run/scan`) must
  work on a python+numpy minimal env; UI and SRW previews degrade.
- Don't add a frontend framework (React/Vue). The UI is intentionally
  vanilla JS + Plotly from CDN — keeps deploy a `pip install`.
- Don't auto-create variants. Forks are only synthesized when overrides
  are supplied; never write to the global `registry` from server code.
- Don't break the `python -m diffman` invocation in pursuit of cleanliness.
  It's the documented entry point.

## When in doubt

Grep for the thing you're touching across both this repo and
`srwl_uti_diffman.py`. Symmetric changes there usually mean you're on the
right track.
