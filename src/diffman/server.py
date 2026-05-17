"""FastAPI server: REST API + WebSocket push + static UI.

Read-only. diffman tracks the *graph of pipeline forks* and the parameter
diffs at each fork; it does not launch runs. Runs are created by users
invoking their pipeline modules directly; diffman discovers the resulting
run directories and lets you browse them.

Routes:
  GET  /                                          → SPA shell
  GET  /static/...                                → app.js, style.css
  GET  /api/pipelines                             → discovered pipelines as a forest
  GET  /api/variants?module=<name>                → variant names for a pipeline module
  GET  /api/describe?module=&variant=             → resolved variant config + fingerprint
  GET  /api/variant_overrides?module=&variant=    → what this variant adds vs its base
  GET  /api/diff?module=<name>                    → variant diff vs parent pipeline
  GET  /api/source_diff?module=<name>             → unified text diff of .py vs parent
  GET  /api/compare?modules=a,b,c&variant=v       → N-way variant comparison
  GET  /api/find?q=<fp-prefix>                    → variants/runs by fingerprint prefix
  GET  /api/artifact_diff?path_a=&path_b=         → numerical/text diff of two artifacts
  GET  /api/runs[?pipeline=&variant=]             → existing run records
  GET  /api/run/{pipeline}/{variant}/{fp}         → single run detail + stages
  GET  /api/stage/{pipeline}/{variant}/{fp}/{st}  → stage detail + artifact list
  GET  /api/render?path=<abs>                     → renderer payload for a file
  GET  /api/render_dataset?path=&dataset=         → h5 dataset preview
  GET  /api/srw_preview?path=<abs>&...            → SRW-aware heatmap + cuts
  GET  /api/chains                                → chain fork forest
  GET  /api/chain/{name}                          → chain metadata + variations
  GET  /api/chain_progress/{name}/{variation}     → per-step status of a variation
  GET  /api/chain_source_diff?chain=<name>        → diff vs parent chain .py
  GET  /api/chain_variation_diff?chain=&variations=a,b  → per-step config diff
  GET  /api/chain_diff?chain=<name>               → variation diff vs parent chain
  GET  /api/scoreboard/{name}[?baseline=<var>]    → variation × metric table
  GET  /api/run_diff?pipeline=&variant=&a=&b=     → explain why two runs differ
  GET  /api/disk_usage                            → bytes per pipeline/variant/run
  GET  /artifact/{pipeline}/{variant}/{fp}/{rest} → raw file download
  WS   /ws                                        → push updates (run_changed, pipelines_changed)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

_log = logging.getLogger(__name__)

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from . import discovery, renderers
from .core import RunRegistry, registry as _global_registry

# Optional: watchdog for filesystem push.
try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _HAS_WATCHDOG = True
except ImportError:
    _HAS_WATCHDOG = False


# ---------------------------------------------------------------------------
# Config diff
# ---------------------------------------------------------------------------

def diff_configs(parent: dict, child: dict, path: str = '') -> list[dict]:
    """Deep diff two merged config dicts.

    Returns a flat list of `{path, kind, parent, child}` entries where
    `kind` is one of 'added' (only in child), 'removed' (only in parent),
    or 'changed' (different values). Nested dicts recurse; leaves compare
    via `!=`. `path` is dotted, like 'scan.width'.
    """
    out: list[dict] = []
    parent_keys = set(parent.keys()) if isinstance(parent, dict) else set()
    child_keys = set(child.keys()) if isinstance(child, dict) else set()
    for k in sorted(parent_keys | child_keys):
        p_here = path + ('.' if path else '') + k
        in_p = k in parent_keys
        in_c = k in child_keys
        if in_p and not in_c:
            out.append({'path': p_here, 'kind': 'removed',
                        'parent': parent[k], 'child': None})
        elif in_c and not in_p:
            out.append({'path': p_here, 'kind': 'added',
                        'parent': None, 'child': child[k]})
        else:
            pv, cv = parent[k], child[k]
            if isinstance(pv, dict) and isinstance(cv, dict):
                out.extend(diff_configs(pv, cv, p_here))
            elif pv != cv:
                out.append({'path': p_here, 'kind': 'changed',
                            'parent': pv, 'child': cv})
    return out


# ---------------------------------------------------------------------------
# Fork forest construction (shared by pipeline + chain endpoints)
# ---------------------------------------------------------------------------

def _build_forest(metas: list[dict], key: str) -> list[dict]:
    """Group entries into a parent→children forest.

    Entries without `key` (e.g. error metas in the pipeline forest) fall
    out as roots without children. An entry whose `parent` doesn't match
    any other entry's `key` is promoted to a root and tagged with
    `orphan_parent`.
    """
    known = {m[key] for m in metas if key in m}
    children: dict[Optional[str], list[dict]] = {}
    for m in metas:
        if key not in m:
            children.setdefault(None, []).append(m)
            continue
        parent = m.get('parent')
        if parent and parent not in known:
            m = {**m, 'orphan_parent': parent}
            parent = None
        children.setdefault(parent, []).append(m)

    sort_key = lambda x: x.get(key) or x.get('module') or ''
    def _node(m):
        kids = sorted(children.get(m[key], []), key=sort_key) if key in m else []
        return {**m, 'children': [_node(c) for c in kids]}
    return [_node(r) for r in sorted(children.get(None, []), key=sort_key)]


# ---------------------------------------------------------------------------
# Pipeline + chain metadata extractors
# ---------------------------------------------------------------------------

def _pipeline_meta(module: str) -> Optional[dict]:
    """Import a module and return its pipeline metadata, or None if it
    is chain-only (declares CHAIN/CHAINS but no PIPELINE) — chain-only
    modules belong in /api/chains, not the pipeline forest."""
    try:
        mod = discovery.load_module(module)
    except Exception as e:
        return {'module': module, 'error': f'import failed: {e}'}
    pipe = getattr(mod, 'PIPELINE', None)
    if pipe is None:
        if discovery.chains_in_module(mod):
            return None
        return {'module': module, 'error': 'no PIPELINE attribute'}
    return {
        'module': module,
        'pipeline': pipe.name,
        'parent': pipe.parent,
        'variant_count': len(_global_registry.for_module(module)),
    }


def _chain_meta(chain) -> dict:
    return {
        'name': chain.name,
        'module': discovery.CHAIN_TO_MODULE.get(chain.name),
        'parent': chain.parent,
        'step_count': len(chain.steps),
        'variation_count': len(chain.variations),
        'steps': [{'name': s.name, 'pipeline': s.pipeline.name,
                   'consumes': list(s.consumes)} for s in chain.steps],
    }


def _resolve_variation_runs(chain, variation, all_runs) -> dict:
    """For each step in `chain`, locate the run that belongs to
    `variation` by matching pipeline + variant + the upstream fingerprints
    threaded through this variation. Returns ``{step_name: RunRecord}``
    with ``None`` for steps with no matching run (pending, unresolved
    upstream, or variation didn't specify the step).

    This is the upstream-fp join shared by /api/chain_progress and
    /api/scoreboard — both endpoints need to walk a variation's
    expected chain and pin down the corresponding on-disk runs.
    """
    mapping = variation.resolve()
    runs: dict = {s.name: None for s in chain.steps}
    upstream_fps: dict = {}
    for step in chain.steps:
        spec = mapping.get(step.name)
        if spec is None:
            upstream_fps[step.name] = None
            continue
        #Fan-out variations bind a step to a list of variants. The
        #single-record-per-step view this helper exposes can't represent
        #all branches, so we pick the first variant — enough for the
        #non-fan-out case (identical behavior) and a graceful fallback
        #for the fan-out case until the UI grows per-branch support.
        variant_name = (spec[0] if isinstance(spec, (list, tuple))
                        else spec)
        required = {u: upstream_fps.get(u) for u in step.consumes}
        if any(v is None for v in required.values()):
            upstream_fps[step.name] = None
            continue
        for r in all_runs:
            if (r.pipeline == step.pipeline.name
                    and r.variant == variant_name
                    and r.upstream == required):
                runs[step.name] = r
                upstream_fps[step.name] = r.fingerprint
                break
        else:
            upstream_fps[step.name] = None
    return runs


def _resolve_variation_branches(chain, variation, all_runs) -> dict:
    """Per-branch counterpart to ``_resolve_variation_runs``.

    For every step in `chain`, returns a list of one entry per branch:
    ``[{'branch_key', 'variant', 'run'}, ...]``. Non-fan-out steps (and
    steps that don't inherit fan-out from upstream) get a single entry
    with ``branch_key=None`` — so callers can always iterate the list.

    Branch-key resolution mirrors ``Chain._run``: if the variation maps
    a step to ``list[str]``, the step fans out into those variants; a
    downstream step inherits keys from its consumed steps. The matched
    run for each branch joins on the upstream branch's run fingerprint,
    same upstream-fp join the single-record resolver does.
    """
    mapping = variation.resolve()
    branches: dict = {}    #step_name -> list[branch_dict]
    upstream_fps: dict = {}    #step_name -> dict[branch_key, fp | None]

    for step in chain.steps:
        spec = mapping.get(step.name)
        if spec is None:
            branches[step.name] = [
                {'branch_key': None, 'variant': None, 'run': None}]
            upstream_fps[step.name] = {None: None}
            continue

        if isinstance(spec, (list, tuple)):
            variants = list(spec)
            keys_to_variant = {v: v for v in variants}
        else:
            #Branch keys may still come from a consumed upstream.
            inherited: list = [None]
            for u in step.consumes:
                ks = list(upstream_fps.get(u, {None: None}).keys())
                if ks == [None]:
                    continue
                if inherited == [None]:
                    inherited = ks
                elif set(inherited) != set(ks):
                    #Mismatched inherited keys would be a hard error at
                    #execution; here we surface a single pending entry
                    #per declared variant so the UI can still render.
                    inherited = ks
            keys_to_variant = {k: spec for k in inherited}

        entries: list = []
        fps_for_step: dict = {}
        for bk, vname in keys_to_variant.items():
            required = {}
            ready = True
            for u in step.consumes:
                up_fps = upstream_fps.get(u, {None: None})
                if list(up_fps.keys()) == [None]:
                    required[u] = up_fps[None]
                else:
                    required[u] = up_fps.get(bk)
                if required[u] is None:
                    ready = False
            run = None
            if ready:
                for r in all_runs:
                    if (r.pipeline == step.pipeline.name
                            and r.variant == vname
                            and r.upstream == required):
                        run = r
                        break
            entries.append({'branch_key': bk, 'variant': vname, 'run': run})
            fps_for_step[bk] = run.fingerprint if run else None

        branches[step.name] = entries
        upstream_fps[step.name] = fps_for_step

    return branches


def _summarize_stage_status(stage_status: dict) -> str:
    """Roll a stage_status dict up into a single chain-step status."""
    statuses = set(stage_status.values())
    if 'failed' in statuses:
        return 'failed'
    if statuses == {'cached'}:
        return 'cached'
    if statuses and statuses <= {'done', 'cached'}:
        return 'done'
    if not statuses:
        return 'pending'
    return 'mixed'


def _unified_file_diff(parent_path: Optional[str],
                       child_path: Optional[str]) -> str:
    """Read two .py files and return a unified text diff.

    Shared by /api/source_diff (pipeline) and /api/chain_source_diff
    (chain) — same operation, different upstream resolution of which
    pair of paths to compare. Raises HTTPException on missing paths /
    read errors so the endpoints surface a consistent status code.
    """
    import difflib
    if not (parent_path and child_path):
        raise HTTPException(status_code=404,
                            detail='source file paths unavailable')
    try:
        child_src = Path(child_path).read_text().splitlines(keepends=True)
        parent_src = Path(parent_path).read_text().splitlines(keepends=True)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f'read failed: {e}')
    return ''.join(difflib.unified_diff(
        parent_src, child_src,
        fromfile=os.path.basename(parent_path),
        tofile=os.path.basename(child_path),
        n=3))


def _resolve_forks_of(child_to_forks_of: dict[str, Optional[str]],
                      child_names: set[str],
                      parent_names: set[str]
                      ) -> tuple[dict[str, Optional[str]], dict[str, str]]:
    """Build a child→parent map honoring explicit ``forks_of=`` first,
    falling back to name-match. A `forks_of` target that doesn't exist in
    the parent is surfaced via the second return value rather than
    silently swallowed (a typo would otherwise pretend the diff worked).

    Used by /api/diff (variant forks across modules) and /api/chain_diff
    (variation forks across chains) — same pattern, different domain.
    """
    child_to_parent: dict[str, Optional[str]] = {}
    unresolved: dict[str, str] = {}
    for cn in child_names:
        fo = child_to_forks_of.get(cn)
        if fo:
            if fo in parent_names:
                child_to_parent[cn] = fo
            else:
                child_to_parent[cn] = None
                unresolved[cn] = fo
        elif cn in parent_names:
            child_to_parent[cn] = cn
        else:
            child_to_parent[cn] = None
    return child_to_parent, unresolved


def _load_stage_metrics(fdir: str):
    """Yield ``(stage_name, metrics_dict)`` for each stage under `fdir`
    that has a parseable metrics.json. Skips silently on missing or
    malformed files — metrics are best-effort instrumentation."""
    stages_dir = os.path.join(fdir, 'stages')
    if not os.path.isdir(stages_dir):
        return
    for st_name in sorted(os.listdir(stages_dir)):
        mp = os.path.join(stages_dir, st_name, 'metrics.json')
        try:
            data = json.loads(Path(mp).read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            continue
        if isinstance(data, dict):
            yield st_name, data


def _scoreboard_rows(chain, all_runs) -> tuple[list[dict], set[str]]:
    """Aggregate ``<step>.<stage>.<metric>`` -> value across a chain's
    variations. Returns (rows, all_metric_keys) where each row is
    ``{'variation': name, 'metrics': flat_dict}``. Malformed variations
    (resolve raises) are skipped. Shared by /api/scoreboard and the
    `diffman scoreboard` CLI so the two stay in lock-step.
    """
    rows: list[dict] = []
    all_metric_keys: set[str] = set()
    for var_name, var in chain.variations.items():
        try:
            branches = _resolve_variation_branches(chain, var, all_runs)
        except KeyError:
            continue   #malformed variation (e.g. unresolved base=)

        #Discover the variation's branch-key set from any fan-out step
        #(all fan-out keys agree by Chain._run's inheritance rule). A
        #variation that doesn't fan out gets a single None-keyed row.
        branch_keys: list = [None]
        for entries in branches.values():
            keys = [e['branch_key'] for e in entries]
            if keys != [None]:
                branch_keys = keys
                break

        for bk in branch_keys:
            flat: dict = {}
            for step in chain.steps:
                step_entries = branches[step.name]
                #If this step itself doesn't fan out, it still has a
                #single None-keyed entry that's shared across all
                #branch rows; otherwise pick the entry matching bk.
                if len(step_entries) == 1 and step_entries[0]['branch_key'] is None:
                    entry = step_entries[0]
                else:
                    entry = next((e for e in step_entries
                                  if e['branch_key'] == bk), None)
                if entry is None or entry['run'] is None:
                    continue
                for st_name, st_metrics in _load_stage_metrics(entry['run'].fdir):
                    for k, v in st_metrics.items():
                        key = f'{step.name}.{st_name}.{k}'
                        flat[key] = v
                        all_metric_keys.add(key)
            label = var_name if bk is None else f'{var_name}[{bk}]'
            rows.append({'variation': label, 'metrics': flat})
    return rows, all_metric_keys


def _load_chain(name: str):
    """Load a chain by name (importing its declaring module if necessary)."""
    mod_name = discovery.CHAIN_TO_MODULE.get(name)
    if mod_name is None:
        #The name might not be indexed yet — sweep discovered modules.
        for entry in discovery.DISCOVERED_LIST:
            try:
                discovery.load_module(entry['module'])
            except Exception:
                continue
            if name in discovery.CHAIN_TO_MODULE:
                mod_name = discovery.CHAIN_TO_MODULE[name]
                break
        if mod_name is None:
            raise HTTPException(status_code=404,
                                detail=f'chain {name!r} not found')
    mod = discovery.load_module(mod_name)
    for ch in discovery.chains_in_module(mod):
        if ch.name == name:
            return ch, mod_name
    raise HTTPException(status_code=404,
                        detail=f'chain {name!r} not in module {mod_name!r}')


# ---------------------------------------------------------------------------
# WebSocket broadcaster
# ---------------------------------------------------------------------------

class _Broadcaster:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)

    def schedule(self, event: dict):
        """Thread-safe enqueue from non-async code (e.g. watchdog thread)."""
        if self.loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._send(event), self.loop)

    async def _send(self, event):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(event)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


# ---------------------------------------------------------------------------
# Watchdog -> broadcaster bridge
# ---------------------------------------------------------------------------

if _HAS_WATCHDOG:
    class _RunsWatcher(FileSystemEventHandler):
        def __init__(self, broadcaster, run_registry, runs_root: str):
            self.bcast = broadcaster
            self.run_registry = run_registry
            self.runs_root = os.path.abspath(runs_root)
            self._last = {}

        def _coalesce(self, key, payload, *, debounce=0.5):
            now = time.time()
            if key in self._last and now - self._last[key] < debounce:
                return
            self._last[key] = now
            self.bcast.schedule(payload)

        def _key(self, src_path):
            try:
                rel = os.path.relpath(src_path, self.runs_root)
            except ValueError:
                return None
            parts = rel.split(os.sep)
            if len(parts) < 3:
                return None
            return {'pipeline': parts[0], 'variant': parts[1], 'fp': parts[2]}

        def on_any_event(self, event):
            if event.is_directory:
                return
            #Read-only events (opened, accessed, closed_no_write) fire when
            #the server itself reads an artifact for /api/render; treating
            #those as run state changes makes the UI re-render and collapse
            #any expanded preview.
            if event.event_type not in ('created', 'modified',
                                         'deleted', 'moved'):
                return
            #Anything under the runs root invalidates the cached run list.
            self.run_registry.invalidate()
            k = self._key(event.src_path)
            if k is None:
                return
            ckey = f"{k['pipeline']}/{k['variant']}/{k['fp']}"
            self._coalesce(ckey, {'type': 'run_changed', **k,
                                  'path': event.src_path,
                                  'event': event.event_type})

    class _ScriptsWatcher(FileSystemEventHandler):
        """Watch `scan_root` for `.py` edits and refresh the pipeline graph.

        On a hit, evicts the affected module from sys.modules and from the
        variant registry, re-discovers (so newly-created files are picked
        up), and pushes a `pipelines_changed` WS event so the UI reloads.
        """

        def __init__(self, broadcaster, scan_root: str):
            self.bcast = broadcaster
            self.scan_root = os.path.abspath(scan_root)
            self._last = 0.0

        def on_any_event(self, event):
            if event.is_directory:
                return
            #Only react to real content changes. Importing a .py fires
            #`opened`/`closed_no_write` events too, which would cause us
            #to evict a module mid-import.
            if event.event_type not in ('created', 'modified',
                                         'deleted', 'moved'):
                return
            if not event.src_path.endswith('.py'):
                return
            #Ignore the byte-compile cache.
            if '__pycache__' in event.src_path.split(os.sep):
                return
            #Coalesce — editors fire many events per save.
            now = time.time()
            if now - self._last < 0.5:
                return
            self._last = now

            #Evict whichever module owns the changed file.
            full = os.path.abspath(event.src_path)
            mod_name = discovery.PATH_TO_MODULE.get(full)
            if mod_name:
                discovery.evict_module(mod_name)
            #Re-discover (a brand-new file would not be in PATH_TO_MODULE).
            try:
                discovery.discover(self.scan_root)
            except OSError as e:
                #Discovery is grep-based and never imports user code, so
                #the only realistic failure is filesystem-level (perm,
                #ENOENT) — surface it so the user sees their watcher is
                #broken rather than silently stuck on stale data.
                _log.warning('script watcher: discover(%s) failed: %s',
                             self.scan_root, e)
            self.bcast.schedule({'type': 'pipelines_changed',
                                 'path': event.src_path,
                                 'event': event.event_type})


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------

def create_app(*, root: str = 'runs', scan_root: str = '.',
               no_scan: bool = False) -> FastAPI:
    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        app.state.bcast.loop = asyncio.get_running_loop()
        if _HAS_WATCHDOG:
            runs_obs = Observer()
            runs_obs.schedule(
                _RunsWatcher(app.state.bcast, app.state.registry,
                             app.state.registry.root),
                app.state.registry.root, recursive=True)
            os.makedirs(app.state.registry.root, exist_ok=True)
            runs_obs.start()
            app.state.observer = runs_obs
            print('[diffman] watchdog: tailing runs at',
                  os.path.abspath(app.state.registry.root))

            scripts_obs = Observer()
            scripts_obs.schedule(
                _ScriptsWatcher(app.state.bcast, app.state.scan_root),
                app.state.scan_root, recursive=True)
            scripts_obs.start()
            app.state.scripts_observer = scripts_obs
            print('[diffman] watchdog: tailing pipeline sources at',
                  os.path.abspath(app.state.scan_root))
        try:
            yield
        finally:
            for obs_attr in ('observer', 'scripts_observer'):
                obs = getattr(app.state, obs_attr, None)
                if obs is not None:
                    obs.stop()
                    obs.join(timeout=2)

    app = FastAPI(title='diffman', lifespan=_lifespan)
    app.state.registry = RunRegistry(root=root)
    app.state.scan_root = scan_root
    app.state.bcast = _Broadcaster()
    app.state.observer = None
    app.state.scripts_observer = None

    if not no_scan:
        n = len(discovery.discover(scan_root))
        print(f'[diffman] discovered {n} pipeline module(s) under '
              f'{os.path.abspath(scan_root)}')

    ui_dir = Path(__file__).parent / 'ui'
    app.mount('/static', StaticFiles(directory=str(ui_dir)), name='static')

    # --- static SPA ------------------------------------------------------
    @app.get('/', response_class=HTMLResponse)
    def _index():
        #Rewrite static asset URLs to include a mtime cache-buster, so
        #edits to app.js / style.css land immediately on next page load
        #instead of waiting for the browser's heuristic revalidation.
        html = (ui_dir / 'index.html').read_text()
        for asset in ('app.js', 'style.css'):
            try:
                v = int((ui_dir / asset).stat().st_mtime)
            except OSError:
                continue
            html = html.replace(f'/static/{asset}', f'/static/{asset}?v={v}')
        return html

    # --- pipeline graph --------------------------------------------------
    @app.get('/api/pipelines')
    def _pipelines():
        """Return the fork forest: roots → children, plus any orphans."""
        metas = [m for m in (_pipeline_meta(d['module'])
                             for d in discovery.DISCOVERED_LIST)
                 if m is not None]
        return {
            'scan_root': os.path.abspath(app.state.scan_root),
            'forest': _build_forest(metas, key='pipeline'),
        }

    # --- variants / describe --------------------------------------------
    @app.get('/api/variants')
    def _variants(module: str):
        try:
            discovery.load_module(module)
        except Exception as e:
            raise HTTPException(status_code=400,
                                detail=f'import {module}: {e}')
        return {'module': module,
                'variants': _global_registry.for_module(module)}

    @app.get('/api/describe')
    def _describe(module: str, variant: str):
        try:
            discovery.load_module(module)
            v = _global_registry.get(module, variant)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {'module': module, 'variant': v.name,
                'fingerprint': v.fingerprint,
                'base': v.base.name if v.base else None,
                'forks_of': v.forks_of,
                'overrides': dict(v.overrides),
                'config': dict(v.config)}

    @app.get('/api/variant_overrides')
    def _variant_overrides(module: str, variant: str):
        """Return what THIS variant adds on top of its inheritance base.

        For `dm.register('jitter', base='base', probe=dict(jitter=True))`,
        this returns the `probe=...` layer plus the base's merged config
        for side-by-side comparison.
        """
        try:
            discovery.load_module(module)
            v = _global_registry.get(module, variant)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        base_cfg = dict(v.base.config) if v.base else {}
        merged = dict(v.config)
        return {
            'module': module, 'variant': v.name,
            'base': v.base.name if v.base else None,
            'overrides': dict(v.overrides),
            'base_config': base_cfg,
            'config': merged,
            'diff': diff_configs(base_cfg, merged),
        }

    # --- fork diff -------------------------------------------------------

    def _resolve_parent_module(module: str) -> Optional[str]:
        """Return the module that declares `Pipeline(<module>'s parent)`."""
        try:
            mod = discovery.load_module(module)
        except Exception as e:
            raise HTTPException(status_code=400,
                                detail=f'import {module}: {e}')
        pipe = getattr(mod, 'PIPELINE', None)
        if pipe is None:
            raise HTTPException(status_code=400,
                                detail=f'{module} has no PIPELINE')
        parent_name = pipe.parent
        if not parent_name:
            return None
        #Fast path: the pipeline-name index is populated as modules load.
        if parent_name in discovery.PIPELINE_TO_MODULE:
            return discovery.PIPELINE_TO_MODULE[parent_name]
        #Index miss — the parent may not have been imported yet. Walk
        #(once) to populate.
        for entry in discovery.DISCOVERED_LIST:
            try:
                discovery.load_module(entry['module'])
            except Exception:
                continue
            if parent_name in discovery.PIPELINE_TO_MODULE:
                return discovery.PIPELINE_TO_MODULE[parent_name]
        raise HTTPException(
            status_code=404,
            detail=f'parent pipeline {parent_name!r} not found in scan')

    @app.get('/api/diff')
    def _diff(module: str):
        """Diff every variant in `module` against its counterpart in the
        parent pipeline. Counterparts are matched by `forks_of='name'` on
        the child variant first, then by exact name.

        Per-variant entries: 'only_in_child' / 'only_in_parent' /
        'matches' / 'differs'.
        """
        try:
            mod = discovery.load_module(module)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        pipe = mod.PIPELINE
        parent_module = _resolve_parent_module(module)

        def _bare_entry(cn):
            cv = _global_registry.get(module, cn)
            return {'variant': cn,
                    'overrides': dict(cv.overrides),
                    'base': cv.base.name if cv.base else None,
                    'kind': 'no_parent'}

        if parent_module is None:
            #Root pipeline: still return its variants so the pipeline page
            #renders without a second round-trip.
            return {
                'module': module, 'pipeline': pipe.name,
                'parent_module': None, 'parent': None,
                'variants': [_bare_entry(cn) for cn in
                             sorted(_global_registry.for_module(module))],
            }
        discovery.load_module(parent_module)

        child_names = _global_registry.for_module(module)
        parent_names = set(_global_registry.for_module(parent_module))

        child_to_forks_of = {
            cn: _global_registry.get(module, cn).forks_of for cn in child_names}
        child_to_parent, unresolved = _resolve_forks_of(
            child_to_forks_of, set(child_names), parent_names)
        matched_parent = {p for p in child_to_parent.values() if p}

        per_variant = []
        for cn in sorted(child_names):
            pn = child_to_parent[cn]
            cv = _global_registry.get(module, cn)
            entry = {'variant': cn, 'overrides': dict(cv.overrides),
                     'base': cv.base.name if cv.base else None}
            if cn in unresolved:
                entry['forks_of_unresolved'] = unresolved[cn]
            if pn is None:
                entry['kind'] = 'only_in_child'
                per_variant.append(entry)
                continue
            child_cfg = dict(cv.config)
            parent_cfg = dict(_global_registry.get(parent_module, pn).config)
            d = diff_configs(parent_cfg, child_cfg)
            entry['parent_variant'] = pn if pn != cn else None
            entry['kind'] = 'matches' if not d else 'differs'
            entry['entries'] = d
            per_variant.append(entry)
        for pn in sorted(parent_names - matched_parent):
            per_variant.append({'variant': pn, 'kind': 'only_in_parent'})

        return {
            'module': module, 'pipeline': pipe.name,
            'parent_module': parent_module, 'parent': pipe.parent,
            'variants': per_variant,
        }

    @app.get('/api/source_diff')
    def _source_diff(module: str):
        """Unified text diff of this pipeline's .py against its parent's."""
        try:
            mod = discovery.load_module(module)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        parent_module = _resolve_parent_module(module)
        if parent_module is None:
            return {'module': module, 'parent_module': None, 'diff': ''}
        child_path = getattr(mod.PIPELINE, '_source_file', None)
        parent_mod = discovery.load_module(parent_module)
        parent_path = getattr(parent_mod.PIPELINE, '_source_file', None)
        return {
            'module': module, 'parent_module': parent_module,
            'parent_path': parent_path, 'child_path': child_path,
            'diff': _unified_file_diff(parent_path, child_path),
        }

    @app.get('/api/compare')
    def _compare(modules: str, variant: str):
        """N-way comparison of a same-named variant across modules.

        `modules` is a comma-separated list. Returns per-module configs
        plus a union-of-keys table showing the value each module has
        (or null if absent).
        """
        mod_list = [m.strip() for m in modules.split(',') if m.strip()]
        if len(mod_list) < 2:
            raise HTTPException(status_code=400,
                                detail='need at least 2 modules to compare')
        columns = []
        for m in mod_list:
            try:
                discovery.load_module(m)
                v = _global_registry.get(m, variant)
                columns.append({'module': m, 'present': True,
                                'fingerprint': v.fingerprint,
                                'config': dict(v.config)})
            except Exception as e:
                columns.append({'module': m, 'present': False,
                                'error': str(e), 'config': {}})

        #Pass None for failed modules so _flatten_union skips them
        #(otherwise every key from successful modules would render as
        #"missing" in the failed column instead of "errored").
        rows = _flatten_union([c['config'] if c['present'] else None
                               for c in columns])
        return {'variant': variant, 'columns': columns, 'rows': rows}

    @app.get('/api/find')
    def _find(q: str):
        """Search by fingerprint prefix (or full) against variants + runs."""
        q = q.strip().lower()
        if len(q) < 4:
            raise HTTPException(status_code=400,
                                detail='query must be at least 4 chars')
        #Make sure all discovered modules' variants are loaded.
        for entry in discovery.DISCOVERED_LIST:
            try:
                discovery.load_module(entry['module'])
            except Exception:
                pass
        variants = []
        for v in list(_global_registry._variants.values()):
            if v.fingerprint.startswith(q):
                variants.append({
                    'module': v.module, 'variant': v.name,
                    'fingerprint': v.fingerprint,
                })
        runs = []
        for r in app.state.registry.list_runs():
            if r.fingerprint.startswith(q):
                runs.append({
                    'pipeline': r.pipeline, 'variant': r.variant,
                    'short_fp': r.fingerprint[:12],
                    'fingerprint': r.fingerprint,
                    'started': r.started, 'ended': r.ended,
                })
        return {'query': q, 'variants': variants, 'runs': runs}

    @app.get('/api/artifact_diff')
    def _artifact_diff(path_a: str, path_b: str, target_max: int = 256,
                       dataset: Optional[str] = None):
        """Numerical diff of two artifacts (.npy / .h5 / .json / text).

        For arrays: shapes, element-wise stats of (b - a), and a
        downsampled delta heatmap if 2-D. Different shapes → stats only.
        For text/JSON: a unified text diff. For .h5/.hdf5, pass
        `dataset=<path>` to compare a specific dataset across both files
        — the loaded array routes through the same array-diff path as
        .npy. Whole-file h5 diffs aren't meaningful (heterogeneous
        datasets in one container), so `dataset=` is required.
        """
        if not (_safe_under(path_a, app.state.registry.root) and
                _safe_under(path_b, app.state.registry.root)):
            raise HTTPException(status_code=400, detail='path escape')
        if not (os.path.isfile(path_a) and os.path.isfile(path_b)):
            raise HTTPException(status_code=404, detail='one or both paths missing')
        return _compute_artifact_diff(path_a, path_b, target_max,
                                      dataset=dataset)

    # --- chains ----------------------------------------------------------
    @app.get('/api/chains')
    def _chains():
        """Return the chain fork forest, mirroring /api/pipelines.

        Imports every discovered module so any CHAIN/CHAINS attribute is
        indexed (chain discovery alone is grep-based and doesn't touch
        the chain objects themselves).
        """
        chains = []
        seen: set[str] = set()
        for entry in discovery.DISCOVERED_LIST:
            try:
                mod = discovery.load_module(entry['module'])
            except Exception:
                continue
            for ch in discovery.chains_in_module(mod):
                if ch.name in seen:
                    continue
                seen.add(ch.name)
                chains.append(_chain_meta(ch))
        return {'forest': _build_forest(chains, key='name')}

    @app.get('/api/chain/{name}')
    def _chain_detail(name: str):
        chain, _ = _load_chain(name)
        variations = []
        for v in chain.variations.values():
            try:
                mapping, err = v.resolve(), None
            except Exception as e:
                mapping, err = dict(v.overrides), str(e)
            variations.append({'name': v.name, 'base': v.base,
                               'forks_of': v.forks_of,
                               'overrides': dict(v.overrides),
                               'mapping': mapping, 'error': err})
        return {**_chain_meta(chain), 'variations': variations}

    @app.get('/api/chain_progress/{name}/{variation}')
    def _chain_progress(name: str, variation: str):
        """Per-step status of a chain variation, reconstructed from the
        upstream provenance recorded in each run.json.

        Each entry: `status` (done / cached / failed / pending / mixed /
        unspecified), the run's short_fp + fingerprint when matched, and
        per-stage statuses + errors for inline failed-step diagnostics.
        """
        chain, _ = _load_chain(name)
        if variation not in chain.variations:
            raise HTTPException(
                status_code=404,
                detail=f'variation {variation!r} not in chain {name!r}')
        try:
            mapping = chain.variations[variation].resolve()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        all_runs = app.state.registry.list_runs()
        matched = _resolve_variation_runs(
            chain, chain.variations[variation], all_runs)
        per_branch = _resolve_variation_branches(
            chain, chain.variations[variation], all_runs)
        steps = []
        for step in chain.steps:
            variant_name = mapping.get(step.name)
            r = matched[step.name]
            #Per-branch view for fan-out / inherited fan-out steps. For
            #plain single-branch steps this is still a one-element list
            #(branch_key=None), so the UI can render uniformly.
            branch_payload = []
            for entry in per_branch[step.name]:
                br = entry['run']
                br_status = ('unspecified' if entry['variant'] is None
                             else _summarize_stage_status(br.stage_status)
                             if br is not None else 'pending')
                branch_payload.append({
                    'branch_key': entry['branch_key'],
                    'variant': entry['variant'],
                    'status': br_status,
                    'short_fp': br.fingerprint[:12] if br else None,
                    'fingerprint': br.fingerprint if br else None,
                    'stage_status': dict(br.stage_status) if br else {},
                    'errors': dict(br.errors) if br else {},
                })
            steps.append({
                'name': step.name, 'pipeline': step.pipeline.name,
                'variant': variant_name, 'consumes': list(step.consumes),
                'status': ('unspecified' if variant_name is None
                           else _summarize_stage_status(r.stage_status)
                           if r is not None else 'pending'),
                'short_fp': r.fingerprint[:12] if r else None,
                'fingerprint': r.fingerprint if r else None,
                'stage_status': dict(r.stage_status) if r else {},
                'errors': dict(r.errors) if r else {},
                'branches': branch_payload,
            })
        return {'chain': name, 'variation': variation, 'steps': steps}

    @app.get('/api/chain_source_diff')
    def _chain_source_diff(chain: str):
        """Unified text diff of this chain's .py against its parent chain's."""
        ch, _ = _load_chain(chain)
        if not ch.parent:
            return {'chain': chain, 'parent': None, 'diff': ''}
        parent_ch, _ = _load_chain(ch.parent)
        child_path = getattr(ch, '_source_file', None)
        parent_path = getattr(parent_ch, '_source_file', None)
        return {'chain': chain, 'parent': ch.parent,
                'parent_path': parent_path, 'child_path': child_path,
                'diff': _unified_file_diff(parent_path, child_path)}

    @app.get('/api/chain_diff')
    def _chain_diff(chain: str):
        """Match each variation in `chain` to its counterpart in the
        parent chain (by `forks_of=` first, then by exact name) and
        report which step→variant mappings differ.

        Entries: 'matches' / 'differs' / 'only_in_child' / 'only_in_parent'.
        Mirrors /api/diff's shape so the UI can render both with one
        component.
        """
        ch, _ = _load_chain(chain)
        if not ch.parent:
            #Mirror the non-root branch: tolerate variations that fail to
            #resolve (e.g. base= points at something missing) by surfacing
            #the error rather than 500'ing the whole endpoint.
            out_root = []
            for v in ch.variations.values():
                try:
                    out_root.append({'variation': v.name,
                                     'mapping': v.resolve(),
                                     'kind': 'no_parent'})
                except Exception as e:
                    out_root.append({'variation': v.name,
                                     'kind': 'unresolved',
                                     'error': str(e)})
            return {'chain': chain, 'parent': None, 'variations': out_root}
        parent_ch, _ = _load_chain(ch.parent)
        child_names = set(ch.variations)
        parent_names = set(parent_ch.variations)
        c_to_p, unresolved = _resolve_forks_of(
            {cn: cv.forks_of for cn, cv in ch.variations.items()},
            child_names, parent_names)
        matched_parent = {p for p in c_to_p.values() if p}

        out = []
        for cn in sorted(child_names):
            cv = ch.variations[cn]
            try:
                c_map = cv.resolve()
            except Exception as e:
                out.append({'variation': cn, 'kind': 'unresolved',
                            'error': str(e)})
                continue
            entry: dict = {'variation': cn, 'mapping': c_map,
                           'base': cv.base}
            if cn in unresolved:
                entry['forks_of_unresolved'] = unresolved[cn]
            pn = c_to_p[cn]
            if pn is None:
                entry['kind'] = 'only_in_child'
                out.append(entry); continue
            try:
                p_map = parent_ch.variations[pn].resolve()
            except Exception:
                p_map = dict(parent_ch.variations[pn].overrides)
            entry['parent_variation'] = pn if pn != cn else None
            step_diffs = [
                {'step': step, 'parent': p_map.get(step),
                 'child': c_map.get(step)}
                for step in sorted(set(c_map) | set(p_map))
                if c_map.get(step) != p_map.get(step)]
            entry['kind'] = 'matches' if not step_diffs else 'differs'
            entry['steps'] = step_diffs
            out.append(entry)
        for pn in sorted(parent_names - matched_parent):
            out.append({'variation': pn, 'kind': 'only_in_parent'})
        return {'chain': chain, 'parent': ch.parent, 'variations': out}

    @app.get('/api/chain_variation_diff')
    def _chain_variation_diff(chain: str, variations: str):
        """N-way per-step parameter diff across variations of a chain.

        Returns one section per step with the resolved variant name in
        each variation and a flattened union-of-keys table over the
        merged variant configs — same shape as /api/compare but rolled
        up across the steps of the chain.
        """
        ch, _ = _load_chain(chain)
        var_names = [v.strip() for v in variations.split(',') if v.strip()]
        if len(var_names) < 2:
            raise HTTPException(status_code=400,
                                detail='need at least 2 variations to compare')
        missing = [v for v in var_names if v not in ch.variations]
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f'variations not in chain: {missing}')
        mappings = {v: ch.variations[v].resolve() for v in var_names}
        sections = []
        for step in ch.steps:
            mod = step.pipeline._module
            try:
                discovery.load_module(mod) if mod else None
            except Exception:
                pass
            columns = []
            for vn in var_names:
                variant_name = mappings[vn].get(step.name)
                try:
                    v = _global_registry.get(mod, variant_name)
                    columns.append({'variation': vn,
                                    'variant': variant_name,
                                    'present': True,
                                    'fingerprint': v.fingerprint,
                                    'config': dict(v.config)})
                except Exception as e:
                    columns.append({'variation': vn,
                                    'variant': variant_name,
                                    'present': False,
                                    'error': str(e),
                                    'config': {}})
            rows = _flatten_union([c['config'] if c['present'] else None
                                   for c in columns])
            sections.append({'step': step.name,
                             'pipeline': step.pipeline.name,
                             'columns': columns,
                             'rows': rows})
        return {'chain': chain, 'variations': var_names, 'steps': sections}

    @app.get('/api/scoreboard/{name}')
    def _scoreboard(name: str, baseline: Optional[str] = None):
        """Cross-variation scoreboard. One row per chain variation, one
        column per metric key (``<step>.<stage>.<name>``) written via
        ``ctx.metric()``.

        When `baseline=<variation>` is set, each row also carries a
        `deltas` dict with ``value - baseline_value`` for numeric metrics
        and ``None`` for non-numeric / missing-in-baseline keys.
        """
        chain, _ = _load_chain(name)
        if baseline is not None and baseline not in chain.variations:
            raise HTTPException(
                status_code=404,
                detail=f'baseline variation {baseline!r} not in chain {name!r}')
        rows, all_metric_keys = _scoreboard_rows(
            chain, app.state.registry.list_runs())

        if baseline is not None:
            base_row = next((r for r in rows if r['variation'] == baseline), {})
            base_metrics = base_row.get('metrics', {})
            for row in rows:
                row['deltas'] = {
                    k: (row['metrics'][k] - base_metrics[k]
                        if isinstance(row['metrics'].get(k), (int, float))
                        and isinstance(base_metrics.get(k), (int, float))
                        else None)
                    for k in row['metrics']}
        return {'chain': name, 'baseline': baseline,
                'metric_keys': sorted(all_metric_keys),
                'rows': rows}

    # --- runs ------------------------------------------------------------
    def _summary(r):
        return {
            'pipeline': r.pipeline, 'variant': r.variant,
            'short_fp': r.fingerprint[:12],
            'fdir': r.fdir,
            'started': r.started, 'ended': r.ended,
            'stage_status': r.stage_status,
        }

    @app.get('/api/runs')
    def _runs(pipeline: Optional[str] = None, variant: Optional[str] = None):
        runs = app.state.registry.list_runs(pipeline=pipeline, variant=variant)
        return {'runs': [_summary(r) for r in runs]}

    @app.get('/api/run/{pipeline}/{variant}/{short_fp}')
    def _run_detail(pipeline: str, variant: str, short_fp: str):
        runs = app.state.registry.list_runs(pipeline=pipeline, variant=variant)
        match = next((r for r in runs
                      if r.fingerprint.startswith(short_fp)), None)
        if match is None:
            raise HTTPException(status_code=404, detail='run not found')
        cfg = {}
        cfg_path = os.path.join(match.fdir, 'config.json')
        if os.path.exists(cfg_path):
            cfg = json.loads(Path(cfg_path).read_text())
        return {'run': match.__dict__, 'config': cfg,
                'stages': _stage_summaries(match)}

    @app.get('/api/disk_usage')
    def _disk_usage():
        """Bytes-on-disk rollup of the runs root, grouped by
        (pipeline, variant, run). The UI uses this to decide what's safe
        to clean. Pure observation — no deletion happens here.

        Symlinks are followed for size attribution unless they loop or
        break, in which case the broken/loop entry contributes 0 bytes.
        """
        root = app.state.registry.root
        if not os.path.isdir(root):
            return {'root': root, 'total': 0, 'pipelines': []}
        pipelines = []
        grand_total = 0
        for pipeline in sorted(os.listdir(root)):
            ppath = os.path.join(root, pipeline)
            #_scripts/ holds the per-run git snapshot repo and isn't a
            #pipeline directory; skip it. Anything else, even if its
            #name starts with `_`, is a real pipeline.
            if not os.path.isdir(ppath) or pipeline == '_scripts':
                continue
            variants = []
            ptotal = 0
            for variant in sorted(os.listdir(ppath)):
                vpath = os.path.join(ppath, variant)
                if not os.path.isdir(vpath):
                    continue
                runs = []
                vtotal = 0
                for short_fp in sorted(os.listdir(vpath)):
                    rpath = os.path.join(vpath, short_fp)
                    if not os.path.isdir(rpath):
                        continue
                    size = _du(rpath)
                    runs.append({'short_fp': short_fp, 'size': size})
                    vtotal += size
                variants.append({'variant': variant, 'size': vtotal, 'runs': runs})
                ptotal += vtotal
            pipelines.append({'pipeline': pipeline, 'size': ptotal,
                              'variants': variants})
            grand_total += ptotal
        return {'root': root, 'total': grand_total, 'pipelines': pipelines}

    @app.get('/api/run_diff')
    def _run_diff(pipeline: str, variant: str, a: str, b: str):
        """Explain why two runs of the same (pipeline, variant) differ.

        For each stage, report whether its cache key matches across runs.
        For stages that differ, attribute the change to one or more of:
        the function source, the config-keys slice, or any upstream
        stage's key. Per-key config diffs are surfaced so the reader
        can see *what* changed without re-running anything.
        """
        runs = app.state.registry.list_runs(pipeline=pipeline, variant=variant)
        ra = next((r for r in runs if r.fingerprint.startswith(a)), None)
        rb = next((r for r in runs if r.fingerprint.startswith(b)), None)
        if ra is None or rb is None:
            raise HTTPException(status_code=404,
                                detail='one or both runs not found')
        if ra.fingerprint == rb.fingerprint:
            raise HTTPException(status_code=400,
                                detail='cannot diff a run against itself')
        stages = []
        for st_name in sorted(set(ra.stage_keys) | set(rb.stage_keys)):
            ka = ra.stage_keys.get(st_name)
            kb = rb.stage_keys.get(st_name)
            entry: dict = {'name': st_name, 'key_a': ka, 'key_b': kb,
                           'identical': ka == kb}
            if ka == kb:
                stages.append(entry); continue
            ma = _read_stage_meta(ra.fdir, st_name).get('components', {})
            mb = _read_stage_meta(rb.fdir, st_name).get('components', {})
            entry['fn_changed'] = ma.get('fn') != mb.get('fn')
            entry['config_changed'] = ma.get('config') != mb.get('config')
            entry['upstream_changed'] = ma.get('upstream') != mb.get('upstream')
            entry['config_diff'] = diff_configs(
                ma.get('config') or {}, mb.get('config') or {})
            entry['upstream_diff'] = [
                {'name': name, 'a': ma.get('upstream', {}).get(name),
                 'b': mb.get('upstream', {}).get(name)}
                for name in sorted(set((ma.get('upstream') or {}))
                                   | set((mb.get('upstream') or {})))
                if (ma.get('upstream') or {}).get(name)
                   != (mb.get('upstream') or {}).get(name)
            ]
            stages.append(entry)
        return {'pipeline': pipeline, 'variant': variant,
                'run_a': {'fingerprint': ra.fingerprint,
                          'short_fp': ra.fingerprint[:12]},
                'run_b': {'fingerprint': rb.fingerprint,
                          'short_fp': rb.fingerprint[:12]},
                'stages': stages}

    @app.get('/api/stage/{pipeline}/{variant}/{short_fp}/{stage}')
    def _stage_detail(pipeline: str, variant: str, short_fp: str, stage: str):
        runs = app.state.registry.list_runs(pipeline=pipeline, variant=variant)
        match = next((r for r in runs
                      if r.fingerprint.startswith(short_fp)), None)
        if match is None:
            raise HTTPException(status_code=404, detail='run not found')
        stage_dir = os.path.join(match.fdir, 'stages', stage)
        outs = os.path.join(stage_dir, 'outputs')
        artifacts = []
        if os.path.isdir(outs):
            for root, _, files in os.walk(outs):
                for fn in files:
                    full = os.path.join(root, fn)
                    rel = os.path.relpath(full, match.fdir)
                    artifacts.append({
                        'path': rel,
                        'size': os.path.getsize(full),
                        'absolute': full,
                    })
        return {
            'stage': stage,
            'status': match.stage_status.get(stage),
            'key': match.stage_keys.get(stage),
            'error': match.errors.get(stage),
            'artifacts': artifacts,
        }

    # --- renderers -------------------------------------------------------
    @app.get('/api/render')
    def _render(path: str):
        if not _safe_under(path, app.state.registry.root):
            raise HTTPException(status_code=400, detail='path escape')
        return renderers.render(path)

    @app.get('/api/render_dataset')
    def _render_dataset(path: str, dataset: str):
        if not _safe_under(path, app.state.registry.root):
            raise HTTPException(status_code=400, detail='path escape')
        return renderers.render_h5_dataset(path, dataset)

    @app.get('/api/srw_preview')
    def _srw_preview(path: str,
                     repr: str = 'intensity',
                     polarization: str = 'both',
                     energy_slice: int = -1,
                     row: int = -1,
                     col: int = -1,
                     target_max: int = 512):
        if not _safe_under(path, app.state.registry.root):
            raise HTTPException(status_code=400, detail='path escape')
        from . import srw_loaders
        loaded = srw_loaders.load(path)
        if 'error' in loaded:
            return {'kind': 'error', 'data': loaded['error'],
                    'meta': {'path': path}}
        proj = srw_loaders.project(loaded, repr,
                                   polarization=polarization,
                                   energy_slice=energy_slice)
        if 'error' in proj:
            return {'kind': 'error', 'data': proj['error'],
                    'meta': {'path': path}}
        data2d, (sy, sx) = srw_loaders.downsample(proj['data'], target_max)
        cut = srw_loaders.cuts(data2d, row=row, col=col)
        mesh = loaded['mesh']
        return {
            'kind': 'srw_preview',
            'meta': {
                'path': path,
                'srw_kind': loaded['kind'],
                'available': list(loaded['available']),
                'note': loaded.get('note'),
                'mesh': mesh,
                'downsampled': [sy, sx],
                'repr': proj['repr'],
                'polarization': proj.get('polarization', polarization),
                'energy_slice': proj.get('energy_slice', energy_slice),
            },
            'data': {
                'z': data2d.tolist(),
                'cut': cut,
            },
        }

    @app.get('/artifact/{pipeline}/{variant}/{short_fp}/{rest:path}')
    def _artifact(pipeline: str, variant: str, short_fp: str, rest: str):
        #Normalize textually (which collapses `..`) so a maliciously
        #crafted `rest` segment can't escape, but don't realpath the
        #candidate — artifacts registered via ctx.artifact() are
        #symlinks pointing outside the runs tree and must remain
        #serveable. `os.path.isfile` follows the symlink for the
        #existence check.
        base = os.path.realpath(app.state.registry.root)
        candidate = os.path.normpath(
            os.path.join(base, pipeline, variant, short_fp, rest))
        if not candidate.startswith(base + os.sep):
            raise HTTPException(status_code=400, detail='path escape')
        if not os.path.isfile(candidate):
            raise HTTPException(status_code=404)
        return FileResponse(candidate)

    # --- websocket -------------------------------------------------------
    @app.websocket('/ws')
    async def _ws(ws: WebSocket):
        await app.state.bcast.connect(ws)
        try:
            while True:
                #We don't expect client messages, but keep the loop alive.
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            app.state.bcast.disconnect(ws)

    return app


def _du(path: str) -> int:
    """Bytes on disk under `path`, following symlinks to real files.

    Loop or broken symlinks contribute 0 — they're surfaced rather than
    crashing the whole walk. Used by /api/disk_usage; not a stable API.
    """
    total = 0
    for root, _, files in os.walk(path, followlinks=False):
        for f in files:
            full = os.path.join(root, f)
            try:
                total += os.stat(full).st_size
            except OSError:
                continue
    return total


def _read_stage_meta(fdir: str, st_name: str) -> dict:
    """Return _meta.json for a stage, or an empty dict if missing/bad."""
    try:
        return json.loads(Path(fdir, 'stages', st_name, '_meta.json').read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _stage_summaries(record) -> list[dict]:
    out = []
    stages_dir = os.path.join(record.fdir, 'stages')
    if not os.path.isdir(stages_dir):
        return out
    for st_name in sorted(os.listdir(stages_dir)):
        outs = os.path.join(stages_dir, st_name, 'outputs')
        artifacts = sum(len(fs) for _, _, fs in os.walk(outs)) \
            if os.path.isdir(outs) else 0
        meta = _read_stage_meta(record.fdir, st_name)
        started, ended = meta.get('started'), meta.get('ended')
        out.append({
            'name': st_name,
            'status': record.stage_status.get(st_name),
            'key': record.stage_keys.get(st_name),
            'artifact_count': artifacts,
            'started': started,
            'ended': ended,
            'duration_s': (ended - started) if (started and ended) else None,
        })
    return out


def _flatten_union(dicts: list[Optional[dict]]) -> list[dict]:
    """Flatten N nested dicts to dotted-path rows for an N-way compare.

    Returns one row per leaf path, with a `values` list (one entry per
    input dict, `None` if absent) and `equal: bool`. Entries that are
    `None` (rather than a dict) are treated as "column not available" —
    they are excluded from key collection and equality comparison, but
    still occupy a slot in `values` so the column index aligns with the
    caller's column list.
    """
    def _walk(d, prefix=''):
        out = {}
        if not isinstance(d, dict):
            out[prefix] = d
            return out
        for k, v in d.items():
            p = f'{prefix}.{k}' if prefix else k
            if isinstance(v, dict):
                out.update(_walk(v, p))
            else:
                out[p] = v
        return out

    flat = [_walk(d) if d is not None else None for d in dicts]
    keys = sorted({k for f in flat if f is not None for k in f.keys()})
    missing = object()
    unavailable = object()
    rows = []
    for k in keys:
        vals = [unavailable if f is None else f.get(k, missing) for f in flat]
        display = [None if (v is missing or v is unavailable) else v
                   for v in vals]
        comparable = [v for v in vals if v is not unavailable]
        non_missing = [v for v in comparable if v is not missing]
        equal = (len(non_missing) == len(comparable) and len(non_missing) > 0
                 and all(v == non_missing[0] for v in non_missing))
        rows.append({'path': k, 'values': display, 'equal': equal})
    return rows


def _compute_artifact_diff(path_a: str, path_b: str, target_max: int,
                           dataset: Optional[str] = None) -> dict:
    """Numerical or textual diff of two artifacts."""
    import difflib
    ext = os.path.splitext(path_a)[1].lower()
    if os.path.splitext(path_b)[1].lower() != ext:
        return {'kind': 'error',
                'note': 'extensions differ; refusing to compare'}

    if ext == '.npy':
        try:
            import numpy as np
        except ImportError:
            return {'kind': 'error', 'note': 'numpy not available'}
        a = np.load(path_a, allow_pickle=False)
        b = np.load(path_b, allow_pickle=False)
        return _array_diff(a, b, target_max, paths=(path_a, path_b))

    if ext in ('.h5', '.hdf5'):
        if not dataset:
            return {'kind': 'error',
                    'note': 'h5 diff requires dataset=<path>; the file '
                            'is a container of heterogeneous arrays.'}
        try:
            import h5py
            import numpy as np
        except ImportError as e:
            return {'kind': 'error', 'note': f'{e.name} not available'}
        try:
            with h5py.File(path_a, 'r') as fa, h5py.File(path_b, 'r') as fb:
                if dataset not in fa or dataset not in fb:
                    where = ('both files' if dataset not in fa
                             and dataset not in fb
                             else 'path_a' if dataset not in fa
                             else 'path_b')
                    return {'kind': 'error',
                            'note': f'dataset {dataset!r} missing in {where}'}
                a = np.asarray(fa[dataset][...])
                b = np.asarray(fb[dataset][...])
        except OSError as e:
            return {'kind': 'error', 'note': f'h5 read failed: {e}'}
        out = _array_diff(a, b, target_max, paths=(path_a, path_b))
        out['dataset'] = dataset
        return out

    if ext in ('.json',):
        try:
            ja = json.loads(Path(path_a).read_text())
            jb = json.loads(Path(path_b).read_text())
        except Exception as e:
            return {'kind': 'error', 'note': f'json load: {e}'}
        if isinstance(ja, dict) and isinstance(jb, dict):
            return {'kind': 'json_diff', 'entries': diff_configs(ja, jb)}
        return {'kind': 'json_diff', 'a': ja, 'b': jb,
                'equal': ja == jb}

    #Text fallback for everything else.
    try:
        ta = Path(path_a).read_text().splitlines(keepends=True)
        tb = Path(path_b).read_text().splitlines(keepends=True)
    except UnicodeDecodeError:
        return {'kind': 'error',
                'note': f'binary file with no diff handler for {ext!r}'}
    unified = ''.join(difflib.unified_diff(
        ta, tb,
        fromfile=os.path.basename(path_a),
        tofile=os.path.basename(path_b),
        n=3))
    return {'kind': 'text_diff', 'diff': unified}


def _array_diff(a, b, target_max: int, paths) -> dict:
    """Numpy array diff: shapes + element-wise stats of (b-a)."""
    import numpy as np
    meta = {
        'kind': 'array_diff',
        'shape_a': list(a.shape), 'shape_b': list(b.shape),
        'dtype_a': str(a.dtype),  'dtype_b': str(b.dtype),
    }
    if a.shape != b.shape:
        meta['note'] = 'shapes differ; element-wise diff not computed'
        return meta
    d = b.astype(float) - a.astype(float)
    abs_d = np.abs(d)
    finite = np.isfinite(d)
    if finite.any():
        denom = np.maximum(np.abs(a.astype(float)),
                           np.abs(b.astype(float)))
        #Mask the divide to avoid a RuntimeWarning when denom has zeros —
        #the np.where chooses the 0.0 branch for those slots regardless,
        #but the unmasked division still trips numpy's divide warning.
        rel = np.zeros_like(d, dtype=float)
        mask = denom > 0
        rel[mask] = abs_d[mask] / denom[mask]
        meta['stats'] = {
            'min':      float(d[finite].min()),
            'max':      float(d[finite].max()),
            'mean':     float(d[finite].mean()),
            'abs_max':  float(abs_d[finite].max()),
            'abs_mean': float(abs_d[finite].mean()),
            'rms':      float(np.sqrt((d[finite] ** 2).mean())),
            'rel_max':  float(rel[finite].max()),
            'n_diff':   int((abs_d > 0).sum()),
            'n_total':  int(d.size),
        }
    else:
        meta['stats'] = None
        meta['note'] = 'all-non-finite delta'
    if d.ndim == 2:
        from . import srw_loaders
        delta_ds, _ = srw_loaders.downsample(d, target_max)
        meta['delta_heatmap'] = delta_ds.tolist()
    elif d.ndim == 1:
        #1-D arrays overlay much better than they summarize. Downsample
        #to `target_max` so a 5e6-point trace doesn't ship megabytes of
        #JSON; the x axis is the original index so the reader can spot
        #where a divergence happens, not the decimated index.
        n = a.size
        stride = max(1, n // target_max)
        idx = list(range(0, n, stride))
        meta['overlay'] = {
            'x':   idx,
            'y_a': a[::stride].astype(float).tolist(),
            'y_b': b[::stride].astype(float).tolist(),
            'stride': stride,
        }
    return meta


def _safe_under(path: str, root: str) -> bool:
    """True iff `path` (after textual normalization) lies inside `root`.

    Only the textual path is normalized — we don't `realpath` it so that
    artifacts registered via ``ctx.artifact`` (which symlinks from the
    user's working area into runs/) remain reachable. The runs root
    itself is resolved so symlinked runs roots (CI tmpdirs on macOS,
    for instance) are matched correctly.

    Path-traversal attacks via ``..`` in the URL still fail: normpath
    collapses those before the prefix test.
    """
    base = os.path.realpath(root)
    p = os.path.normpath(os.path.abspath(path))
    return p == base or p.startswith(base + os.sep)


def run(*, root='runs', port=8765, bind='127.0.0.1',
        scan_root='.', no_scan=False):
    """Start the server (blocking). Convenience wrapper around uvicorn.run()."""
    import uvicorn
    app = create_app(root=root, scan_root=scan_root, no_scan=no_scan)
    if bind != '127.0.0.1':
        print(f'[diffman] warning: binding to {bind}; the UI is unauthenticated.',
              file=sys.stderr)
    print(f'diffman UI: http://{bind}:{port}  (runs: {os.path.abspath(root)})')
    uvicorn.run(app, host=bind, port=port, log_level='warning')
