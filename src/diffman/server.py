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
  GET  /api/scoreboard/{name}                     → variation × metric table
  GET  /artifact/{pipeline}/{variant}/{fp}/{rest} → raw file download
  WS   /ws                                        → push updates (run_changed)
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

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
# Pipeline-forest construction
# ---------------------------------------------------------------------------

def _pipeline_meta(module: str) -> dict:
    """Import a discovered module and report its pipeline metadata."""
    try:
        mod = discovery.load_module(module)
    except Exception as e:
        return {'module': module, 'error': f'import failed: {e}'}
    pipe = getattr(mod, 'PIPELINE', None)
    if pipe is None:
        return {'module': module, 'error': 'no PIPELINE attribute'}
    return {
        'module': module,
        'pipeline': pipe.name,
        'parent': pipe.parent,
        'variant_count': len(_global_registry.for_module(module)),
    }


def _build_forest(metas: list[dict]) -> list[dict]:
    """Group pipelines into roots → children by their `parent` declaration.

    Matching is by pipeline name (the string in `Pipeline('name', ...)`).
    Pipelines whose parent doesn't match anything in the scan are treated
    as roots and flagged via `orphan_parent`.
    """
    known = {m['pipeline'] for m in metas if 'pipeline' in m}
    children: dict[Optional[str], list[dict]] = {}
    for m in metas:
        if 'pipeline' not in m:
            children.setdefault(None, []).append(m)
            continue
        parent = m.get('parent')
        if parent and parent not in known:
            m = {**m, 'orphan_parent': parent}
            parent = None
        children.setdefault(parent, []).append(m)

    def _node(m):
        key = lambda x: x.get('pipeline') or x['module']
        #Only nodes with a pipeline name can have children. Error metas
        #live under `children[None]` as roots, so without this guard
        #_node(error_meta) would recurse into its sibling roots forever.
        kids = (sorted(children.get(m['pipeline'], []), key=key)
                if 'pipeline' in m else [])
        return {**m, 'children': [_node(c) for c in kids]}

    return [_node(r) for r in sorted(children.get(None, []),
                                     key=lambda x: x.get('pipeline') or x['module'])]


# ---------------------------------------------------------------------------
# Chain metadata + forest
# ---------------------------------------------------------------------------

def _list_chains_in_module(mod) -> list:
    """Return every Chain object exposed by a module (CHAIN + CHAINS)."""
    out = []
    single = getattr(mod, 'CHAIN', None)
    if single is not None:
        out.append(single)
    multi = getattr(mod, 'CHAINS', None)
    if multi:
        out.extend(multi)
    return out


def _chain_meta(chain) -> dict:
    return {
        'name': chain.name,
        'module': discovery.CHAIN_TO_MODULE.get(chain.name),
        'parent': chain.parent,
        'step_count': len(chain.steps),
        'variation_count': len(chain.variations),
        'steps': [{'name': s.name,
                   'pipeline': s.pipeline.name,
                   'consumes': list(s.consumes)}
                  for s in chain.steps],
    }


def _build_chain_forest(metas: list[dict]) -> list[dict]:
    """Group chains into a fork forest by chain `parent` declaration.

    Same shape and orphan-flagging semantics as the pipeline forest, so
    the UI can render them identically.
    """
    known = {m['name'] for m in metas if 'name' in m}
    children: dict[Optional[str], list[dict]] = {}
    for m in metas:
        if 'name' not in m:
            children.setdefault(None, []).append(m)
            continue
        parent = m.get('parent')
        if parent and parent not in known:
            m = {**m, 'orphan_parent': parent}
            parent = None
        children.setdefault(parent, []).append(m)

    def _node(m):
        key = lambda x: x.get('name') or x.get('module') or ''
        kids = (sorted(children.get(m['name'], []), key=key)
                if 'name' in m else [])
        return {**m, 'children': [_node(c) for c in kids]}

    return [_node(r) for r in sorted(children.get(None, []),
                                     key=lambda x: x.get('name') or x.get('module') or '')]


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
    for ch in _list_chains_in_module(mod):
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
            except Exception:
                pass
            self.bcast.schedule({'type': 'pipelines_changed',
                                 'path': event.src_path,
                                 'event': event.event_type})


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------

def create_app(*, root: str = 'runs', scan_root: str = '.',
               no_scan: bool = False) -> FastAPI:
    app = FastAPI(title='diffman')
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

    @app.on_event('startup')
    async def _startup():
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

    @app.on_event('shutdown')
    async def _shutdown():
        for obs_attr in ('observer', 'scripts_observer'):
            obs = getattr(app.state, obs_attr, None)
            if obs is not None:
                obs.stop()
                obs.join(timeout=2)

    # --- static SPA ------------------------------------------------------
    @app.get('/', response_class=HTMLResponse)
    def _index():
        return (ui_dir / 'index.html').read_text()

    # --- pipeline graph --------------------------------------------------
    @app.get('/api/pipelines')
    def _pipelines():
        """Return the fork forest: roots → children, plus any orphans."""
        metas = [_pipeline_meta(m['module'])
                 for m in discovery.DISCOVERED_LIST]
        return {
            'scan_root': os.path.abspath(app.state.scan_root),
            'forest': _build_forest(metas),
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

        #Build child→parent mapping. Explicit forks_of is authoritative —
        #if the named target doesn't exist in the parent, surface it as
        #'unresolved' rather than silently falling back to name match (a
        #typo would otherwise pretend the diff worked).
        child_to_parent: dict[str, Optional[str]] = {}
        unresolved: dict[str, str] = {}
        for cn in child_names:
            cv = _global_registry.get(module, cn)
            if cv.forks_of:
                if cv.forks_of in parent_names:
                    child_to_parent[cn] = cv.forks_of
                else:
                    child_to_parent[cn] = None
                    unresolved[cn] = cv.forks_of
            elif cn in parent_names:
                child_to_parent[cn] = cn
            else:
                child_to_parent[cn] = None
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
        import difflib
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
        if not (child_path and parent_path):
            raise HTTPException(status_code=404,
                                detail='source file paths unavailable')
        try:
            child_src = Path(child_path).read_text().splitlines(keepends=True)
            parent_src = Path(parent_path).read_text().splitlines(keepends=True)
        except OSError as e:
            raise HTTPException(status_code=500, detail=f'read failed: {e}')
        unified = ''.join(difflib.unified_diff(
            parent_src, child_src,
            fromfile=os.path.basename(parent_path),
            tofile=os.path.basename(child_path),
            n=3))
        return {
            'module': module, 'parent_module': parent_module,
            'parent_path': parent_path, 'child_path': child_path,
            'diff': unified,
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
    def _artifact_diff(path_a: str, path_b: str, target_max: int = 256):
        """Numerical diff of two artifacts (.npy / .h5 / .json / text).

        For arrays: shapes, element-wise stats of (b - a), and a
        downsampled delta heatmap if 2-D. Different shapes → stats only.
        For text/JSON: a unified text diff.
        """
        if not (_safe_under(path_a, app.state.registry.root) and
                _safe_under(path_b, app.state.registry.root)):
            raise HTTPException(status_code=400, detail='path escape')
        if not (os.path.isfile(path_a) and os.path.isfile(path_b)):
            raise HTTPException(status_code=404, detail='one or both paths missing')
        return _compute_artifact_diff(path_a, path_b, target_max)

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
            for ch in _list_chains_in_module(mod):
                if ch.name in seen:
                    continue
                seen.add(ch.name)
                chains.append(_chain_meta(ch))
        return {'forest': _build_chain_forest(chains)}

    @app.get('/api/chain/{name}')
    def _chain_detail(name: str):
        chain, mod_name = _load_chain(name)
        variations = []
        for v in chain.variations.values():
            try:
                mapping = v.resolve()
                err = None
            except Exception as e:
                mapping = dict(v.overrides)
                err = str(e)
            variations.append({'name': v.name, 'base': v.base,
                               'overrides': dict(v.overrides),
                               'mapping': mapping, 'error': err})
        return {
            **_chain_meta(chain),
            'module': mod_name,
            'variations': variations,
        }

    @app.get('/api/chain_progress/{name}/{variation}')
    def _chain_progress(name: str, variation: str):
        """Per-step status of a chain variation, reconstructed from the
        upstream provenance recorded in each run.json.

        Returns one entry per chain step with: `status` (done / cached /
        failed / pending / mixed), the run's short_fp (if any), and
        per-stage statuses & errors for failed-step diagnostics.
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
        steps = []
        upstream_fps: dict[str, Optional[str]] = {}
        for step in chain.steps:
            variant_name = mapping.get(step.name)
            entry = {'name': step.name,
                     'pipeline': step.pipeline.name,
                     'variant': variant_name,
                     'consumes': list(step.consumes),
                     'status': 'pending',
                     'short_fp': None,
                     'fingerprint': None,
                     'stage_status': {},
                     'errors': {}}
            if variant_name is None:
                entry['status'] = 'unspecified'
                upstream_fps[step.name] = None
                steps.append(entry)
                continue
            #Find candidate runs whose upstream provenance matches what
            #earlier steps in this variation produced. None upstream
            #means we couldn't resolve an earlier step → treat as
            #pending without trying to match.
            required = {u: upstream_fps.get(u) for u in step.consumes}
            if any(v is None for v in required.values()):
                upstream_fps[step.name] = None
                steps.append(entry)
                continue
            match = None
            for r in all_runs:
                if r.pipeline != step.pipeline.name:
                    continue
                if r.variant != variant_name:
                    continue
                if dict(r.upstream or {}) != required:
                    continue
                match = r
                break
            if match is None:
                upstream_fps[step.name] = None
                steps.append(entry)
                continue
            statuses = set(match.stage_status.values())
            if 'failed' in statuses:
                entry['status'] = 'failed'
            elif statuses == {'cached'}:
                entry['status'] = 'cached'
            elif statuses and statuses <= {'done', 'cached'}:
                entry['status'] = 'done'
            elif not statuses:
                entry['status'] = 'pending'
            else:
                entry['status'] = 'mixed'
            entry['short_fp'] = match.fingerprint[:12]
            entry['fingerprint'] = match.fingerprint
            entry['stage_status'] = dict(match.stage_status)
            entry['errors'] = dict(match.errors)
            upstream_fps[step.name] = match.fingerprint
            steps.append(entry)
        return {'chain': name, 'variation': variation, 'steps': steps}

    @app.get('/api/chain_source_diff')
    def _chain_source_diff(chain: str):
        """Unified text diff of this chain's .py against its parent chain's."""
        import difflib
        ch, _ = _load_chain(chain)
        if not ch.parent:
            return {'chain': chain, 'parent': None, 'diff': ''}
        parent_ch, _ = _load_chain(ch.parent)
        child_path = getattr(ch, '_source_file', None)
        parent_path = getattr(parent_ch, '_source_file', None)
        if not (child_path and parent_path):
            raise HTTPException(status_code=404,
                                detail='source file paths unavailable')
        try:
            child_src = Path(child_path).read_text().splitlines(keepends=True)
            parent_src = Path(parent_path).read_text().splitlines(keepends=True)
        except OSError as e:
            raise HTTPException(status_code=500, detail=f'read failed: {e}')
        unified = ''.join(difflib.unified_diff(
            parent_src, child_src,
            fromfile=os.path.basename(parent_path),
            tofile=os.path.basename(child_path),
            n=3))
        return {'chain': chain, 'parent': ch.parent,
                'parent_path': parent_path, 'child_path': child_path,
                'diff': unified}

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
    def _scoreboard(name: str):
        """Cross-variation scoreboard: rows = variations, columns =
        every metric name written via `ctx.metric()` across the chain's
        runs. Metric values come from each stage's metrics.json file.
        """
        chain, _ = _load_chain(name)
        all_runs = app.state.registry.list_runs()
        #Index runs by pipeline+variant+upstream-fp signature for fast
        #lookup, same logic as _chain_progress.
        rows = []
        all_metric_keys: set[str] = set()
        for var_name, var in chain.variations.items():
            try:
                mapping = var.resolve()
            except Exception:
                continue
            upstream_fps: dict[str, Optional[str]] = {}
            metrics: dict[str, dict] = {}
            for step in chain.steps:
                variant_name = mapping.get(step.name)
                if variant_name is None:
                    upstream_fps[step.name] = None
                    continue
                required = {u: upstream_fps.get(u) for u in step.consumes}
                if any(v is None for v in required.values()):
                    upstream_fps[step.name] = None
                    continue
                match = None
                for r in all_runs:
                    if r.pipeline != step.pipeline.name: continue
                    if r.variant != variant_name: continue
                    if dict(r.upstream or {}) != required: continue
                    match = r
                    break
                if match is None:
                    upstream_fps[step.name] = None
                    continue
                upstream_fps[step.name] = match.fingerprint
                #Collect metrics from every stage's metrics.json.
                stages_dir = os.path.join(match.fdir, 'stages')
                if not os.path.isdir(stages_dir):
                    continue
                step_metrics = {}
                for st in sorted(os.listdir(stages_dir)):
                    mp = os.path.join(stages_dir, st, 'metrics.json')
                    if not os.path.isfile(mp):
                        continue
                    try:
                        data = json.loads(Path(mp).read_text())
                    except Exception:
                        continue
                    if not isinstance(data, dict):
                        continue
                    for k, v in data.items():
                        key = f'{step.name}.{st}.{k}'
                        step_metrics[key] = v
                        all_metric_keys.add(key)
                metrics[step.name] = step_metrics
            #Flatten this variation's metrics into one row.
            flat = {}
            for step_metrics in metrics.values():
                flat.update(step_metrics)
            rows.append({'variation': var_name, 'metrics': flat})
        return {'chain': name,
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
        base = os.path.realpath(app.state.registry.root)
        candidate = os.path.realpath(
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


def _stage_summaries(record) -> list[dict]:
    out = []
    stages_dir = os.path.join(record.fdir, 'stages')
    if not os.path.isdir(stages_dir):
        return out
    for st_name in sorted(os.listdir(stages_dir)):
        outs = os.path.join(stages_dir, st_name, 'outputs')
        artifacts = []
        if os.path.isdir(outs):
            for root, _, files in os.walk(outs):
                for fn in files:
                    artifacts.append(os.path.relpath(
                        os.path.join(root, fn), record.fdir))
        out.append({
            'name': st_name,
            'status': record.stage_status.get(st_name),
            'key': record.stage_keys.get(st_name),
            'artifact_count': len(artifacts),
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


def _compute_artifact_diff(path_a: str, path_b: str, target_max: int) -> dict:
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
        return {'kind': 'error',
                'note': 'h5 diff: pass dataset= via render_dataset for now'}

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
        rel = np.where(denom > 0, abs_d / denom, 0.0)
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
    return meta


def _safe_under(path: str, root: str) -> bool:
    real = os.path.realpath(path)
    base = os.path.realpath(root)
    return real == base or real.startswith(base + os.sep)


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
