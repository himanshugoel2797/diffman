"""FastAPI server: REST API + WebSocket push + static UI.

Routes:
  GET  /                                          → SPA shell
  GET  /static/...                                → app.js, style.css
  GET  /api/modules                               → list known modules
  GET  /api/scan?root=<path>                      → re-discover modules
  GET  /api/variants?module=<name>                → variant names for a module
  GET  /api/describe?module=&variant=&var=...     → resolved config (w/ overrides)
  GET  /api/runs[?pipeline=&variant=]             → all runs
  GET  /api/run/{pipeline}/{variant}/{fp}         → single run detail + stages
  GET  /api/stage/{pipeline}/{variant}/{fp}/{st}  → stage detail + artifact list
  GET  /api/render?path=<abs>                     → renderer payload for a file
  GET  /api/render_dataset?path=&dataset=         → h5 dataset preview
  GET  /artifact/{pipeline}/{variant}/{fp}/{rest} → raw file download
  GET  /api/submitter                             → submitter kind + defaults
  POST /api/launch                                → submit a run
  GET  /api/script?module=<name>                  → pipeline .py source + fork sidecar
  PUT  /api/script   {module, source}             → save edits to a pipeline .py
  POST /api/fork_script {parent_module, new_name} → copy parent .py to new name
  WS   /ws                                        → push updates (run_changed, etc.)
"""

from __future__ import annotations

import ast
import asyncio
import json
import mimetypes
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import discovery, renderers
from .core import (
    RunRegistry, Variant, fingerprint as _fp, registry as _global_registry,
)
from .submitters import Submitter, default_submitter

# Optional: watchdog for filesystem push.
try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _HAS_WATCHDOG = True
except ImportError:
    _HAS_WATCHDOG = False


# ---------------------------------------------------------------------------
# Override parsing (shared with CLI)
# ---------------------------------------------------------------------------

_BOOL_TOKENS = {'true': True, 'false': False, 'none': None, 'null': None}


def parse_value(s: str):
    s = s.strip()
    if s.lower() in _BOOL_TOKENS:
        return _BOOL_TOKENS[s.lower()]
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return s


def parse_overrides(entries) -> dict:
    out = {}
    for entry in entries or ():
        if '=' not in entry:
            raise ValueError(f'expected key=value, got {entry!r}')
        key, val = entry.split('=', 1)
        v = parse_value(val)
        parts = key.strip().split('.')
        cur = out
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = v
    return out


def make_override_variant(base: Variant, overrides: dict) -> Variant:
    """Build a synthetic Variant carrying inline config overrides.

    Not a script fork — purely a config tweak on top of `base`. Name is
    `<base>+<short-fp-of-overrides>` so a given override set has a stable
    run directory. Inherits `module` from base for UI attribution.
    """
    if not overrides:
        return base
    v = Variant(base.name, base, overrides, module=base.module)
    short = _fp(overrides)[:8]
    v.name = f'{base.name}+{short}'
    return v


def flatten_overrides(d, prefix=''):
    for k, v in d.items():
        full = f'{prefix}.{k}' if prefix else k
        if isinstance(v, dict):
            yield from flatten_overrides(v, full)
        else:
            yield full, v


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
        def __init__(self, broadcaster: _Broadcaster, runs_root: str):
            self.bcast = broadcaster
            self.runs_root = os.path.abspath(runs_root)
            self._last = {}

        def _coalesce(self, key: str, payload: dict, *, debounce=0.5):
            now = time.time()
            if key in self._last and now - self._last[key] < debounce:
                return
            self._last[key] = now
            self.bcast.schedule(payload)

        def _key(self, src_path: str) -> Optional[dict]:
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
            k = self._key(event.src_path)
            if k is None:
                return
            ckey = f"{k['pipeline']}/{k['variant']}/{k['fp']}"
            self._coalesce(ckey, {'type': 'run_changed', **k,
                                  'path': event.src_path,
                                  'event': event.event_type})


# ---------------------------------------------------------------------------
# FastAPI app factory
# ---------------------------------------------------------------------------

def create_app(*,
               root: str = 'runs',
               scan_root: str = '.',
               submitter: Optional[Submitter] = None,
               no_scan: bool = False) -> FastAPI:
    app = FastAPI(title='diffman')
    app.state.registry = RunRegistry(root=root)
    app.state.scan_root = scan_root
    app.state.submitter = submitter or default_submitter('auto')
    app.state.bcast = _Broadcaster()
    app.state.observer = None

    if not no_scan:
        n = len(discovery.discover(scan_root))
        print(f'[diffman] discovered {n} pipeline module(s) under {os.path.abspath(scan_root)}')

    ui_dir = Path(__file__).parent / 'ui'
    app.mount('/static', StaticFiles(directory=str(ui_dir)), name='static')

    @app.on_event('startup')
    async def _startup():
        app.state.bcast.loop = asyncio.get_running_loop()
        if _HAS_WATCHDOG:
            obs = Observer()
            handler = _RunsWatcher(app.state.bcast, app.state.registry.root)
            os.makedirs(app.state.registry.root, exist_ok=True)
            obs.schedule(handler, app.state.registry.root, recursive=True)
            obs.start()
            app.state.observer = obs
            print('[diffman] watchdog: tailing', os.path.abspath(app.state.registry.root))

    @app.on_event('shutdown')
    async def _shutdown():
        if app.state.observer is not None:
            app.state.observer.stop()
            app.state.observer.join(timeout=2)

    # --- static SPA ------------------------------------------------------
    @app.get('/', response_class=HTMLResponse)
    def _index():
        return (ui_dir / 'index.html').read_text()

    # --- discovery / modules --------------------------------------------
    def _modules_payload():
        return discovery.DISCOVERED_LIST

    @app.get('/api/modules')
    def _modules():
        return {'modules': _modules_payload(),
                'scan_root': os.path.abspath(app.state.scan_root)}

    @app.get('/api/submitter')
    def _submitter_info():
        sub = app.state.submitter
        defaults = getattr(sub, 'sbatch_flags', None)
        return {
            'kind': sub.kind,
            'accepts_sbatch_flags': sub.kind == 'slurm',
            'default_sbatch_flags': list(defaults) if defaults else [],
        }

    @app.get('/api/scan')
    def _scan(root: Optional[str] = None):
        if root:
            app.state.scan_root = root
        found = discovery.discover(app.state.scan_root)
        return {'root': os.path.abspath(app.state.scan_root),
                'modules': found}

    # --- variants / describe --------------------------------------------
    @app.get('/api/variants')
    def _variants(module: str):
        try:
            discovery.load_module(module)
        except Exception as e:
            raise HTTPException(status_code=400,
                                detail=f'import {module}: {e}')
        #Prefer authoritative per-Variant module attribution (set at
        #register() time). Fall back to the import-time diff captured by
        #discovery.load_module for variants registered before this change.
        names = _global_registry.for_module(module)
        if not names:
            names = discovery.module_variants(module)
        return {'module': module, 'variants': names}

    @app.get('/api/describe')
    def _describe(module: str, variant: str, var: list[str] = ()):
        try:
            discovery.load_module(module)
            base = _global_registry.get(variant)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        overrides = parse_overrides(var)
        v = make_override_variant(base, overrides)
        return {'module': module, 'variant': v.name,
                'fingerprint': v.fingerprint,
                'config': v.config.merged()}

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
        """Return an SRW-aware preview: 2D heatmap + h/v cuts at (row, col).

        - repr ∈ {intensity, amplitude, phase, real, imag}
        - polarization ∈ {both, Ex, Ey} (wavefields only)
        - energy_slice = -1 sums (intensity/amplitude) or picks center
          (phase/real/imag).
        - row/col = -1 means center cut.
        """
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

    # --- script forks ----------------------------------------------------
    # A "script fork" is a real on-disk copy of a pipeline .py with a
    # recorded parent. Distinct from an "override variant" (config tweak,
    # in-memory only). The fork is editable and discoverable like any
    # other pipeline module.

    def _module_file(module: str) -> str:
        d = discovery.DISCOVERED_PATHS.get(module)
        if not d:
            raise HTTPException(status_code=404,
                                detail=f'unknown module {module!r}; rescan?')
        path = os.path.join(d, f'{module}.py')
        if not os.path.isfile(path):
            raise HTTPException(status_code=404, detail=f'no file at {path}')
        return path

    _NAME_RE = __import__('re').compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    @app.get('/api/script')
    def _get_script(module: str):
        path = _module_file(module)
        sidecar = path[:-3] + '.fork.json'
        info = None
        if os.path.isfile(sidecar):
            try:
                info = json.loads(Path(sidecar).read_text())
            except Exception:
                info = {'error': 'sidecar unreadable'}
        return {'module': module, 'path': path,
                'source': Path(path).read_text(),
                'fork': info}

    @app.put('/api/script')
    async def _put_script(req: Request):
        data = await req.json()
        module = data.get('module')
        source = data.get('source')
        if not module or source is None:
            raise HTTPException(status_code=400,
                                detail='module and source required')
        path = _module_file(module)
        Path(path).write_text(source)
        #Snapshot the edit into _scripts/.git/ for traceability.
        try:
            from .git_backup import snapshot
            snapshot(app.state.registry.root, path,
                     f'edit {module} via UI')
        except Exception:
            pass
        return {'module': module, 'path': path, 'bytes': len(source)}

    @app.post('/api/fork_script')
    async def _fork_script(req: Request):
        import re as _re
        data = await req.json()
        parent_module = data.get('parent_module')
        new_name = (data.get('new_name') or '').strip()
        if not parent_module or not new_name:
            raise HTTPException(status_code=400,
                                detail='parent_module and new_name required')
        if not _NAME_RE.match(new_name):
            raise HTTPException(status_code=400,
                                detail='new_name must be a valid Python identifier')
        parent_path = _module_file(parent_module)
        parent_dir = os.path.dirname(parent_path)
        new_path = os.path.join(parent_dir, f'{new_name}.py')
        if os.path.exists(new_path):
            raise HTTPException(status_code=409,
                                detail=f'{new_path} already exists')

        src = Path(parent_path).read_text()

        #Rewrite the first `Pipeline('<parent_module>', ...)` literal so
        #runs land under a distinct top-level dir. Parents that build the
        #name dynamically are left alone; the user can edit by hand.
        new_src, n_pipe = _re.subn(
            r"""(Pipeline\s*\(\s*)(['"])""" + _re.escape(parent_module) + r"""\2""",
            lambda m: f"{m.group(1)}{m.group(2)}{new_name}{m.group(2)}",
            src, count=1)

        #Rewrite variant names so the fork's dm.register() calls don't
        #collide with the parent's when both modules are loaded in the
        #same server process. We prefix every variant name in this file:
        #`register('X', ...)` and `base='X'` both get the new prefix.
        #Discover existing names by parsing the parent module's
        #attributed variants.
        discovery.load_module(parent_module)
        existing = set(_global_registry.for_module(parent_module))
        prefix = f'{new_name}__'

        def _rename(m, existing=existing, prefix=prefix):
            head, q, nm = m.group(1), m.group(2), m.group(3)
            if nm in existing:
                return f'{head}{q}{prefix}{nm}{q}'
            return m.group(0)

        # register('name', ...) / dm.register('name', ...)
        new_src, n_reg = _re.subn(
            r"""(\bregister\s*\(\s*)(['"])([A-Za-z_][A-Za-z0-9_]*)\2""",
            _rename, new_src)
        # base='name'
        new_src, n_base = _re.subn(
            r"""(\bbase\s*=\s*)(['"])([A-Za-z_][A-Za-z0-9_]*)\2""",
            _rename, new_src)

        Path(new_path).write_text(new_src)

        sidecar = {
            'parent_module': parent_module,
            'parent_path': parent_path,
            'created': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'pipeline_name_rewritten': bool(n_pipe),
            'variant_prefix': prefix,
            'variants_renamed': n_reg,
            'base_refs_renamed': n_base,
        }
        sidecar_path = new_path[:-3] + '.fork.json'
        Path(sidecar_path).write_text(json.dumps(sidecar, indent=2))

        try:
            from .git_backup import snapshot
            snapshot(app.state.registry.root, new_path,
                     f'fork {parent_module} -> {new_name}')
            snapshot(app.state.registry.root, sidecar_path,
                     f'fork sidecar for {new_name}')
        except Exception:
            pass

        #Re-scan so the fork shows up in /api/modules.
        discovery.discover(app.state.scan_root)
        return {'module': new_name, 'path': new_path,
                'sidecar': sidecar_path, 'sidecar_data': sidecar}

    # --- launch ----------------------------------------------------------
    @app.post('/api/launch')
    async def _launch(req: Request):
        data = await req.json()
        module = data.get('module')
        variant = data.get('variant')
        if not module or not variant:
            raise HTTPException(status_code=400,
                                detail='module and variant required')
        discovery.load_module(module)
        base = _global_registry.get(variant)
        overrides = parse_overrides(data.get('vars') or [])
        ov = make_override_variant(base, overrides)
        cmd = [sys.executable, '-m', 'diffman', 'run', module, variant,
               '--runs-root', app.state.registry.root]
        for k, v in flatten_overrides(overrides):
            cmd += ['--var', f'{k}={v}']
        if data.get('only'):
            cmd += ['--only', data['only']]
        if data.get('force'):
            cmd += ['--force', data['force']]

        log_dir = os.path.join(app.state.registry.root, '_jobs',
                               f'{ov.name}_{ov.short_fingerprint()}')
        env = os.environ.copy()
        extra = discovery.DISCOVERED_PATHS.get(module)
        if extra:
            pp = env.get('PYTHONPATH', '')
            env['PYTHONPATH'] = extra + (os.pathsep + pp if pp else '')
        #Per-launch sbatch overrides. Accepts a shell-style string
        #('--partition=regular --time=01:00:00') or a list of flags.
        #Silently ignored by LocalSubmitter.
        raw_flags = data.get('sbatch_flags')
        if isinstance(raw_flags, str):
            extra_flags = shlex.split(raw_flags)
        elif isinstance(raw_flags, list):
            extra_flags = list(raw_flags)
        else:
            extra_flags = None
        info = app.state.submitter.submit(cmd, cwd=os.getcwd(),
                                          env=env, log_dir=log_dir,
                                          extra_flags=extra_flags)
        info.update({'cmd': cmd, 'variant_name': ov.name,
                     'variant_short_fp': ov.short_fingerprint()})
        app.state.bcast.schedule({'type': 'launch', **info})
        return info

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


def _safe_under(path: str, root: str) -> bool:
    real = os.path.realpath(path)
    base = os.path.realpath(root)
    return real == base or real.startswith(base + os.sep)


def run(*, root='runs', port=8765, bind='127.0.0.1',
        scan_root='.', submitter=None, no_scan=False):
    """Start the server (blocking). Convenience wrapper around uvicorn.run()."""
    import uvicorn
    app = create_app(root=root, scan_root=scan_root,
                     submitter=submitter, no_scan=no_scan)
    if bind != '127.0.0.1':
        print(f'[diffman] warning: binding to {bind}; the UI is unauthenticated.',
              file=sys.stderr)
    print(f'diffman UI: http://{bind}:{port}  (runs: {os.path.abspath(root)})')
    uvicorn.run(app, host=bind, port=port, log_level='warning')
