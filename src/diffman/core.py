"""Core data structures: Config, Variant, Stage, Pipeline, RunRegistry.

Ported from srwl_uti_diffman.py (in SRW) with class/function names dropped
to plain (no SRWLDfm prefix), conventions modernized (type hints, Path),
and the git-script-backup hook moved out into git_backup.py.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import pickle
import socket
import subprocess
import time
import traceback
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None
    _HAS_NUMPY = False


FP_VERSION = 1
"""Bump to invalidate every existing cache entry (e.g. on SRW upgrade)."""


# ---------------------------------------------------------------------------
# Config tree
# ---------------------------------------------------------------------------

class Config(dict):
    """Nested dict with attribute access and deep merge.

    Lookups via attribute (`cfg.scan.width`) descend into nested Config
    instances. Assigning a plain dict auto-promotes to Config.
    """

    def __init__(self, *layers, **kw):
        super().__init__()
        for layer in layers:
            if layer:
                self._merge(layer)
        if kw:
            self._merge(kw)

    def _merge(self, other):
        for k, v in other.items():
            if isinstance(v, dict) and not isinstance(v, Config):
                v = Config(v)
            if k in self and isinstance(self[k], Config) and isinstance(v, Config):
                self[k]._merge(v)
            elif isinstance(v, Config):
                fresh = Config()
                fresh._merge(v)
                self[k] = fresh
            else:
                self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(key) from e

    def __setattr__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, Config):
            value = Config(value)
        self[key] = value

    def derive(self, **overrides) -> 'Config':
        out = Config(self)
        out._merge(overrides)
        return out

    def merged(self) -> dict:
        return {
            k: v.merged() if isinstance(v, Config) else v
            for k, v in self.items()
        }

    def subtree(self, keys: Iterable[str]) -> 'Config':
        return Config({k: self[k] for k in keys if k in self})

    def fingerprint(self, include=None, exclude=None) -> str:
        return fingerprint(_restrict(self, include, exclude))


def _restrict(cfg: Config, include, exclude) -> Config:
    if include is None and exclude is None:
        return cfg
    out = Config()
    inc = set(include) if include is not None else None
    exc = set(exclude) if exclude is not None else None
    for k, v in cfg.items():
        if inc is not None and k not in inc:
            continue
        if exc is not None and k in exc:
            continue
        out[k] = v
    return out


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------

def fingerprint(obj: Any) -> str:
    """Stable sha256 hex digest for arbitrary Python objects.

    Canonical typed encoding for primitives / list / tuple / dict; numpy
    arrays via (shape, dtype, sha-of-bytes); anything else via pickle.dumps
    prefixed with FP_VERSION so SRW-version bumps invalidate caches.
    """
    h = hashlib.sha256()
    _digest(obj, h)
    return h.hexdigest()


def _digest(obj, h):
    if obj is None:
        h.update(b'N\x00')
    elif isinstance(obj, bool):
        h.update(b'B' + (b'1' if obj else b'0'))
    elif isinstance(obj, int):
        h.update(b'I' + repr(obj).encode())
    elif isinstance(obj, float):
        h.update(b'F' + repr(obj).encode())
    elif isinstance(obj, str):
        b = obj.encode('utf-8')
        h.update(b'S' + len(b).to_bytes(8, 'big') + b)
    elif isinstance(obj, bytes):
        h.update(b'b' + len(obj).to_bytes(8, 'big') + obj)
    elif isinstance(obj, (list, tuple)):
        h.update(b'L' if isinstance(obj, list) else b'T')
        h.update(len(obj).to_bytes(8, 'big'))
        for item in obj:
            _digest(item, h)
    elif isinstance(obj, dict):
        h.update(b'D')
        items = sorted(obj.items(), key=lambda kv: kv[0])
        h.update(len(items).to_bytes(8, 'big'))
        for k, v in items:
            _digest(k, h)
            _digest(v, h)
    elif _HAS_NUMPY and isinstance(obj, np.ndarray):
        a = np.ascontiguousarray(obj)
        h.update(b'A')
        h.update(repr(a.shape).encode())
        h.update(a.dtype.str.encode())
        h.update(hashlib.sha256(a.tobytes()).digest())
    elif _HAS_NUMPY and isinstance(obj, (np.integer, np.floating, np.bool_)):
        _digest(obj.item(), h)
    else:
        try:
            payload = pickle.dumps(obj, protocol=4)
        except Exception:
            payload = repr(obj).encode()
        h.update(b'P')
        h.update(FP_VERSION.to_bytes(4, 'big'))
        h.update(hashlib.sha256(payload).digest())


def _fn_source_hash(fn: Callable) -> str:
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        src = repr(fn)
    return hashlib.sha256(src.encode('utf-8')).hexdigest()


# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

class Variant:
    def __init__(self, name: str, base: Optional['Variant'], overrides: dict,
                 module: Optional[str] = None):
        self.name = name
        self.base = base
        self.overrides = Config(overrides)
        #Fully-qualified module name that called `register()`. None for
        #ad-hoc/synthetic variants (e.g. override-variants built at submit time).
        self.module = module

    @property
    def config(self) -> Config:
        if self.base is None:
            return Config(self.overrides)
        return self.base.config.derive(**self.overrides)

    @property
    def fingerprint(self) -> str:
        return self.config.fingerprint()

    def short_fingerprint(self, n: int = 12) -> str:
        return self.fingerprint[:n]

    def __repr__(self):
        b = self.base.name if self.base else None
        return f"Variant({self.name!r}, base={b!r})"


class VariantRegistry:
    def __init__(self):
        self._variants: dict[str, Variant] = {}

    def register(self, name: str, *, base: Optional[str] = None,
                 module: Optional[str] = None, **overrides) -> Variant:
        if name in self._variants:
            raise ValueError(f"variant {name!r} already registered")
        base_v = self._variants[base] if base is not None else None
        v = Variant(name, base_v, overrides, module=module)
        self._variants[name] = v
        return v

    def for_module(self, module: str) -> list[str]:
        return [v.name for v in self._variants.values() if v.module == module]

    def get(self, name: str) -> Variant:
        return self._variants[name]

    def __iter__(self) -> Iterator[Variant]:
        return iter(self._variants.values())

    def names(self) -> list[str]:
        return list(self._variants.keys())


registry = VariantRegistry()


def register(name: str, *, base: Optional[str] = None, **overrides) -> Variant:
    """Convenience wrapper around the module-default registry.

    Records the caller's `__name__` on the Variant so the UI can list
    variants per-pipeline instead of dumping the global registry.
    """
    caller = inspect.currentframe().f_back
    mod_name = caller.f_globals.get('__name__') if caller else None
    return registry.register(name, base=base, module=mod_name, **overrides)


# ---------------------------------------------------------------------------
# Stages & pipelines
# ---------------------------------------------------------------------------

@dataclass
class Stage:
    name: str
    fn: Callable
    inputs: tuple = ()
    config_keys: tuple = ()
    produces: tuple = ()
    sharded: bool = False

    def key(self, variant: Variant, upstream_keys: dict) -> str:
        cfg = (variant.config.subtree(self.config_keys).merged()
               if self.config_keys else variant.config.merged())
        return fingerprint({
            'stage': self.name,
            'fn': _fn_source_hash(self.fn),
            'config': cfg,
            'upstream': {k: upstream_keys[k] for k in self.inputs},
        })


class Pipeline:
    """An ordered list of Stages with cache-aware dispatch."""

    def __init__(self, name: str, stages: list[Stage]):
        self.name = name
        self.stages = list(stages)
        self._by_name = {s.name: s for s in self.stages}
        self._source_file: Optional[str] = None  #set by discovery.load_module
        self._validate()

    def _validate(self):
        seen = set()
        for s in self.stages:
            for inp in s.inputs:
                if inp not in seen:
                    raise ValueError(
                        f"stage {s.name!r} declares input {inp!r} that hasn't run yet")
            seen.add(s.name)

    def run(self, variant: Variant, registry: 'RunRegistry', *,
            force: Optional[set] = None,
            only: Optional[set] = None) -> 'RunRecord':
        force = set(force or ())
        only = set(only) if only else None
        ctx = registry.open_run(self.name, variant)
        upstream_keys: dict[str, str] = {}

        #Snapshot pipeline source into the git backup repo (best-effort).
        if self._source_file:
            try:
                from .git_backup import snapshot
                snapshot(registry.root, self._source_file,
                         f'run {self.name}/{variant.name}/{variant.short_fingerprint()}')
            except Exception:
                pass

        for stage in self.stages:
            key = stage.key(variant, upstream_keys)
            ctx.record.stage_keys[stage.name] = key

            run_stage = True
            if only is not None and stage.name not in only:
                run_stage = False
            if stage.name in force:
                run_stage = True

            stage_dir = ctx.stage_dir(stage.name)
            key_file = Path(stage_dir) / '_key'
            cached = key_file.exists() and key_file.read_text().strip() == key

            if not run_stage:
                ctx.record.stage_status[stage.name] = 'skipped'
                upstream_keys[stage.name] = key
                continue
            if cached and stage.name not in force:
                ctx.record.stage_status[stage.name] = 'cached'
                upstream_keys[stage.name] = key
                continue

            ctx.record.stage_status[stage.name] = 'running'
            registry._flush(ctx.record)
            t0 = time.time()
            try:
                outputs = stage.fn(ctx) or {}
                meta = {
                    'name': stage.name,
                    'key': key,
                    'started': t0,
                    'ended': time.time(),
                    'outputs': {k: str(v) for k, v in outputs.items()},
                }
                Path(stage_dir).mkdir(parents=True, exist_ok=True)
                (Path(stage_dir) / '_meta.json').write_text(json.dumps(meta, indent=2))
                key_file.write_text(key)
                ctx.record.stage_status[stage.name] = 'done'
            except Exception:
                ctx.record.stage_status[stage.name] = 'failed'
                ctx.record.errors[stage.name] = traceback.format_exc()
                registry._flush(ctx.record)
                raise
            upstream_keys[stage.name] = key
            registry._flush(ctx.record)

        ctx.record.ended = _now()
        registry._flush(ctx.record)
        return ctx.record


# ---------------------------------------------------------------------------
# Run registry
# ---------------------------------------------------------------------------

@dataclass
class RunRecord:
    pipeline: str
    variant: str
    fingerprint: str
    fdir: str
    started: str
    ended: Optional[str] = None
    stage_keys: dict = field(default_factory=dict)
    stage_status: dict = field(default_factory=dict)
    errors: dict = field(default_factory=dict)
    git_rev: Optional[str] = None
    host: Optional[str] = None
    slurm: Optional[dict] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


class RunContext:
    def __init__(self, fdir: str, variant: Variant, record: RunRecord):
        self.fdir = fdir
        self.variant = variant
        self.record = record

    def stage_dir(self, stage_name: str) -> str:
        d = os.path.join(self.fdir, 'stages', stage_name)
        os.makedirs(os.path.join(d, 'outputs'), exist_ok=True)
        return d

    def artifact(self, stage_name: str, relpath: str) -> str:
        return os.path.join(self.stage_dir(stage_name), 'outputs', relpath)

    def log(self, msg: str) -> None:
        with open(os.path.join(self.fdir, 'run.log'), 'a') as f:
            f.write(f'[{_now()}] {msg}\n')


class RunRegistry:
    """Owns the on-disk layout: <root>/<pipeline>/<variant>/<fp>/..."""

    def __init__(self, root: str = 'runs'):
        self.root = root

    def open_run(self, pipeline: str, variant: Variant) -> RunContext:
        fp = variant.short_fingerprint()
        fdir = os.path.join(self.root, pipeline, variant.name, fp)
        os.makedirs(fdir, exist_ok=True)
        record = RunRecord(
            pipeline=pipeline,
            variant=variant.name,
            fingerprint=variant.fingerprint,
            fdir=fdir,
            started=_now(),
            git_rev=_git_rev(),
            host=socket.gethostname(),
            slurm=_slurm_meta(),
        )
        ctx = RunContext(fdir, variant, record)
        self._flush(record)
        with open(os.path.join(fdir, 'config.json'), 'w') as f:
            json.dump(variant.config.merged(), f, indent=2, default=str)
        return ctx

    def _flush(self, record: RunRecord) -> None:
        with open(os.path.join(record.fdir, 'run.json'), 'w') as f:
            f.write(record.to_json())

    def list_runs(self, *, pipeline=None, variant=None) -> list[RunRecord]:
        out: list[RunRecord] = []
        root = Path(self.root)
        if not root.exists():
            return out
        for run_json in root.glob('*/*/*/run.json'):
            if pipeline and run_json.parts[-4] != pipeline:
                continue
            if variant and run_json.parts[-3] != variant:
                continue
            try:
                data = json.loads(run_json.read_text())
            except Exception:
                continue
            out.append(RunRecord(**data))
        return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%S')


def _git_rev() -> Optional[str]:
    try:
        out = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL, timeout=2,
        )
        return out.decode().strip()
    except Exception:
        return None


def _slurm_meta() -> Optional[dict]:
    keys = ('SLURM_JOB_ID', 'SLURM_PROCID', 'SLURM_NNODES',
            'SLURM_NTASKS', 'SLURM_JOB_NAME')
    found = {k: os.environ[k] for k in keys if k in os.environ}
    return found or None
