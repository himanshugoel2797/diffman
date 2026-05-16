"""Core data structures: Config, Variant, Stage, Pipeline, RunRegistry.

Minimal API used by hand-written pipeline modules:

    import diffman as dm
    dm.register('base',   scan=dict(width=5e-6))
    dm.register('jitter', base='base', probe=dict(jitter=True))

    def _sim(ctx): ...
    PIPELINE = dm.Pipeline('mypipe', [dm.Stage('sim', _sim)],
                           parent='other_pipeline')

The pipeline is responsible for invoking `PIPELINE.run(variant, registry)`
itself; diffman does not launch runs.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import socket
import subprocess
import time
import traceback
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Iterable, Optional


# ---------------------------------------------------------------------------
# Config: nested dict with attribute access and deep merge
# ---------------------------------------------------------------------------

class Config(dict):
    """Nested dict supporting attribute access. `cfg.scan.width` descends.

    Plain dicts assigned at any level auto-promote to Config so attribute
    access works at every depth.
    """

    def __init__(self, *layers, **kw):
        super().__init__()
        for layer in layers:
            if layer:
                _deep_merge_into(self, layer)
        if kw:
            _deep_merge_into(self, kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(key) from e

    def __setattr__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, Config):
            value = Config(value)
        self[key] = value


def _deep_merge_into(dst: dict, src: dict) -> None:
    for k, v in src.items():
        if isinstance(v, dict) and not isinstance(v, Config):
            v = Config(v)
        if isinstance(dst.get(k), Config) and isinstance(v, Config):
            _deep_merge_into(dst[k], v)
        elif isinstance(v, Config):
            #Always deep-copy a Config into dst so callers can't mutate
            #the source layer by later modifying the destination.
            #Config(v) walks v via this same function recursively.
            dst[k] = Config(v)
        else:
            dst[k] = v


# ---------------------------------------------------------------------------
# Fingerprinting (JSON-canonical sha256)
# ---------------------------------------------------------------------------

def fingerprint(obj: Any) -> str:
    """Stable sha256 hex digest of a JSON-serializable object.

    Configs registered via `dm.register()` are dicts of primitives — we
    canonicalize via `json.dumps(..., sort_keys=True, default=str)` and
    hash the bytes. Non-JSON values fall back to `str(value)` so this
    never raises.
    """
    payload = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def _fn_source_hash(fn: Callable) -> str:
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        src = repr(fn)
    return hashlib.sha256(src.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

class Variant:
    """A named config layer with optional inheritance from another Variant.

    `config` walks the base chain and deep-merges. `module` is the
    `__name__` of the file that registered this variant; used to scope
    same-named variants across different pipeline modules.
    """

    __slots__ = ('name', 'base', 'overrides', 'module', 'forks_of')

    def __init__(self, name: str, base: Optional['Variant'], overrides: dict,
                 module: Optional[str] = None,
                 forks_of: Optional[str] = None):
        self.name = name
        self.base = base
        self.overrides = Config(overrides)
        self.module = module
        #Cross-pipeline lineage: name of the corresponding variant in the
        #parent pipeline. Used by /api/diff to match variants across forks
        #when names changed. None means "match by name".
        self.forks_of = forks_of

    @property
    def config(self) -> Config:
        if self.base is None:
            return Config(self.overrides)
        merged = Config(self.base.config)
        _deep_merge_into(merged, self.overrides)
        return merged

    @property
    def fingerprint(self) -> str:
        return fingerprint(_to_plain(self.config))

    @property
    def short_fp(self) -> str:
        return self.fingerprint[:12]

    def __repr__(self):
        b = self.base.name if self.base else None
        return f'Variant({self.name!r}, base={b!r}, module={self.module!r})'


def _to_plain(d) -> dict:
    """Strip Config wrappers so json.dumps sees vanilla dicts everywhere."""
    if isinstance(d, dict):
        return {k: _to_plain(v) for k, v in d.items()}
    return d


class VariantRegistry:
    """Maps `(module, variant_name)` -> Variant.

    Keying on the pair lets two pipeline modules each register a variant
    called e.g. `base` without collision. `module` is captured from the
    caller's `__name__` by the top-level `register()`.
    """

    def __init__(self):
        self._variants: dict[tuple[Optional[str], str], Variant] = {}

    def register(self, name: str, *, base: Optional[str] = None,
                 module: Optional[str] = None,
                 forks_of: Optional[str] = None,
                 **overrides) -> Variant:
        key = (module, name)
        if key in self._variants:
            raise ValueError(
                f'variant {name!r} already registered in {module!r}')
        base_v = None
        if base is not None:
            base_v = self._variants.get((module, base))
            if base_v is None:
                #Allow forks to declare base=parent's variant; only one
                #cross-module match is permitted (ambiguity is an error).
                matches = [v for (m, n), v in self._variants.items() if n == base]
                if len(matches) != 1:
                    raise KeyError(
                        f'base variant {base!r} not found' if not matches
                        else f'base {base!r} is ambiguous across modules')
                base_v = matches[0]
        v = Variant(name, base_v, overrides, module=module, forks_of=forks_of)
        self._variants[key] = v
        return v

    def get(self, module: str, name: str) -> Variant:
        return self._variants[(module, name)]

    def for_module(self, module: str) -> list[str]:
        return [n for (m, n) in self._variants if m == module]

    def drop_module(self, module: str) -> None:
        """Forget every variant registered by `module`. Used when a
        pipeline file is re-imported after an edit."""
        for k in [k for k in self._variants if k[0] == module]:
            del self._variants[k]

    def __iter__(self):
        return iter(self._variants.values())


registry = VariantRegistry()


def register(name: str, *, base: Optional[str] = None,
             forks_of: Optional[str] = None, **overrides) -> Variant:
    """Register a variant, attributing it to the calling module.

    `forks_of='other_variant'` declares that this variant is a renamed
    descendant of `other_variant` in the parent pipeline; the UI uses it
    to line up cross-fork diffs when variant names changed.
    """
    caller = inspect.currentframe().f_back
    mod_name = caller.f_globals.get('__name__') if caller else None
    return registry.register(name, base=base, module=mod_name,
                             forks_of=forks_of, **overrides)


# ---------------------------------------------------------------------------
# Stages & pipelines
# ---------------------------------------------------------------------------

@dataclass
class Stage:
    name: str
    fn: Callable
    inputs: tuple = ()
    config_keys: tuple = ()

    def key(self, variant: Variant, upstream_keys: dict) -> str:
        cfg = variant.config
        if self.config_keys:
            cfg = {k: cfg[k] for k in self.config_keys if k in cfg}
        return fingerprint({
            'stage': self.name,
            'fn': _fn_source_hash(self.fn),
            'config': _to_plain(cfg),
            'upstream': {k: upstream_keys[k] for k in self.inputs},
        })


class Pipeline:
    """An ordered list of Stages with optional fork-parent attribution."""

    def __init__(self, name: str, stages: list[Stage], *,
                 parent: Optional[str] = None):
        self.name = name
        self.stages = list(stages)
        self.parent = parent  # name of the pipeline this was forked from
        self._source_file: Optional[str] = None  # set by discovery.load_module
        self._validate()

    def _validate(self):
        seen = set()
        for s in self.stages:
            for inp in s.inputs:
                if inp not in seen:
                    raise ValueError(
                        f'stage {s.name!r} declares input {inp!r} '
                        f"that hasn't run yet")
            seen.add(s.name)

    def run(self, variant: Variant, registry: 'RunRegistry', *,
            force: Optional[set] = None,
            only: Optional[set] = None) -> 'RunRecord':
        force = set(force or ())
        only = set(only) if only else None
        ctx = registry.open_run(self.name, variant)
        upstream_keys: dict[str, str] = {}

        if self._source_file:
            try:
                from .git_backup import snapshot
                snapshot(registry.root, self._source_file,
                         f'run {self.name}/{variant.name}/{variant.short_fp}')
            except Exception:
                pass

        for stage in self.stages:
            key = stage.key(variant, upstream_keys)
            ctx.record.stage_keys[stage.name] = key

            run_stage = only is None or stage.name in only
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
                    'name': stage.name, 'key': key,
                    'started': t0, 'ended': time.time(),
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
    """Owns the on-disk layout: <root>/<pipeline>/<variant>/<fp>/...

    `list_runs()` caches its result; call `invalidate()` (or let the
    server's watchdog do it) after any filesystem change under `root`.
    """

    def __init__(self, root: str = 'runs'):
        self.root = root
        self._cache: Optional[list['RunRecord']] = None

    def invalidate(self) -> None:
        self._cache = None

    def open_run(self, pipeline: str, variant: Variant) -> RunContext:
        fdir = os.path.join(self.root, pipeline, variant.name, variant.short_fp)
        os.makedirs(fdir, exist_ok=True)
        record = RunRecord(
            pipeline=pipeline,
            variant=variant.name,
            fingerprint=variant.fingerprint,
            fdir=fdir,
            started=_now(),
            git_rev=_git_rev(),
            host=socket.gethostname(),
        )
        ctx = RunContext(fdir, variant, record)
        self._flush(record)
        with open(os.path.join(fdir, 'config.json'), 'w') as f:
            json.dump(_to_plain(variant.config), f, indent=2, default=str)
        self._cache = None
        return ctx

    def _flush(self, record: RunRecord) -> None:
        with open(os.path.join(record.fdir, 'run.json'), 'w') as f:
            json.dump(asdict(record), f, indent=2, default=str)
        self._cache = None

    def _load_all(self) -> list[RunRecord]:
        out: list[RunRecord] = []
        root = Path(self.root)
        if not root.exists():
            return out
        for run_json in root.glob('*/*/*/run.json'):
            try:
                out.append(RunRecord(**json.loads(run_json.read_text())))
            except Exception:
                continue
        return out

    def list_runs(self, *, pipeline=None, variant=None) -> list[RunRecord]:
        if self._cache is None:
            self._cache = self._load_all()
        items = self._cache
        if pipeline:
            items = [r for r in items if r.pipeline == pipeline]
        if variant:
            items = [r for r in items if r.variant == variant]
        return items


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%S')


def _git_rev() -> Optional[str]:
    try:
        out = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL, timeout=2)
        return out.decode().strip()
    except Exception:
        return None
