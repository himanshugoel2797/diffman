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
import logging
import os
import shutil
import socket
import subprocess
import time
import traceback
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Optional

_log = logging.getLogger(__name__)


def _caller_module() -> Optional[str]:
    """Return the calling frame's __name__, two frames up.

    Used by `register()` and `Pipeline()` to attribute themselves to the
    file that constructed them — same source captured by both, so the
    chain layer can resolve a step's variant against the right module.
    """
    return inspect.currentframe().f_back.f_back.f_globals.get('__name__')


def _try_snapshot(root: str, source_file: str, message: str) -> None:
    """Best-effort git snapshot of a pipeline / chain source file.

    Snapshotting is traceability infrastructure, not a run prerequisite,
    so failures don't propagate. They DO get logged at WARNING on every
    occurrence — git_backup itself logs the specific git stderr; this
    layer just ensures a snapshot failure never aborts a run.
    """
    try:
        from .git_backup import snapshot
        snapshot(root, source_file, message)
    except Exception as e:
        _log.warning('git_backup.snapshot failed: %s', e)


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
        return fingerprint(self.config)

    @property
    def short_fp(self) -> str:
        return self.fingerprint[:12]

    def __repr__(self):
        b = self.base.name if self.base else None
        return f'Variant({self.name!r}, base={b!r}, module={self.module!r})'


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
    return registry.register(name, base=base, module=_caller_module(),
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

    def key_components(self, variant: Variant,
                       upstream_keys: dict) -> dict:
        """The three inputs to this stage's cache key, returned as a dict
        so the run-diff endpoint can attribute a re-execution to a
        changed fn / config / upstream rather than just "key flipped"."""
        cfg = variant.config
        if self.config_keys:
            cfg = {k: cfg[k] for k in self.config_keys if k in cfg}
        return {
            'fn': _fn_source_hash(self.fn),
            'config': dict(cfg),
            'upstream': {k: upstream_keys[k] for k in self.inputs},
        }


class Pipeline:
    """An ordered list of Stages with optional fork-parent attribution."""

    def __init__(self, name: str, stages: list[Stage], *,
                 parent: Optional[str] = None,
                 module: Optional[str] = None):
        self.name = name
        self.stages = list(stages)
        self.parent = parent  # name of the pipeline this was forked from
        self._source_file: Optional[str] = None  # set by discovery.load_module
        #Caller's __name__ — used by Chain to resolve variant names against
        #the right module-scoped registry entry. Pass `module=` explicitly
        #to override (constructing Pipelines dynamically in tests).
        self._module = module if module is not None else _caller_module()
        self._validate()

    def _validate(self):
        seen: set[str] = set()
        for s in self.stages:
            for inp in s.inputs:
                if inp not in seen:
                    raise ValueError(
                        f'stage {s.name!r} declares input {inp!r} '
                        f"that hasn't run yet")
            seen.add(s.name)

    def run(self, variant: Variant, registry: 'RunRegistry', *,
            force: Optional[set] = None,
            only: Optional[set] = None,
            upstream: Optional[dict] = None,
            chain: Optional[str] = None,
            variation: Optional[str] = None) -> 'RunRecord':
        force = set(force or ())
        only = set(only) if only else None
        ctx = registry.open_run(self.name, variant,
                                upstream=upstream or {},
                                chain=chain, variation=variation)

        if self._source_file:
            _try_snapshot(registry.root, self._source_file,
                          f'run {self.name}/{variant.name}/'
                          f'{ctx.record.fingerprint[:12]}')

        upstream_keys: dict[str, str] = {}
        for stage in self.stages:
            components = stage.key_components(variant, upstream_keys)
            key = fingerprint({'stage': stage.name, **components})
            ctx.record.stage_keys[stage.name] = key
            upstream_keys[stage.name] = key

            if only is not None and stage.name not in only \
                    and stage.name not in force:
                ctx.record.stage_status[stage.name] = 'skipped'
                continue

            stage_dir = Path(ctx.stage_dir(stage.name))
            key_file = stage_dir / '_key'
            if (stage.name not in force and key_file.exists()
                    and key_file.read_text().strip() == key):
                ctx.record.stage_status[stage.name] = 'cached'
                continue

            ctx.record.stage_status[stage.name] = 'running'
            registry._flush(ctx.record)
            t0 = time.time()
            try:
                stage.fn(ctx)
                #_meta.json records both the terminal key AND its three
                #inputs (fn / config / upstream) so /api/run_diff can
                #attribute a cache miss to which input changed.
                (stage_dir / '_meta.json').write_text(json.dumps(
                    {'name': stage.name, 'key': key,
                     'started': t0, 'ended': time.time(),
                     'components': components}, indent=2, default=str))
                #Mark done and persist BEFORE writing `_key`, so a crash
                #between the two leaves the stage looking incomplete
                #(re-runnable) rather than cached-but-`running`.
                ctx.record.stage_status[stage.name] = 'done'
                registry._flush(ctx.record)
                key_file.write_text(key)
            except Exception:
                ctx.record.stage_status[stage.name] = 'failed'
                ctx.record.errors[stage.name] = traceback.format_exc()
                registry._flush(ctx.record)
                raise
            registry._flush(ctx.record)

        ctx.record.ended = _now()
        registry._flush(ctx.record)
        return ctx.record


# ---------------------------------------------------------------------------
# Chains
# ---------------------------------------------------------------------------

def _run_fingerprint(variant: Variant, upstream: dict) -> str:
    """Per-run fingerprint that folds in upstream run fingerprints.

    Equals ``variant.fingerprint`` when there's no upstream, so an
    un-chained pipeline's run-dir name matches the fingerprint that
    ``dm describe`` prints — useful for hand navigation. With upstream,
    it hashes the (variant, upstream-fps) tuple so distinct upstream
    chains produce distinct downstream dirs.
    """
    if not upstream:
        return variant.fingerprint
    return fingerprint({
        'variant': variant.fingerprint,
        'upstream': {name: rec.fingerprint for name, rec in upstream.items()},
    })


@dataclass
class ChainStep:
    """A node in a Chain: a pipeline plus the names of earlier steps whose
    runs this step consumes as input. `consumes` may point to any prior
    step, not only the immediate predecessor, so chains are DAGs not just
    sequences (analysis can read both `recon` and `forward_sim`, etc.).
    """
    name: str
    pipeline: 'Pipeline'
    consumes: tuple = ()

    def __post_init__(self):
        self.consumes = tuple(self.consumes)


class Variation:
    """A named tuple of (chain step -> variant name) — picks one variant per
    step in the chain. `base=` inherits another variation's mapping; any
    keyword overrides replace individual entries. Two variations that
    happen to share a step's variant choice also share the corresponding
    run directory on disk (fingerprint caching).

    ``forks_of`` declares that this variation is the renamed descendant
    of a variation in the parent chain. /api/chain_diff uses it to line
    up cross-fork comparisons when variation names changed, mirroring
    `forks_of` on `Variant`.
    """

    __slots__ = ('chain', 'name', 'base', 'forks_of', 'overrides')

    def __init__(self, chain: 'Chain', name: str, *,
                 base: Optional[str] = None,
                 forks_of: Optional[str] = None,
                 **mapping):
        self.chain = chain
        self.name = name
        self.base = base
        self.forks_of = forks_of
        self.overrides = mapping

    def resolve(self) -> dict:
        if self.base is None:
            return dict(self.overrides)
        if self.base not in self.chain.variations:
            raise KeyError(
                f'variation base {self.base!r} not found in '
                f'chain {self.chain.name!r}')
        out = self.chain.variations[self.base].resolve()
        out.update(self.overrides)
        return out

    def run(self, run_registry: 'RunRegistry') -> dict:
        return self.chain._run(self, run_registry)


class Chain:
    """A declarative DAG of Pipelines linked by upstream/downstream edges.

    Chains observe, they don't execute: ``Chain.variations[name].run(rr)``
    iterates the steps in order, looks up each step's variant from the
    variation's mapping, and calls the existing ``Pipeline.run`` with the
    upstream `RunRecord`s threaded through. The pipeline's per-stage
    cache plus upstream-aware run fingerprints make re-invocation
    idempotent — re-running a variation after editing only the analysis
    code re-executes only those stages whose function source changed.

    ``parent`` declares this chain as a fork of another chain by name —
    the UI groups child chains under their parent and offers a source
    diff of the two chain ``.py`` files, mirroring the pipeline fork
    forest.
    """

    def __init__(self, name: str, steps: list[ChainStep], *,
                 parent: Optional[str] = None):
        self.name = name
        self.steps = list(steps)
        self.parent = parent
        self.variations: dict[str, Variation] = {}
        self._source_file: Optional[str] = None
        self._validate()

    def _validate(self):
        seen: set[str] = set()
        for s in self.steps:
            if s.name in seen:
                raise ValueError(f'duplicate chain step {s.name!r}')
            for u in s.consumes:
                if u not in seen:
                    raise ValueError(
                        f'chain step {s.name!r} consumes {u!r} '
                        f'which is not an earlier step')
            seen.add(s.name)

    def variation(self, name: str, *, base: Optional[str] = None,
                  forks_of: Optional[str] = None,
                  **mapping) -> Variation:
        if name in self.variations:
            raise ValueError(
                f'variation {name!r} already defined in chain {self.name!r}')
        v = Variation(self, name, base=base, forks_of=forks_of, **mapping)
        self.variations[name] = v
        return v

    def _run(self, variation: Variation, rr: 'RunRegistry') -> dict:
        mapping = variation.resolve()
        if self._source_file:
            _try_snapshot(rr.root, self._source_file,
                          f'chain {self.name}/{variation.name}')
        runs: dict[str, RunRecord] = {}
        for step in self.steps:
            if step.name not in mapping:
                raise KeyError(
                    f'variation {variation.name!r} does not specify '
                    f'a variant for step {step.name!r}')
            module = step.pipeline._module
            if module is None:
                raise RuntimeError(
                    f'pipeline {step.pipeline.name!r} has no module '
                    f'attribution; cannot resolve variant '
                    f'{mapping[step.name]!r} from the registry')
            runs[step.name] = step.pipeline.run(
                registry.get(module, mapping[step.name]), rr,
                upstream={u: runs[u] for u in step.consumes},
                chain=self.name, variation=variation.name)
        return runs


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
    #Chain provenance. `chain`/`variation` name the chain context this run
    #was executed under (if any). `upstream` maps consumed step name ->
    #upstream run's fingerprint, the same value used to derive this run's
    #own fingerprint, so chain progress is reconstructible from run.json
    #alone without a separate state file.
    chain: Optional[str] = None
    variation: Optional[str] = None
    upstream: dict = field(default_factory=dict)


class RunContext:
    def __init__(self, fdir: str, variant: Variant, record: RunRecord,
                 upstream: Optional[dict] = None):
        self.fdir = fdir
        self.variant = variant
        self.record = record
        #Mapping {chain_step_name: RunRecord} so stages can read upstream
        #artifacts during chain execution. Empty for un-chained runs.
        self.upstream: dict = dict(upstream or {})

    def upstream_artifact(self, step_name: str, relpath: str = '') -> str:
        """Path to an upstream run's directory (or an artifact within it).

        Example: ``ctx.upstream_artifact('forward_sim',
        'stages/sim/outputs/data.npy')``.
        """
        rec = self.upstream[step_name]
        return os.path.join(rec.fdir, relpath) if relpath else rec.fdir

    def stage_dir(self, stage_name: str) -> str:
        d = os.path.join(self.fdir, 'stages', stage_name)
        os.makedirs(os.path.join(d, 'outputs'), exist_ok=True)
        return d

    def artifact(self, stage_name: str, relpath: str, source: str) -> str:
        """Register an existing file or directory as an artifact.

        The pipeline writes its output wherever convenient (often a path
        chosen by an external tool); this method links it into
        ``stages/<stage>/outputs/<relpath>`` so diffman can serve it.
        Symlinks when supported, falls back to copy / copytree otherwise.
        Returns the destination path.
        """
        src = os.path.abspath(source)
        if not os.path.exists(src):
            raise FileNotFoundError(
                f'artifact source does not exist: {source}')
        dest = os.path.join(self.stage_dir(stage_name), 'outputs', relpath)
        os.makedirs(os.path.dirname(dest) or '.', exist_ok=True)
        if os.path.islink(dest) or os.path.isfile(dest):
            os.unlink(dest)
        elif os.path.isdir(dest):
            shutil.rmtree(dest)
        try:
            os.symlink(src, dest)
        except OSError:
            #Symlinks unsupported (Windows without privilege, exotic FS);
            #fall back to a real copy.
            (shutil.copytree if os.path.isdir(src) else shutil.copy2)(src, dest)
        return dest

    def metric(self, stage_name: str, name: str, value) -> None:
        """Record a scalar metric under ``stages/<stage>/metrics.json``.

        Repeat calls with the same `name` overwrite. The scoreboard
        endpoint aggregates these across a chain's variations into one
        ablation-sweep table (avg_flux, frc_score, etc.). Pre-existing
        non-dict / malformed JSON at the path is reset rather than raised
        so a corrupt write from a prior crash doesn't poison further
        metric() calls.
        """
        path = Path(self.stage_dir(stage_name), 'metrics.json')
        data: dict = {}
        if path.exists():
            try:
                loaded = json.loads(path.read_text())
                if isinstance(loaded, dict):
                    data = loaded
            except json.JSONDecodeError:
                pass
        data[name] = value
        path.write_text(json.dumps(data, indent=2, default=str))


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

    def open_run(self, pipeline: str, variant: Variant, *,
                 upstream: Optional[dict] = None,
                 chain: Optional[str] = None,
                 variation: Optional[str] = None) -> RunContext:
        upstream = upstream or {}
        run_fp = _run_fingerprint(variant, upstream)
        fdir = os.path.join(self.root, pipeline, variant.name, run_fp[:12])
        os.makedirs(fdir, exist_ok=True)
        record = RunRecord(
            pipeline=pipeline, variant=variant.name,
            fingerprint=run_fp, fdir=fdir, started=_now(),
            git_rev=_git_rev(), host=socket.gethostname(),
            chain=chain, variation=variation,
            upstream={n: r.fingerprint for n, r in upstream.items()},
        )
        self._flush(record)
        Path(fdir, 'config.json').write_text(
            json.dumps(variant.config, indent=2, default=str))
        return RunContext(fdir, variant, record, upstream=upstream)

    def _flush(self, record: RunRecord) -> None:
        Path(record.fdir, 'run.json').write_text(
            json.dumps(asdict(record), indent=2, default=str))
        self._cache = None

    def _load_all(self) -> list[RunRecord]:
        out: list[RunRecord] = []
        root = Path(self.root)
        if not root.exists():
            return out
        known = set(RunRecord.__dataclass_fields__)
        for run_json in root.glob('*/*/*/run.json'):
            try:
                raw = json.loads(run_json.read_text())
                out.append(RunRecord(**{k: v for k, v in raw.items()
                                        if k in known}))
            except (json.JSONDecodeError, TypeError) as e:
                _log.warning('skipping %s: %s', run_json, e)
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
    """Current git HEAD, or None if not in a repo / git not installed.

    Catches only the failure modes we expect from `git rev-parse`:
      - FileNotFoundError: no `git` on PATH
      - CalledProcessError: not a repo, or repo has no commits yet
      - TimeoutExpired: pathological I/O latency
    Any other exception (e.g. OSError from a broken process state) is
    a real bug and propagates.
    """
    try:
        out = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL, timeout=2)
        return out.decode().strip()
    except (FileNotFoundError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired):
        return None
