"""Tests for the Chain / ChainStep / Variation layer."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import diffman as dm
from diffman import discovery


# ---------------------------------------------------------------------------
# Static validation
# ---------------------------------------------------------------------------

def test_chain_validates_consumes_topo_order(scan_root):
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    q = dm.Pipeline('p2', [dm.Stage('s', lambda c: {})], module='m2')
    with pytest.raises(ValueError, match='not an earlier step'):
        dm.Chain('bad', steps=[
            dm.ChainStep('downstream', q, consumes=('upstream',)),
            dm.ChainStep('upstream',   p),
        ])


def test_chain_rejects_duplicate_step_names(scan_root):
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    with pytest.raises(ValueError, match='duplicate chain step'):
        dm.Chain('bad', steps=[
            dm.ChainStep('x', p),
            dm.ChainStep('x', p),
        ])


def test_variation_resolve_inherits_from_base(scan_root):
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    chain = dm.Chain('c', steps=[dm.ChainStep('x', p)])
    chain.variation('a', x='base')
    chain.variation('b', base='a')                       #inherits unchanged
    chain.variation('c', base='a', x='other')            #overrides
    assert chain.variations['a'].resolve() == {'x': 'base'}
    assert chain.variations['b'].resolve() == {'x': 'base'}
    assert chain.variations['c'].resolve() == {'x': 'other'}


def test_variation_duplicate_name_raises(scan_root):
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    chain = dm.Chain('c', steps=[dm.ChainStep('x', p)])
    chain.variation('a', x='base')
    with pytest.raises(ValueError, match='already defined'):
        chain.variation('a', x='base')


def test_variation_resolve_missing_base_raises(scan_root):
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    chain = dm.Chain('c', steps=[dm.ChainStep('x', p)])
    chain.variation('a', base='ghost', x='base')
    with pytest.raises(KeyError, match='ghost'):
        chain.variations['a'].resolve()


def test_chain_step_consumes_normalized_to_tuple(scan_root):
    """ChainStep.__post_init__ must coerce a list to a tuple so the
    validation that compares membership in `seen` (a set of strings)
    works uniformly regardless of how the user wrote `consumes=`."""
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    q = dm.Pipeline('p2', [dm.Stage('s', lambda c: {})], module='m2')
    step = dm.ChainStep('downstream', q, consumes=['upstream'])
    assert step.consumes == ('upstream',)
    #Round-trip through a chain — should not crash.
    dm.Chain('c', steps=[
        dm.ChainStep('upstream', p),
        step,
    ])


def test_chain_step_validates_consumes_against_nonexistent_step(scan_root):
    """Consumes that points at a step that doesn't exist anywhere in
    the chain is a typo — must be caught with the same error path as
    out-of-order consumes (uniform error UX)."""
    p = dm.Pipeline('p1', [dm.Stage('s', lambda c: {})], module='m1')
    with pytest.raises(ValueError, match='not an earlier step'):
        dm.Chain('c', steps=[
            dm.ChainStep('only', p, consumes=('ghost',)),
        ])


def test_variation_run_with_missing_step_in_mapping_raises(tmp_path,
                                                            monkeypatch):
    """A variation that doesn't specify every chain step must fail loudly
    on .run() — silently skipping would leave the on-disk record looking
    incomplete in a way that's hard to debug."""
    monkeypatch.syspath_prepend(str(tmp_path))
    dm.registry._variants.clear()
    dm.registry.register('base', module='m', x=1)

    def _f(ctx): return {}
    a = dm.Pipeline('a', [dm.Stage('s', _f)], module='m')
    b = dm.Pipeline('b', [dm.Stage('s', _f)], module='m')
    chain = dm.Chain('c', steps=[
        dm.ChainStep('first',  a),
        dm.ChainStep('second', b, consumes=('first',)),
    ])
    chain.variation('partial', first='base')   #no `second=`
    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    with pytest.raises(KeyError, match="step 'second'"):
        chain.variations['partial'].run(rr)


def test_chain_with_module_less_pipeline_raises_on_run(tmp_path):
    """If a Pipeline has no _module attribution (e.g. constructed
    dynamically without a captureable frame), the chain can't resolve
    variant names against the registry. Surface this with a clear
    RuntimeError rather than a confusing KeyError from registry.get()."""
    p = dm.Pipeline('p', [dm.Stage('s', lambda c: {})])
    p._module = None   #simulate the no-attribution case
    chain = dm.Chain('c', steps=[dm.ChainStep('only', p)])
    chain.variation('v', only='whatever')
    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    with pytest.raises(RuntimeError, match='no module attribution'):
        chain.variations['v'].run(rr)


def test_pipeline_captures_caller_module_by_default():
    """Pipeline() without an explicit module= must capture the caller's
    __name__ via inspect. The Chain layer depends on this for variant
    resolution."""
    p = dm.Pipeline('autop', [dm.Stage('s', lambda c: {})])
    assert p._module == __name__   #i.e. 'test_chains'


# ---------------------------------------------------------------------------
# End-to-end run via a chain
# ---------------------------------------------------------------------------

def _build_two_pipeline_chain(tmp_path, monkeypatch):
    """Two pipelines + a chain linking them, returning (chain, rr).

    Upstream writes a small file under its sim stage; downstream copies
    that file's bytes through a stage of its own so we can verify
    upstream artifact access from inside a stage function.
    """
    monkeypatch.syspath_prepend(str(tmp_path))

    dm.registry._variants.clear()

    #Upstream pipeline + variants
    dm.registry.register('base',     module='upstream', n=1)
    dm.registry.register('high',     module='upstream', n=42)

    def _up(ctx):
        out = os.path.join(tmp_path, f'up_{ctx.variant.name}.txt')
        Path(out).write_text(str(ctx.variant.config['n']))
        ctx.artifact('sim', 'value.txt', out)
        return {}

    up_pipe = dm.Pipeline('up_sim',
                          [dm.Stage('sim', _up, config_keys=('n',))],
                          module='upstream')

    #Downstream pipeline + variants. The downstream stage reads the
    #upstream artifact via ctx.upstream_artifact() to prove the wiring.
    dm.registry.register('default', module='downstream', mult=2)

    def _down(ctx):
        src = ctx.upstream_artifact('forward', 'stages/sim/outputs/value.txt')
        n = int(Path(src).read_text())
        out = os.path.join(tmp_path, f'down_{ctx.variant.name}.txt')
        Path(out).write_text(str(n * ctx.variant.config['mult']))
        ctx.artifact('proc', 'doubled.txt', out)
        return {}

    down_pipe = dm.Pipeline('down_proc',
                            [dm.Stage('proc', _down, config_keys=('mult',))],
                            module='downstream')

    chain = dm.Chain('mychain', steps=[
        dm.ChainStep('forward', up_pipe),
        dm.ChainStep('process', down_pipe, consumes=('forward',)),
    ])
    chain.variation('baseline', forward='base', process='default')
    chain.variation('strong',   base='baseline', forward='high')

    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    return chain, rr


def test_chain_run_threads_upstream_through_pipelines(tmp_path, monkeypatch):
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    runs = chain.variations['baseline'].run(rr)
    assert set(runs) == {'forward', 'process'}
    #Downstream record has upstream pointing at upstream's run fp.
    proc = runs['process']
    fwd  = runs['forward']
    assert proc.upstream == {'forward': fwd.fingerprint}
    #Chain provenance is persisted to run.json.
    persisted = json.loads(Path(proc.fdir, 'run.json').read_text())
    assert persisted['chain'] == 'mychain'
    assert persisted['variation'] == 'baseline'
    assert persisted['upstream'] == {'forward': fwd.fingerprint}
    #Downstream stage actually read the upstream artifact (n=1, mult=2 -> 2).
    assert Path(proc.fdir, 'stages/proc/outputs/doubled.txt').read_text() == '2'


def test_changing_upstream_variant_produces_distinct_downstream_dirs(
        tmp_path, monkeypatch):
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    base = chain.variations['baseline'].run(rr)
    strong = chain.variations['strong'].run(rr)
    #Downstream variant is identical, but upstream changed → different fp
    #→ different run directories.
    assert base['process'].variant == strong['process'].variant
    assert base['process'].fingerprint != strong['process'].fingerprint
    assert base['process'].fdir != strong['process'].fdir
    #Numerical proof the downstream actually used the new upstream value.
    assert Path(strong['process'].fdir,
                'stages/proc/outputs/doubled.txt').read_text() == '84'


def test_shared_upstream_variant_reuses_run_dir_across_variations(
        tmp_path, monkeypatch):
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    #Two variations that pick the same upstream variant should share the
    #same forward run directory (one shared run, per the design decision).
    chain.variation('also_baseline', base='baseline')   #identical mapping
    a = chain.variations['baseline'].run(rr)
    b = chain.variations['also_baseline'].run(rr)
    assert a['forward'].fdir == b['forward'].fdir
    assert a['forward'].fingerprint == b['forward'].fingerprint


def test_re_running_same_variation_is_a_noop_via_stage_cache(
        tmp_path, monkeypatch):
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    chain.variations['baseline'].run(rr)
    runs2 = chain.variations['baseline'].run(rr)
    #Re-run lands in the same fdir, and every stage reports 'cached'.
    assert runs2['forward'].stage_status == {'sim': 'cached'}
    assert runs2['process'].stage_status == {'proc': 'cached'}


def test_unchained_pipeline_run_still_uses_variant_fingerprint(
        tmp_path, monkeypatch):
    """Backward compat: calling Pipeline.run with no chain/upstream
    produces a run directory keyed by the variant's intrinsic fp."""
    monkeypatch.syspath_prepend(str(tmp_path))
    dm.registry._variants.clear()
    dm.registry.register('base', module='m', x=1)
    def _f(ctx): return {}
    p = dm.Pipeline('solo', [dm.Stage('s', _f)], module='m')
    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    rec = p.run(dm.registry.get('m', 'base'), rr)
    variant = dm.registry.get('m', 'base')
    assert rec.fingerprint == variant.fingerprint
    assert rec.fdir.endswith(variant.short_fp)
    assert rec.chain is None and rec.variation is None and rec.upstream == {}


def test_run_fingerprint_stable_for_same_upstream(tmp_path, monkeypatch):
    """_run_fingerprint must produce the same value for the same
    (variant, upstream) pair across two calls — otherwise the run
    directory would jitter and caching would be broken."""
    from diffman.core import _run_fingerprint
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    runs1 = chain.variations['baseline'].run(rr)
    proc1_fp = runs1['process'].fingerprint
    proc_variant = dm.registry.get('downstream', 'default')
    recomputed = _run_fingerprint(proc_variant,
                                  {'forward': runs1['forward']})
    assert proc1_fp == recomputed


def test_run_fingerprint_no_upstream_equals_variant_fingerprint(
        tmp_path, monkeypatch):
    from diffman.core import _run_fingerprint
    monkeypatch.syspath_prepend(str(tmp_path))
    dm.registry._variants.clear()
    v = dm.registry.register('only', module='m', x=1)
    assert _run_fingerprint(v, {}) == v.fingerprint


def test_chain_provenance_survives_fresh_registry(tmp_path, monkeypatch):
    """RunRegistry reads run.json from disk on first .list_runs(); a
    fresh registry instance must surface the same chain/variation/
    upstream fields that the chain.run() persisted."""
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    chain.variations['baseline'].run(rr)
    fresh = dm.RunRegistry(root=str(tmp_path / 'runs'))
    proc_records = [r for r in fresh.list_runs() if r.pipeline == 'down_proc']
    assert len(proc_records) == 1
    rec = proc_records[0]
    assert rec.chain == 'mychain'
    assert rec.variation == 'baseline'
    fwd_records = [r for r in fresh.list_runs() if r.pipeline == 'up_sim']
    assert rec.upstream == {'forward': fwd_records[0].fingerprint}


def test_chain_run_snapshots_source_file(tmp_path, monkeypatch):
    """When a chain has _source_file set, Chain._run must hand it to
    git_backup.snapshot. Catching the call is enough — we don't need
    a real git repo, just proof the integration is wired."""
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    chain._source_file = str(tmp_path / 'fake_chain.py')
    Path(chain._source_file).write_text('# fake')
    calls = []
    import diffman.git_backup as gb
    monkeypatch.setattr(gb, 'snapshot',
                        lambda root, path, msg: calls.append((root, path, msg)))
    chain.variations['baseline'].run(rr)
    chain_calls = [c for c in calls if 'chain mychain/baseline' in c[2]]
    assert len(chain_calls) == 1
    assert chain_calls[0][1] == chain._source_file


def test_pipeline_run_snapshot_uses_run_fp_not_variant_fp(tmp_path,
                                                           monkeypatch):
    """The git snapshot message should embed the actual run fingerprint
    (which folds in upstream) — not the variant's intrinsic fp — so
    the snapshot history aligns with the run directory it describes."""
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    #Give the pipelines source files so the snapshot path fires.
    chain.steps[1].pipeline._source_file = str(tmp_path / 'down.py')
    Path(chain.steps[1].pipeline._source_file).write_text('# fake')
    calls = []
    import diffman.git_backup as gb
    monkeypatch.setattr(gb, 'snapshot',
                        lambda root, path, msg: calls.append(msg))
    runs = chain.variations['baseline'].run(rr)
    proc_short = runs['process'].fingerprint[:12]
    proc_variant_short = dm.registry.get('downstream', 'default').short_fp
    proc_snap = [m for m in calls if 'down_proc' in m]
    assert len(proc_snap) == 1
    assert proc_short in proc_snap[0]
    #When upstream changes the run fp, it diverges from the variant fp.
    assert proc_short != proc_variant_short


# ---------------------------------------------------------------------------
# Fan-out: a chain variation can bind a step to a list of variants
# ---------------------------------------------------------------------------

def _build_fanout_chain(tmp_path, monkeypatch):
    """Same two-pipeline shape as _build_two_pipeline_chain, but with
    three downstream variants so the variation can fan out the `process`
    step into [a, b, c]."""
    monkeypatch.syspath_prepend(str(tmp_path))
    dm.registry._variants.clear()

    dm.registry.register('base', module='upstream', n=1)

    def _up(ctx):
        out = os.path.join(tmp_path, f'up_{ctx.variant.name}.txt')
        Path(out).write_text(str(ctx.variant.config['n']))
        ctx.artifact('sim', 'value.txt', out)
        return {}

    up_pipe = dm.Pipeline('up_sim',
                          [dm.Stage('sim', _up, config_keys=('n',))],
                          module='upstream')

    for name, mult in [('a', 2), ('b', 3), ('c', 5)]:
        dm.registry.register(name, module='downstream', mult=mult)

    def _down(ctx):
        src = ctx.upstream_artifact('forward', 'stages/sim/outputs/value.txt')
        n = int(Path(src).read_text())
        out = os.path.join(tmp_path, f'down_{ctx.variant.name}.txt')
        Path(out).write_text(str(n * ctx.variant.config['mult']))
        ctx.artifact('proc', 'doubled.txt', out)
        return {}

    down_pipe = dm.Pipeline('down_proc',
                            [dm.Stage('proc', _down, config_keys=('mult',))],
                            module='downstream')

    chain = dm.Chain('mychain', steps=[
        dm.ChainStep('forward', up_pipe),
        dm.ChainStep('process', down_pipe, consumes=('forward',)),
    ])
    chain.variation('sweep', forward='base', process=['a', 'b', 'c'])

    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    return chain, rr


def test_chain_fanout_runs_one_branch_per_variant(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    runs = chain.variations['sweep'].run(rr)
    #Nested shape: the fan-out step exposes branch keys (= variant names).
    assert set(runs['process'].keys()) == {'a', 'b', 'c'}
    #Forward step is single-variant — still wrapped so the shape is uniform.
    assert set(runs['forward'].keys()) == {None}
    #Each branch ran with its own variant and produced its own output file.
    for variant in ('a', 'b', 'c'):
        rec = runs['process'][variant]
        assert rec.variant == variant
        expected = int(Path(runs['forward'][None].fdir,
                            'stages/sim/outputs/value.txt').read_text()) \
            * dm.registry.get('downstream', variant).config['mult']
        assert Path(rec.fdir,
                    'stages/proc/outputs/doubled.txt').read_text() == str(expected)


def test_chain_fanout_branches_have_distinct_run_dirs(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    runs = chain.variations['sweep'].run(rr)
    fps = {bk: rec.fingerprint for bk, rec in runs['process'].items()}
    assert len(set(fps.values())) == 3   #all three branches distinct
    fdirs = {bk: rec.fdir for bk, rec in runs['process'].items()}
    assert len(set(fdirs.values())) == 3


def test_chain_fanout_step_branch_targets_one_branch(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    #Prime the cache with a full run, then re-target one branch and
    #confirm only that branch's RunRecord comes back.
    chain.variations['sweep'].run(rr)
    runs = chain.variations['sweep'].run(rr, step='process:b')
    assert set(runs['process'].keys()) == {'b'}
    #The unrelated branches are absent from this targeted result, even
    #though they exist on disk from the priming run.
    assert 'a' not in runs['process']


def test_chain_fanout_missing_branch_raises(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    chain.variations['sweep'].run(rr)   #prime upstream caches
    with pytest.raises(KeyError, match='branch'):
        chain.variations['sweep'].run(rr, step='process:nonexistent')


def test_chain_fanout_rejects_empty_variant_list(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    chain.variation('empty', forward='base', process=[])
    with pytest.raises(ValueError, match='empty variant list'):
        chain.variations['empty'].run(rr)


def test_chain_fanout_rejects_duplicates(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    chain.variation('dup', forward='base', process=['a', 'a', 'b'])
    with pytest.raises(ValueError, match='duplicate'):
        chain.variations['dup'].run(rr)


def test_chain_fanout_rejects_non_str_non_list_spec(tmp_path, monkeypatch):
    chain, rr = _build_fanout_chain(tmp_path, monkeypatch)
    chain.variation('bad', forward='base', process=42)
    with pytest.raises(TypeError, match='must be str or list'):
        chain.variations['bad'].run(rr)


def test_chain_fanout_inherited_by_downstream(tmp_path, monkeypatch):
    """A downstream step that consumes a fan-out step should inherit
    its branch keys — one downstream run per upstream branch, even
    though the downstream itself binds a single variant."""
    monkeypatch.syspath_prepend(str(tmp_path))
    dm.registry._variants.clear()

    dm.registry.register('base', module='m_up', n=1)
    for name in ('a', 'b'):
        dm.registry.register(name, module='m_mid', tag=name)
    dm.registry.register('only', module='m_down', kind='sum')

    def _f(ctx):
        ctx.artifact('s', 'out.txt', _write(tmp_path, ctx, 'up'))
        return {}

    def _g(ctx):
        ctx.artifact('s', 'out.txt', _write(tmp_path, ctx, 'mid'))
        return {}

    def _h(ctx):
        ctx.artifact('s', 'out.txt', _write(tmp_path, ctx, 'down'))
        return {}

    up = dm.Pipeline('p_up',   [dm.Stage('s', _f)], module='m_up')
    mid = dm.Pipeline('p_mid', [dm.Stage('s', _g)], module='m_mid')
    down = dm.Pipeline('p_down', [dm.Stage('s', _h)], module='m_down')

    chain = dm.Chain('three', steps=[
        dm.ChainStep('u', up),
        dm.ChainStep('m', mid, consumes=('u',)),
        dm.ChainStep('d', down, consumes=('m',)),
    ])
    chain.variation('v', u='base', m=['a', 'b'], d='only')

    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    runs = chain.variations['v'].run(rr)
    assert set(runs['m'].keys()) == {'a', 'b'}
    assert set(runs['d'].keys()) == {'a', 'b'}    #inherited from m
    #Both downstream branches share the same variant name but distinct
    #fingerprints (upstream differs) → distinct run directories.
    assert runs['d']['a'].variant == runs['d']['b'].variant == 'only'
    assert runs['d']['a'].fingerprint != runs['d']['b'].fingerprint


def _write(tmp_path, ctx, tag):
    p = os.path.join(tmp_path, f'{tag}_{ctx.variant.name}.txt')
    Path(p).write_text(tag)
    return p


def test_chain_fanout_double_fanout_collision_rejected(tmp_path, monkeypatch):
    """A step that fans out AND inherits branch keys from an upstream
    fan-out is ambiguous — cartesian product is rarely intended."""
    monkeypatch.syspath_prepend(str(tmp_path))
    dm.registry._variants.clear()
    dm.registry.register('base', module='m_up')
    for name in ('a', 'b'):
        dm.registry.register(name, module='m_mid')
    for name in ('x', 'y'):
        dm.registry.register(name, module='m_down')

    def _noop(ctx):
        return {}

    chain = dm.Chain('c', steps=[
        dm.ChainStep('u', dm.Pipeline('p_u', [dm.Stage('s', _noop)], module='m_up')),
        dm.ChainStep('m', dm.Pipeline('p_m', [dm.Stage('s', _noop)], module='m_mid'),
                     consumes=('u',)),
        dm.ChainStep('d', dm.Pipeline('p_d', [dm.Stage('s', _noop)], module='m_down'),
                     consumes=('m',)),
    ])
    chain.variation('v', u='base', m=['a', 'b'], d=['x', 'y'])
    rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
    with pytest.raises(ValueError, match='cannot fan out'):
        chain.variations['v'].run(rr)


def test_chain_fanout_legacy_shape_preserved_when_no_fanout(tmp_path,
                                                            monkeypatch):
    """Variations that don't fan out must still get `{step: RunRecord}`
    back (not the nested branch dict) — the contract existing callers
    depend on."""
    chain, rr = _build_two_pipeline_chain(tmp_path, monkeypatch)
    runs = chain.variations['baseline'].run(rr)
    #The legacy shape: values are RunRecord, not dict.
    for v in runs.values():
        assert isinstance(v, dm.RunRecord)
