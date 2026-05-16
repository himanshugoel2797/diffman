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
