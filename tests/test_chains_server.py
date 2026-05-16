"""Tests for chain-aware server endpoints + discovery + metrics."""

from __future__ import annotations

import json
import os
import sys
import textwrap
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import diffman as dm
from diffman import discovery
from diffman.server import (
    create_app, _build_chain_forest, _list_chains_in_module,
)


# ---------------------------------------------------------------------------
# Fixtures: a two-pipeline chain plus a forked variant
# ---------------------------------------------------------------------------

PIPELINE_FORWARD = """
    import diffman as dm
    dm.register('base',     n=1)
    dm.register('jitter',   n=2)

    def _f(ctx):
        out = ctx.artifact('sim', 'val.txt', _write_tmp(ctx))
        ctx.metric('sim', 'flux', ctx.variant.config['n'] * 10)
        return {}

    def _write_tmp(ctx):
        import os, tempfile
        p = os.path.join(tempfile.mkdtemp(), 'val.txt')
        open(p, 'w').write(str(ctx.variant.config['n']))
        return p

    PIPELINE = dm.Pipeline('forward', [dm.Stage('sim', _f, config_keys=('n',))])
"""

PIPELINE_RECON = """
    import diffman as dm
    dm.register('ePIE', algo='epie', iters=100)
    dm.register('DM',   algo='dm',   iters=50)

    def _f(ctx):
        src = ctx.upstream_artifact('forward', 'stages/sim/outputs/val.txt')
        n = int(open(src).read())
        out_path = _write_tmp(n * ctx.variant.config['iters'])
        ctx.artifact('recon', 'result.txt', out_path)
        ctx.metric('recon', 'iters_done', ctx.variant.config['iters'])
        return {}

    def _write_tmp(v):
        import os, tempfile
        p = os.path.join(tempfile.mkdtemp(), 'r.txt')
        open(p, 'w').write(str(v))
        return p

    PIPELINE = dm.Pipeline('recon', [dm.Stage('recon', _f, config_keys=('algo','iters'))])
"""

CHAIN_PARENT = """
    import diffman as dm
    import _diffman_chain_fwd as forward_mod
    import _diffman_chain_recon as recon_mod

    CHAIN = dm.Chain('mychain', steps=[
        dm.ChainStep('forward', forward_mod.PIPELINE),
        dm.ChainStep('recon',   recon_mod.PIPELINE, consumes=('forward',)),
    ])
    CHAIN.variation('baseline', forward='base',   recon='ePIE')
    CHAIN.variation('jittered', base='baseline', forward='jitter')
    CHAIN.variation('algo_dm',  base='baseline', recon='DM')
"""

CHAIN_CHILD = """
    import diffman as dm
    import _diffman_chain_fwd as forward_mod
    import _diffman_chain_recon as recon_mod

    CHAIN = dm.Chain('mychain_v2', parent='mychain', steps=[
        dm.ChainStep('forward', forward_mod.PIPELINE),
        dm.ChainStep('recon',   recon_mod.PIPELINE, consumes=('forward',)),
    ])
    CHAIN.variation('baseline', forward='base', recon='ePIE')
"""


@pytest.fixture
def chain_client(scan_root, make_pipeline):
    """Server + client with two pipeline modules and two chain modules."""
    make_pipeline('_diffman_chain_fwd',   PIPELINE_FORWARD)
    make_pipeline('_diffman_chain_recon', PIPELINE_RECON)
    make_pipeline('_diffman_chain_parent', CHAIN_PARENT)
    make_pipeline('_diffman_chain_child',  CHAIN_CHILD)
    app = create_app(root=str(scan_root / 'runs'),
                     scan_root=str(scan_root), no_scan=False)
    with TestClient(app) as c:
        yield c, scan_root


# ---------------------------------------------------------------------------
# Discovery picks up chain modules
# ---------------------------------------------------------------------------

class TestChainDiscovery:
    def test_discover_finds_chain_only_modules(self, scan_root):
        #A file that mentions diffman + CHAIN but no PIPELINE must still
        #be discovered (chain-only modules are valid).
        path = os.path.join(scan_root, '_diffman_chain_only.py')
        Path(path).write_text(textwrap.dedent("""
            import diffman as dm
            # no PIPELINE here; just declare an empty placeholder chain
            CHAIN = dm.Chain('placeholder', steps=[])
        """).lstrip())
        modules = discovery.discover(str(scan_root))
        names = {m['module'] for m in modules}
        assert '_diffman_chain_only' in names

    def test_load_module_indexes_chain_by_name(self, chain_client):
        c, _ = chain_client
        #/api/chains triggers the import-all sweep that populates the
        #chain index — discovery alone is grep-based.
        c.get('/api/chains')
        assert discovery.CHAIN_TO_MODULE['mychain'] == '_diffman_chain_parent'
        assert discovery.CHAIN_TO_MODULE['mychain_v2'] == '_diffman_chain_child'

    def test_load_module_assigns_chain_source_file(self, chain_client):
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        assert mod.CHAIN._source_file is not None
        assert mod.CHAIN._source_file.endswith('_diffman_chain_parent.py')

    def test_evict_module_clears_chain_entry(self, chain_client):
        c, _ = chain_client
        c.get('/api/chains')
        assert 'mychain' in discovery.CHAIN_TO_MODULE
        discovery.evict_module('_diffman_chain_parent')
        assert 'mychain' not in discovery.CHAIN_TO_MODULE


# ---------------------------------------------------------------------------
# Forest construction
# ---------------------------------------------------------------------------

class TestChainForest:
    def test_forest_nests_child_under_parent(self):
        forest = _build_chain_forest([
            {'name': 'a', 'parent': None, 'step_count': 1, 'variation_count': 0},
            {'name': 'b', 'parent': 'a',  'step_count': 1, 'variation_count': 0},
        ])
        assert len(forest) == 1
        assert forest[0]['name'] == 'a'
        assert forest[0]['children'][0]['name'] == 'b'

    def test_forest_flags_orphan_parent(self):
        forest = _build_chain_forest([
            {'name': 'lone', 'parent': 'ghost',
             'step_count': 1, 'variation_count': 0},
        ])
        assert forest[0]['orphan_parent'] == 'ghost'


# ---------------------------------------------------------------------------
# /api/chains and /api/chain/{name}
# ---------------------------------------------------------------------------

class TestChainsEndpoint:
    def test_chains_endpoint_returns_fork_forest(self, chain_client):
        c, _ = chain_client
        forest = c.get('/api/chains').json()['forest']
        roots = [n for n in forest if n['name'] == 'mychain']
        assert len(roots) == 1
        assert roots[0]['children'][0]['name'] == 'mychain_v2'

    def test_chain_detail_returns_steps_and_variations(self, chain_client):
        c, _ = chain_client
        d = c.get('/api/chain/mychain').json()
        assert d['name'] == 'mychain'
        assert [s['name'] for s in d['steps']] == ['forward', 'recon']
        assert d['steps'][1]['consumes'] == ['forward']
        var_names = sorted(v['name'] for v in d['variations'])
        assert var_names == ['algo_dm', 'baseline', 'jittered']
        baseline = next(v for v in d['variations'] if v['name'] == 'baseline')
        assert baseline['mapping'] == {'forward': 'base', 'recon': 'ePIE'}

    def test_chain_detail_404_for_unknown(self, chain_client):
        c, _ = chain_client
        r = c.get('/api/chain/no_such_chain')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# /api/chain_progress
# ---------------------------------------------------------------------------

class TestChainProgress:
    def test_progress_pending_before_any_runs(self, chain_client):
        c, _ = chain_client
        d = c.get('/api/chain_progress/mychain/baseline').json()
        statuses = [s['status'] for s in d['steps']]
        assert statuses == ['pending', 'pending']

    def test_progress_reports_done_after_run(self, chain_client, scan_root):
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        mod.CHAIN.variations['baseline'].run(rr)
        d = c.get('/api/chain_progress/mychain/baseline').json()
        statuses = {s['name']: s['status'] for s in d['steps']}
        assert statuses == {'forward': 'done', 'recon': 'done'}
        #Run records' fingerprints are surfaced for the UI to link to.
        for s in d['steps']:
            assert s['short_fp'] is not None and s['fingerprint'] is not None

    def test_progress_reports_cached_on_re_run(self, chain_client, scan_root):
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        mod.CHAIN.variations['baseline'].run(rr)
        mod.CHAIN.variations['baseline'].run(rr)
        d = c.get('/api/chain_progress/mychain/baseline').json()
        statuses = {s['name']: s['status'] for s in d['steps']}
        assert statuses == {'forward': 'cached', 'recon': 'cached'}

    def test_progress_distinguishes_variations_by_upstream(
            self, chain_client, scan_root):
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        mod.CHAIN.variations['baseline'].run(rr)
        #Run jittered: same recon variant but different upstream — the
        #progress endpoint must NOT report 'done' for jittered.recon
        #just because baseline.recon ran with the same variant name.
        d = c.get('/api/chain_progress/mychain/jittered').json()
        statuses = {s['name']: s['status'] for s in d['steps']}
        assert statuses == {'forward': 'pending', 'recon': 'pending'}

    def test_progress_surfaces_failed_step_with_error(
            self, chain_client, scan_root):
        c, scan_root_dir = chain_client
        #Replace the recon pipeline body with one that raises.
        Path(os.path.join(scan_root_dir, '_diffman_chain_recon.py')).write_text(
            textwrap.dedent("""
                import diffman as dm
                dm.register('ePIE', algo='epie', iters=100)
                dm.register('DM',   algo='dm',   iters=50)
                def _f(ctx):
                    raise RuntimeError('recon kaboom')
                PIPELINE = dm.Pipeline('recon',
                    [dm.Stage('recon', _f, config_keys=('algo','iters'))])
            """).lstrip())
        discovery.evict_module('_diffman_chain_recon')
        discovery.evict_module('_diffman_chain_parent')
        discovery.discover(str(scan_root_dir))
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root_dir / 'runs'))
        with pytest.raises(RuntimeError):
            mod.CHAIN.variations['baseline'].run(rr)
        d = c.get('/api/chain_progress/mychain/baseline').json()
        recon_step = next(s for s in d['steps'] if s['name'] == 'recon')
        assert recon_step['status'] == 'failed'
        assert 'recon kaboom' in recon_step['errors']['recon']


# ---------------------------------------------------------------------------
# /api/chain_source_diff
# ---------------------------------------------------------------------------

class TestChainSourceDiff:
    def test_source_diff_returns_unified_diff(self, chain_client):
        c, _ = chain_client
        d = c.get('/api/chain_source_diff?chain=mychain_v2').json()
        assert d['parent'] == 'mychain'
        #The child differs from the parent (variations + name + parent=).
        assert 'mychain_v2' in d['diff']
        assert d['diff'].startswith('---') or '@@' in d['diff']

    def test_source_diff_returns_empty_for_root_chain(self, chain_client):
        c, _ = chain_client
        d = c.get('/api/chain_source_diff?chain=mychain').json()
        assert d['parent'] is None
        assert d['diff'] == ''


# ---------------------------------------------------------------------------
# /api/chain_variation_diff
# ---------------------------------------------------------------------------

class TestChainVariationDiff:
    def test_variation_diff_returns_per_step_rows(self, chain_client):
        c, _ = chain_client
        d = c.get(
            '/api/chain_variation_diff'
            '?chain=mychain&variations=baseline,jittered').json()
        assert d['variations'] == ['baseline', 'jittered']
        by_step = {s['step']: s for s in d['steps']}
        #Forward's `n` differs (base=1 vs jitter=2).
        fwd_rows = by_step['forward']['rows']
        n_row = next(r for r in fwd_rows if r['path'] == 'n')
        assert n_row['values'] == [1, 2]
        assert n_row['equal'] is False
        #Recon is identical across these two variations (both ePIE).
        recon_rows = by_step['recon']['rows']
        assert all(r['equal'] for r in recon_rows)

    def test_variation_diff_rejects_fewer_than_two(self, chain_client):
        c, _ = chain_client
        r = c.get('/api/chain_variation_diff'
                  '?chain=mychain&variations=baseline')
        assert r.status_code == 400

    def test_variation_diff_404_unknown_variation(self, chain_client):
        c, _ = chain_client
        r = c.get('/api/chain_variation_diff'
                  '?chain=mychain&variations=baseline,ghost')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# /api/scoreboard
# ---------------------------------------------------------------------------

class TestScoreboard:
    def test_scoreboard_aggregates_metrics_across_variations(
            self, chain_client, scan_root):
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        mod.CHAIN.variations['baseline'].run(rr)
        mod.CHAIN.variations['jittered'].run(rr)
        d = c.get('/api/scoreboard/mychain').json()
        assert d['chain'] == 'mychain'
        keys = set(d['metric_keys'])
        assert 'forward.sim.flux' in keys
        assert 'recon.recon.iters_done' in keys
        rows = {row['variation']: row['metrics'] for row in d['rows']}
        #baseline: n=1 -> flux=10; jittered: n=2 -> flux=20
        assert rows['baseline']['forward.sim.flux'] == 10
        assert rows['jittered']['forward.sim.flux'] == 20
        #Recon metric same for both (ePIE, iters=100).
        assert rows['baseline']['recon.recon.iters_done'] == 100
        assert rows['jittered']['recon.recon.iters_done'] == 100

    def test_scoreboard_skips_pending_steps(self, chain_client, scan_root):
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        mod.CHAIN.variations['baseline'].run(rr)
        d = c.get('/api/scoreboard/mychain').json()
        #Jittered has no runs yet — its row should still be present but
        #with no metrics.
        rows = {row['variation']: row['metrics'] for row in d['rows']}
        assert rows['jittered'] == {}


# ---------------------------------------------------------------------------
# Chain provenance surfaces through existing /api/run
# ---------------------------------------------------------------------------

class TestRunDetailHasChainProvenance:
    def test_run_record_carries_chain_variation_upstream(
            self, chain_client, scan_root):
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        runs = mod.CHAIN.variations['baseline'].run(rr)
        recon = runs['recon']
        d = c.get(f'/api/run/recon/ePIE/{recon.fingerprint[:12]}').json()
        assert d['run']['chain'] == 'mychain'
        assert d['run']['variation'] == 'baseline'
        assert d['run']['upstream'] == {'forward': runs['forward'].fingerprint}


# ---------------------------------------------------------------------------
# ctx.metric() core
# ---------------------------------------------------------------------------

class TestMetricCore:
    def test_metric_writes_to_metrics_json(self, tmp_path, monkeypatch):
        monkeypatch.syspath_prepend(str(tmp_path))
        dm.registry._variants.clear()
        dm.registry.register('base', module='m', x=1)
        def _f(ctx):
            ctx.metric('s', 'foo', 42)
            ctx.metric('s', 'bar', 1.5)
            return {}
        p = dm.Pipeline('mp', [dm.Stage('s', _f)], module='m')
        rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
        rec = p.run(dm.registry.get('m', 'base'), rr)
        mp = Path(rec.fdir, 'stages/s/metrics.json')
        assert mp.exists()
        data = json.loads(mp.read_text())
        assert data == {'foo': 42, 'bar': 1.5}

    def test_metric_repeat_call_overwrites_same_key(self, tmp_path, monkeypatch):
        monkeypatch.syspath_prepend(str(tmp_path))
        dm.registry._variants.clear()
        dm.registry.register('base', module='m', x=1)
        def _f(ctx):
            ctx.metric('s', 'x', 1)
            ctx.metric('s', 'x', 99)
            return {}
        p = dm.Pipeline('mp', [dm.Stage('s', _f)], module='m')
        rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
        rec = p.run(dm.registry.get('m', 'base'), rr)
        data = json.loads(Path(rec.fdir, 'stages/s/metrics.json').read_text())
        assert data == {'x': 99}
