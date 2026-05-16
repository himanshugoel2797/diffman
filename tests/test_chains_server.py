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
from diffman.server import create_app, _build_forest


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
        forest = _build_forest([
            {'name': 'a', 'parent': None, 'step_count': 1, 'variation_count': 0},
            {'name': 'b', 'parent': 'a',  'step_count': 1, 'variation_count': 0},
        ], key='name')
        assert len(forest) == 1
        assert forest[0]['name'] == 'a'
        assert forest[0]['children'][0]['name'] == 'b'

    def test_forest_flags_orphan_parent(self):
        forest = _build_forest([
            {'name': 'lone', 'parent': 'ghost',
             'step_count': 1, 'variation_count': 0},
        ], key='name')
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

    def test_progress_does_not_misattribute_sibling_recon_run(
            self, chain_client, scan_root):
        """The critical correctness property: when baseline.recon ran with
        upstream={forward: base_fp} and jittered.forward later ran on its
        own, asking for jittered's progress must NOT report jittered.recon
        as done — that recon=ePIE run's upstream fingerprint doesn't match
        jittered's forward fingerprint, even though the variant name does.
        """
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        discovery.load_module('_diffman_chain_fwd')
        mod = sys.modules['_diffman_chain_parent']
        fwd_mod = sys.modules['_diffman_chain_fwd']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        #Run baseline fully: forward=base AND recon=ePIE+upstream=base_fp.
        baseline_runs = mod.CHAIN.variations['baseline'].run(rr)
        #Manually create JUST forward=jitter so the cascading-pending
        #short-circuit doesn't hide the upstream-fp join we want to test.
        jitter_variant = dm.registry.get('_diffman_chain_fwd', 'jitter')
        fwd_jitter = fwd_mod.PIPELINE.run(jitter_variant, rr)
        d = c.get('/api/chain_progress/mychain/jittered').json()
        steps = {s['name']: s for s in d['steps']}
        #forward must be done — the run we just made exists.
        assert steps['forward']['status'] in ('done', 'cached')
        assert steps['forward']['fingerprint'] == fwd_jitter.fingerprint
        #recon must be pending — the only recon=ePIE run's upstream points
        #at forward=base, not forward=jitter.
        assert steps['recon']['status'] == 'pending'
        assert steps['recon']['fingerprint'] is None
        #Sanity: baseline still shows recon=done (proves the recon run
        #from earlier is being correctly attributed when upstream DOES match).
        d2 = c.get('/api/chain_progress/mychain/baseline').json()
        baseline_recon = {s['name']: s for s in d2['steps']}['recon']
        assert baseline_recon['status'] in ('done', 'cached')
        assert baseline_recon['fingerprint'] == baseline_runs['recon'].fingerprint

    def test_progress_reports_mixed_when_some_stages_not_terminal(
            self, chain_client, scan_root):
        """A run whose stages have a mix of statuses (e.g. one done,
        another still running because the process crashed mid-flush)
        must surface as 'mixed' so the UI can flag it for inspection."""
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        mod = sys.modules['_diffman_chain_parent']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        runs = mod.CHAIN.variations['baseline'].run(rr)
        #Hand-edit recon's run.json to simulate a crash mid-execution.
        rj_path = Path(runs['recon'].fdir, 'run.json')
        rj = json.loads(rj_path.read_text())
        rj['stage_status']['recon'] = 'running'   #not in {done, cached}
        rj_path.write_text(json.dumps(rj))
        app_reg = c.app.state.registry
        app_reg.invalidate()
        d = c.get('/api/chain_progress/mychain/baseline').json()
        recon = {s['name']: s for s in d['steps']}['recon']
        assert recon['status'] == 'mixed'

    def test_progress_surfaces_failed_step_with_error_and_completed_siblings(
            self, chain_client, scan_root):
        """Mid-chain failure must (a) surface the failed step's traceback
        in the progress response and (b) not hide that earlier steps
        completed successfully — both are needed for the UI to render a
        useful failure view."""
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
        steps = {s['name']: s for s in d['steps']}
        #recon: failed, with the exception text in the per-stage errors.
        assert steps['recon']['status'] == 'failed'
        assert 'recon kaboom' in steps['recon']['errors']['recon']
        #forward: ran successfully before recon raised — must NOT be
        #reported as anything other than done/cached.
        assert steps['forward']['status'] in ('done', 'cached')
        assert steps['forward']['fingerprint'] is not None


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

    def test_metric_recovers_from_corrupt_metrics_json(self, tmp_path,
                                                        monkeypatch):
        """A malformed pre-existing metrics.json (truncated write, manual
        edit gone wrong) must not poison subsequent metric() calls — the
        file should just be reset rather than raising."""
        monkeypatch.syspath_prepend(str(tmp_path))
        dm.registry._variants.clear()
        dm.registry.register('base', module='m', x=1)
        def _f(ctx):
            #Stage runs once, sees a broken metrics.json from "elsewhere".
            mp = Path(ctx.stage_dir('s')) / 'metrics.json'
            mp.write_text('{this is not json')
            ctx.metric('s', 'fresh', 7)
            return {}
        p = dm.Pipeline('mp', [dm.Stage('s', _f)], module='m')
        rr = dm.RunRegistry(root=str(tmp_path / 'runs'))
        rec = p.run(dm.registry.get('m', 'base'), rr)
        data = json.loads(Path(rec.fdir, 'stages/s/metrics.json').read_text())
        assert data == {'fresh': 7}


# ---------------------------------------------------------------------------
# Multi-chain modules (CHAINS = [...])
# ---------------------------------------------------------------------------

class TestChainsListExport:
    def test_module_exporting_CHAINS_list_indexes_all_of_them(
            self, scan_root, make_pipeline):
        make_pipeline('_diffman_chain_multi', """
            import diffman as dm
            def _f(ctx): return {}
            P = dm.Pipeline('mp', [dm.Stage('s', _f)])
            CHAINS = [
                dm.Chain('ch_one', steps=[dm.ChainStep('a', P)]),
                dm.Chain('ch_two', steps=[dm.ChainStep('a', P)]),
            ]
        """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=False)
        with TestClient(app) as c:
            forest = c.get('/api/chains').json()['forest']
        names = {n['name'] for n in forest}
        assert {'ch_one', 'ch_two'} <= names
        assert discovery.CHAIN_TO_MODULE['ch_one'] == '_diffman_chain_multi'
        assert discovery.CHAIN_TO_MODULE['ch_two'] == '_diffman_chain_multi'

    def test_module_with_both_CHAIN_and_CHAINS_indexes_union(
            self, scan_root, make_pipeline):
        """A module is allowed to declare both `CHAIN = X` and
        `CHAINS = [Y, Z]` simultaneously — all three should be picked up."""
        make_pipeline('_diffman_chain_both', """
            import diffman as dm
            def _f(ctx): return {}
            P = dm.Pipeline('p', [dm.Stage('s', _f)])
            CHAIN  = dm.Chain('singleton', steps=[dm.ChainStep('a', P)])
            CHAINS = [dm.Chain('list_a', steps=[dm.ChainStep('a', P)]),
                      dm.Chain('list_b', steps=[dm.ChainStep('a', P)])]
        """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=False)
        with TestClient(app) as c:
            names = {n['name'] for n in
                     c.get('/api/chains').json()['forest']}
        assert {'singleton', 'list_a', 'list_b'} <= names


# ---------------------------------------------------------------------------
# /api/chain_variation_diff edge cases
# ---------------------------------------------------------------------------

class TestChainVariationDiffEdges:
    def test_variation_diff_marks_absent_variant_column(self, scan_root,
                                                         make_pipeline):
        """When one of the variations resolves a variant name that doesn't
        exist in the pipeline's registry, that column must be marked
        present=False rather than crashing."""
        make_pipeline('_diffman_chain_vd_fwd', """
            import diffman as dm
            dm.register('base', n=1)
            def _f(ctx): return {}
            PIPELINE = dm.Pipeline('vd_fwd',
                [dm.Stage('s', _f, config_keys=('n',))])
        """)
        make_pipeline('_diffman_chain_vd_chain', """
            import diffman as dm
            import _diffman_chain_vd_fwd as fwd
            CHAIN = dm.Chain('vd', steps=[dm.ChainStep('fwd', fwd.PIPELINE)])
            CHAIN.variation('real',   fwd='base')
            CHAIN.variation('phantom', fwd='does_not_exist')
        """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=False)
        with TestClient(app) as c:
            c.get('/api/chains')
            d = c.get('/api/chain_variation_diff'
                      '?chain=vd&variations=real,phantom').json()
        fwd_step = d['steps'][0]
        by_variation = {col['variation']: col for col in fwd_step['columns']}
        assert by_variation['real']['present'] is True
        assert by_variation['phantom']['present'] is False
        assert 'error' in by_variation['phantom']

    def test_variation_diff_404_when_chain_missing(self, chain_client):
        c, _ = chain_client
        r = c.get('/api/chain_variation_diff'
                  '?chain=ghost&variations=a,b')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# /api/scoreboard edge cases
# ---------------------------------------------------------------------------

class TestScoreboardEdges:
    def test_scoreboard_empty_when_no_runs(self, chain_client):
        c, _ = chain_client
        d = c.get('/api/scoreboard/mychain').json()
        assert d['chain'] == 'mychain'
        assert d['metric_keys'] == []
        #Every declared variation gets a row, even with no metrics.
        var_names = {r['variation'] for r in d['rows']}
        assert var_names == {'baseline', 'jittered', 'algo_dm'}
        assert all(r['metrics'] == {} for r in d['rows'])

    def test_scoreboard_does_not_misattribute_metrics_across_variations(
            self, chain_client, scan_root):
        """Same critical correctness property as test_progress_does_not_
        misattribute: a variation's scoreboard row must only include
        metrics from runs whose upstream-fp matches that variation, even
        when sibling variations share the same downstream variant name."""
        c, _ = chain_client
        discovery.load_module('_diffman_chain_parent')
        discovery.load_module('_diffman_chain_fwd')
        mod = sys.modules['_diffman_chain_parent']
        fwd_mod = sys.modules['_diffman_chain_fwd']
        rr = dm.RunRegistry(root=str(scan_root / 'runs'))
        #Run baseline fully (forward=base + recon=ePIE+upstream=base_fp).
        mod.CHAIN.variations['baseline'].run(rr)
        #Run JUST forward=jitter alone, no recon for jittered.
        jitter_v = dm.registry.get('_diffman_chain_fwd', 'jitter')
        fwd_mod.PIPELINE.run(jitter_v, rr)
        d = c.get('/api/scoreboard/mychain').json()
        rows = {row['variation']: row['metrics'] for row in d['rows']}
        #jittered must show forward.sim.flux (its own forward ran) but
        #must NOT show recon.recon.iters_done — that's baseline's recon.
        assert rows['jittered'].get('forward.sim.flux') == 20
        assert 'recon.recon.iters_done' not in rows['jittered']
        #baseline still has both (sanity).
        assert rows['baseline']['forward.sim.flux'] == 10
        assert rows['baseline']['recon.recon.iters_done'] == 100


# ---------------------------------------------------------------------------
# /api/chains: a module that fails to import is tolerated
# ---------------------------------------------------------------------------

class TestChainsToleratesBrokenModule:
    def test_chains_endpoint_skips_modules_that_fail_to_import(
            self, scan_root, make_pipeline):
        #One good chain + one broken module. The endpoint should return
        #the good one rather than 500'ing.
        make_pipeline('_diffman_chain_good', """
            import diffman as dm
            def _f(ctx): return {}
            P = dm.Pipeline('good', [dm.Stage('s', _f)])
            CHAIN = dm.Chain('alive', steps=[dm.ChainStep('a', P)])
        """)
        make_pipeline('_diffman_chain_broken', """
            import diffman as dm
            raise RuntimeError('boom on import')
            PIPELINE = dm.Pipeline('never', [])
        """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=False)
        with TestClient(app) as c:
            forest = c.get('/api/chains').json()['forest']
        names = {n['name'] for n in forest}
        assert 'alive' in names


# ---------------------------------------------------------------------------
# Chain source diff edges
# ---------------------------------------------------------------------------

class TestChainSourceDiffEdges:
    def test_source_diff_404_when_parent_chain_not_in_scan(
            self, scan_root, make_pipeline):
        """Chain declares parent='ghost' but no chain named 'ghost' exists
        anywhere — must surface as 404, not silent empty diff (the user
        likely typoed the parent name)."""
        make_pipeline('_diffman_chain_orphan', """
            import diffman as dm
            def _f(ctx): return {}
            P = dm.Pipeline('p', [dm.Stage('s', _f)])
            CHAIN = dm.Chain('orphaned', parent='ghost',
                             steps=[dm.ChainStep('a', P)])
        """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=False)
        with TestClient(app) as c:
            c.get('/api/chains')
            r = c.get('/api/chain_source_diff?chain=orphaned')
        assert r.status_code == 404

    def test_source_diff_404_when_chain_not_found(self, chain_client):
        c, _ = chain_client
        r = c.get('/api/chain_source_diff?chain=ghost')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Chain-only modules don't pretend to be broken pipelines
# ---------------------------------------------------------------------------

class TestChainOnlyModuleInPipelineEndpoint:
    def test_chain_only_module_visible_in_chain_forest_not_pipeline_forest(
            self, scan_root, make_pipeline):
        """A module that declares only CHAIN (no PIPELINE) is a valid
        chain-only module. It must appear in /api/chains, and must NOT
        clutter /api/pipelines as a 'no PIPELINE attribute' error
        (which would imply the user forgot to declare one)."""
        make_pipeline('_diffman_chain_alone', """
            import diffman as dm
            def _f(ctx): return {}
            P = dm.Pipeline('helper', [dm.Stage('s', _f)],
                            module='_diffman_chain_alone_pipes')
            CHAIN = dm.Chain('solo', steps=[dm.ChainStep('a', P)])
        """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=False)
        with TestClient(app) as c:
            chains = c.get('/api/chains').json()['forest']
            pipes = c.get('/api/pipelines').json()['forest']
        assert any(n['name'] == 'solo' for n in chains)
        #The pipeline forest may include the module-defined PIPELINE
        #(here, 'helper') but must NOT contain an error entry for the
        #chain-only module just because it lacks a PIPELINE attribute.
        for n in pipes:
            assert n.get('error') != 'no PIPELINE attribute'
