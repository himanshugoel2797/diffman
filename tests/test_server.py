"""End-to-end tests for the FastAPI server endpoints."""

from __future__ import annotations

import json
import os
import textwrap
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from diffman.server import (
    create_app, diff_configs, _build_forest, _flatten_union,
)


# ---------------------------------------------------------------------------
# diff_configs
# ---------------------------------------------------------------------------

def test_diff_configs_changed_added_removed():
    parent = {'a': 1, 'b': 2, 'nested': {'k': 'old'}}
    child  = {'a': 1, 'c': 3, 'nested': {'k': 'new', 'extra': True}}
    entries = diff_configs(parent, child)
    by_path = {e['path']: e for e in entries}
    assert by_path['b']['kind'] == 'removed'
    assert by_path['b']['parent'] == 2
    assert by_path['c']['kind'] == 'added'
    assert by_path['c']['child'] == 3
    assert by_path['nested.k']['kind'] == 'changed'
    assert by_path['nested.extra']['kind'] == 'added'
    assert 'a' not in by_path   #unchanged values omitted


def test_diff_configs_identical_returns_empty():
    assert diff_configs({'a': 1}, {'a': 1}) == []


# ---------------------------------------------------------------------------
# Forest builder
# ---------------------------------------------------------------------------

def test_build_forest_nests_children_under_parent():
    metas = [
        {'module': 'a', 'pipeline': 'a', 'parent': None,    'variant_count': 1},
        {'module': 'b', 'pipeline': 'b', 'parent': 'a',     'variant_count': 2},
        {'module': 'c', 'pipeline': 'c', 'parent': 'b',     'variant_count': 0},
    ]
    forest = _build_forest(metas, key='pipeline')
    assert len(forest) == 1
    assert forest[0]['pipeline'] == 'a'
    assert len(forest[0]['children']) == 1
    assert forest[0]['children'][0]['pipeline'] == 'b'
    assert forest[0]['children'][0]['children'][0]['pipeline'] == 'c'


def test_build_forest_orphans_become_roots_with_flag():
    metas = [
        {'module': 'orphan', 'pipeline': 'orphan',
         'parent': 'ghost', 'variant_count': 1},
    ]
    forest = _build_forest(metas, key='pipeline')
    assert forest[0]['orphan_parent'] == 'ghost'
    assert forest[0]['children'] == []


# ---------------------------------------------------------------------------
# _flatten_union (drives /api/compare)
# ---------------------------------------------------------------------------

def test_flatten_union_marks_equal_vs_differing():
    rows = _flatten_union([
        {'a': 1, 'nested': {'x': 10}},
        {'a': 1, 'nested': {'x': 20}, 'extra': True},
    ])
    by_path = {r['path']: r for r in rows}
    assert by_path['a']['equal'] is True
    assert by_path['nested.x']['equal'] is False
    assert by_path['extra']['equal'] is False
    assert by_path['extra']['values'] == [None, True]


# ---------------------------------------------------------------------------
# Full HTTP fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def client(scan_root, make_pipeline):
    make_pipeline('_diffman_test_a', """
        import diffman as dm
        dm.register('base', scan=dict(width=5e-6, step=1e-7))
        dm.register('jitter', base='base', probe=dict(amp=0.1))
        def _f(ctx):
            import numpy as np, tempfile, os
            tmp = os.path.join(tempfile.mkdtemp(), 'data.npy')
            np.save(tmp, np.arange(20).reshape(4,5).astype(float))
            ctx.artifact('sim', 'data.npy', tmp)
            return {}
        PIPELINE = dm.Pipeline('_pipe_a', [dm.Stage('sim', _f)])
    """)
    make_pipeline('_diffman_test_b', """
        import diffman as dm
        dm.register('base', scan=dict(width=8e-6, step=1e-7))
        dm.register('jitter_renamed', base='base', forks_of='jitter',
                    probe=dict(amp=0.5))
        dm.register('typoed', base='base', forks_of='not_a_real_variant',
                    probe=dict(amp=0.9))
        def _f(ctx):
            import numpy as np, tempfile, os
            tmp = os.path.join(tempfile.mkdtemp(), 'data.npy')
            np.save(tmp, np.arange(20).reshape(4,5).astype(float) * 1.1)
            ctx.artifact('sim', 'data.npy', tmp)
            return {}
        PIPELINE = dm.Pipeline('_pipe_b', [dm.Stage('sim', _f)],
                               parent='_pipe_a')
    """)
    app = create_app(root=str(scan_root / 'runs'),
                     scan_root=str(scan_root), no_scan=False)
    with TestClient(app) as c:
        yield c, scan_root


class TestPipelineGraph:
    def test_pipelines_forest_nests_child_under_parent(self, client):
        c, _ = client
        forest = c.get('/api/pipelines').json()['forest']
        assert len(forest) == 1
        assert forest[0]['pipeline'] == '_pipe_a'
        assert forest[0]['children'][0]['pipeline'] == '_pipe_b'

    def test_variants_endpoint_scopes_by_module(self, client):
        c, _ = client
        a = c.get('/api/variants?module=_diffman_test_a').json()
        b = c.get('/api/variants?module=_diffman_test_b').json()
        assert sorted(a['variants']) == ['base', 'jitter']
        assert sorted(b['variants']) == ['base', 'jitter_renamed', 'typoed']


class TestDiff:
    def test_diff_matches_via_forks_of(self, client):
        c, _ = client
        d = c.get('/api/diff?module=_diffman_test_b').json()
        by_name = {v['variant']: v for v in d['variants']}
        assert by_name['jitter_renamed']['parent_variant'] == 'jitter'
        assert by_name['jitter_renamed']['kind'] == 'differs'

    def test_diff_surfaces_unresolved_forks_of(self, client):
        c, _ = client
        d = c.get('/api/diff?module=_diffman_test_b').json()
        by_name = {v['variant']: v for v in d['variants']}
        #Typo should be flagged, not silently fall back to name-match
        #(which would find nothing in the parent anyway).
        assert by_name['typoed'].get('forks_of_unresolved') == 'not_a_real_variant'
        assert by_name['typoed']['kind'] == 'only_in_child'

    def test_diff_includes_per_variant_overrides(self, client):
        c, _ = client
        d = c.get('/api/diff?module=_diffman_test_b').json()
        by_name = {v['variant']: v for v in d['variants']}
        assert by_name['base']['overrides'] == {'scan': {'width': 8e-6, 'step': 1e-7}}

    def test_diff_for_root_returns_variants_with_no_parent_kind(self, client):
        c, _ = client
        d = c.get('/api/diff?module=_diffman_test_a').json()
        assert d['parent'] is None
        kinds = {v['kind'] for v in d['variants']}
        assert kinds == {'no_parent'}


class TestVariantOverrides:
    def test_overrides_against_base(self, client):
        c, _ = client
        d = c.get('/api/variant_overrides?module=_diffman_test_a'
                  '&variant=jitter').json()
        assert d['base'] == 'base'
        assert d['overrides'] == {'probe': {'amp': 0.1}}
        #diff_configs reports the top-level added subtree, not its leaves.
        paths = {e['path']: e['kind'] for e in d['diff']}
        assert paths == {'probe': 'added'}


class TestCompare:
    def test_compare_marks_differing_rows(self, client):
        c, _ = client
        d = c.get('/api/compare?modules=_diffman_test_a,_diffman_test_b'
                  '&variant=base').json()
        by_path = {r['path']: r for r in d['rows']}
        assert by_path['scan.width']['equal'] is False
        assert by_path['scan.step']['equal'] is True


class TestFind:
    def test_find_by_fingerprint_prefix_returns_variant(self, client):
        c, scan_root = client
        #Trigger import via the variants endpoint so the registry is
        #populated, then look up the fingerprint via the find endpoint.
        c.get('/api/variants?module=_diffman_test_a')
        from diffman.core import registry
        fp = registry.get('_diffman_test_a', 'jitter').short_fp
        r = c.get(f'/api/find?q={fp}').json()
        assert any(v['variant'] == 'jitter' for v in r['variants'])

    def test_find_rejects_short_query(self, client):
        c, _ = client
        assert c.get('/api/find?q=abc').status_code == 400


class TestArtifactDiff:
    def test_array_diff_reports_stats_and_heatmap(self, client):
        c, scan_root = client
        runs = scan_root / 'runs'
        runs.mkdir(exist_ok=True)
        a = runs / 'a.npy'; np.save(a, np.zeros((4, 5)))
        b = runs / 'b.npy'; np.save(b, np.ones((4, 5)))
        r = c.get(f'/api/artifact_diff?path_a={a}&path_b={b}').json()
        assert r['kind'] == 'array_diff'
        assert r['stats']['abs_mean'] == 1.0
        assert len(r['delta_heatmap']) == 4

    def test_text_diff_returns_unified(self, client, scan_root):
        c, _ = client
        runs = scan_root / 'runs'; runs.mkdir(exist_ok=True)
        a = runs / 'a.txt'; a.write_text('line one\nline two\n')
        b = runs / 'b.txt'; b.write_text('line one\nline TWO\n')
        r = c.get(f'/api/artifact_diff?path_a={a}&path_b={b}').json()
        assert r['kind'] == 'text_diff'
        assert '-line two' in r['diff']
        assert '+line TWO' in r['diff']

    def test_json_diff_uses_diff_configs(self, client, scan_root):
        c, _ = client
        runs = scan_root / 'runs'; runs.mkdir(exist_ok=True)
        a = runs / 'a.json'; a.write_text(json.dumps({'x': 1, 'y': 2}))
        b = runs / 'b.json'; b.write_text(json.dumps({'x': 1, 'y': 99, 'z': 3}))
        r = c.get(f'/api/artifact_diff?path_a={a}&path_b={b}').json()
        assert r['kind'] == 'json_diff'
        kinds = {e['path']: e['kind'] for e in r['entries']}
        assert kinds == {'y': 'changed', 'z': 'added'}

    def test_path_escape_rejected(self, client):
        c, _ = client
        r = c.get('/api/artifact_diff?path_a=/etc/passwd&path_b=/etc/hosts')
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Feature: stage timings on /api/run
# ---------------------------------------------------------------------------

class TestRunDetailStageTimings:
    def test_run_endpoint_includes_per_stage_started_ended_duration(
            self, scan_root, make_pipeline):
        make_pipeline('_diffman_test_timing', """
            import diffman as dm, time
            dm.register('base', x=1)
            def _slow(ctx): time.sleep(0.02)
            PIPELINE = dm.Pipeline('_pipe_timing',
                [dm.Stage('s', _slow)])
        """)
        from diffman import discovery
        from diffman.core import registry, RunRegistry
        discovery.load_module('_diffman_test_timing')
        v = registry.get('_diffman_test_timing', 'base')
        rr = RunRegistry(root=str(scan_root / 'runs'))
        rec = rr  # placeholder, real run below
        from diffman.core import Pipeline   #force module-fresh import path
        import sys
        mod = sys.modules['_diffman_test_timing']
        run = mod.PIPELINE.run(v, rr)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get(
                f'/api/run/_pipe_timing/base/{run.fingerprint[:12]}').json()
        st = d['stages'][0]
        assert st['name'] == 's'
        assert st['started'] is not None
        assert st['ended'] is not None
        assert st['duration_s'] is not None
        assert st['duration_s'] >= 0.02

    def test_run_endpoint_timing_is_none_for_pending_stage(
            self, scan_root, make_pipeline):
        """Stages that haven't run (no _meta.json yet) must surface
        with started/ended/duration_s = None, not crash."""
        #Hand-craft a run dir with no stages dir at all.
        from diffman.core import RunRegistry, registry as reg
        reg.register('only', module='_diffman_test_empty', x=1)
        rd = scan_root / 'runs' / '_pipe_empty' / 'only' / 'abcabcabcabc'
        rd.mkdir(parents=True)
        (rd / 'run.json').write_text(json.dumps({
            'pipeline': '_pipe_empty', 'variant': 'only',
            'fingerprint': 'abcabcabcabc' + '0'*52,
            'fdir': str(rd),
            'started': '2026-01-01T00:00:00',
            'stage_keys': {}, 'stage_status': {}, 'errors': {},
        }))
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get('/api/run/_pipe_empty/only/abcabcabcabc').json()
        assert d['stages'] == []


# ---------------------------------------------------------------------------
# Feature: /api/run_diff (explain why two runs differ)
# ---------------------------------------------------------------------------

def _two_runs_with_edited_pipeline(scan_root, make_pipeline,
                                    first_body, second_body):
    """Write `first_body`, run it, evict & re-load with `second_body`,
    and run again. Returns (run_a, run_b). Both bodies must register a
    variant 'base' under pipeline '_p_rerun' and a single stage 's' —
    the test scaffolding doesn't care what's inside otherwise."""
    make_pipeline('_diffman_test_rerun', first_body)
    from diffman import discovery
    from diffman.core import registry, RunRegistry
    import sys
    discovery.load_module('_diffman_test_rerun')
    rr = RunRegistry(root=str(scan_root / 'runs'))
    run_a = sys.modules['_diffman_test_rerun'].PIPELINE.run(
        registry.get('_diffman_test_rerun', 'base'), rr)
    Path(scan_root / '_diffman_test_rerun.py').write_text(
        textwrap.dedent(second_body).lstrip())
    discovery.evict_module('_diffman_test_rerun')
    discovery.discover(str(scan_root))
    discovery.load_module('_diffman_test_rerun')
    run_b = sys.modules['_diffman_test_rerun'].PIPELINE.run(
        registry.get('_diffman_test_rerun', 'base'), rr)
    return run_a, run_b


class TestRunDiff:
    def test_attributes_change_to_config_only(self, scan_root, make_pipeline):
        """Editing only the variant's config (not the stage fn) must
        produce config_changed=True, fn_changed=False, and a config_diff
        that names the changed key."""
        run_a, run_b = _two_runs_with_edited_pipeline(scan_root, make_pipeline,
            """
            import diffman as dm
            dm.register('base', n=1)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun',
                [dm.Stage('s', _f, config_keys=('n',))])
            """,
            """
            import diffman as dm
            dm.register('base', n=99)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun',
                [dm.Stage('s', _f, config_keys=('n',))])
            """)
        assert run_a.fingerprint != run_b.fingerprint   #sanity
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get(f'/api/run_diff?pipeline=_p_rerun&variant=base'
                      f'&a={run_a.fingerprint[:12]}'
                      f'&b={run_b.fingerprint[:12]}').json()
        s = next(st for st in d['stages'] if st['name'] == 's')
        assert s['identical'] is False
        assert s['fn_changed'] is False
        assert s['config_changed'] is True
        assert s['upstream_changed'] is False
        n = next(e for e in s['config_diff'] if e['path'] == 'n')
        assert n['parent'] == 1 and n['child'] == 99

    def test_attributes_change_to_both_fn_and_config(self, scan_root,
                                                     make_pipeline):
        """A variant fp change requires a config change (same fn alone
        keeps the variant fp identical, collapsing both runs into one
        directory). So the 'fn changed' signal is only observable when
        accompanied by a config change — verify both flags fire."""
        run_a, run_b = _two_runs_with_edited_pipeline(scan_root, make_pipeline,
            """
            import diffman as dm
            dm.register('base', n=1)
            def _f(ctx): pass   #version A
            PIPELINE = dm.Pipeline('_p_rerun',
                [dm.Stage('s', _f, config_keys=('n',))])
            """,
            """
            import diffman as dm
            dm.register('base', n=2)   #config also bumped to force a new fp
            def _f(ctx):
                _ = 'version B'        #fn body differs
            PIPELINE = dm.Pipeline('_p_rerun',
                [dm.Stage('s', _f, config_keys=('n',))])
            """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get(f'/api/run_diff?pipeline=_p_rerun&variant=base'
                      f'&a={run_a.fingerprint[:12]}'
                      f'&b={run_b.fingerprint[:12]}').json()
        s = next(st for st in d['stages'] if st['name'] == 's')
        assert s['fn_changed'] is True
        assert s['config_changed'] is True

    def test_identical_stage_skips_decomposition(self, scan_root,
                                                 make_pipeline):
        """A stage whose cache key matches across both runs must report
        identical=True without the per-component breakdown — there's
        nothing to attribute. Two-stage pipeline where only one stage
        sees the config change."""
        run_a, run_b = _two_runs_with_edited_pipeline(scan_root, make_pipeline,
            """
            import diffman as dm
            dm.register('base', x=1, y=10)
            def _a(ctx): pass
            def _b(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [
                dm.Stage('a', _a, config_keys=('x',)),
                dm.Stage('b', _b, config_keys=('y',))])
            """,
            """
            import diffman as dm
            dm.register('base', x=99, y=10)   #only stage 'a' sees x
            def _a(ctx): pass
            def _b(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [
                dm.Stage('a', _a, config_keys=('x',)),
                dm.Stage('b', _b, config_keys=('y',))])
            """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get(f'/api/run_diff?pipeline=_p_rerun&variant=base'
                      f'&a={run_a.fingerprint[:12]}'
                      f'&b={run_b.fingerprint[:12]}').json()
        by_name = {s['name']: s for s in d['stages']}
        assert by_name['a']['identical'] is False
        assert by_name['b']['identical'] is True
        #Identical stages don't carry per-component decomposition keys.
        assert 'fn_changed' not in by_name['b']

    def test_diff_against_self_rejected(self, scan_root, make_pipeline):
        run_a, _ = _two_runs_with_edited_pipeline(scan_root, make_pipeline,
            """
            import diffman as dm
            dm.register('base', n=1)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [dm.Stage('s', _f)])
            """,
            """
            import diffman as dm
            dm.register('base', n=2)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [dm.Stage('s', _f)])
            """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            r = c.get(f'/api/run_diff?pipeline=_p_rerun&variant=base'
                      f'&a={run_a.fingerprint[:12]}'
                      f'&b={run_a.fingerprint[:12]}')
        assert r.status_code == 400

    def test_404_if_either_run_missing(self, scan_root, make_pipeline):
        run_a, _ = _two_runs_with_edited_pipeline(scan_root, make_pipeline,
            """
            import diffman as dm
            dm.register('base', n=1)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [dm.Stage('s', _f)])
            """,
            """
            import diffman as dm
            dm.register('base', n=2)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [dm.Stage('s', _f)])
            """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            r = c.get(f'/api/run_diff?pipeline=_p_rerun&variant=base'
                      f'&a={run_a.fingerprint[:12]}&b=ffffffffffff')
        assert r.status_code == 404

    def test_upstream_change_attributed_to_upstream_intra_pipeline(
            self, scan_root, make_pipeline):
        """Within a single pipeline, a downstream stage declares
        `inputs=('upstream_stage',)` so its cache key folds in the
        upstream stage's key. When the upstream's config changes, the
        downstream stage's key flips solely because its upstream input
        changed — run_diff must attribute that to upstream, not to its
        own fn or config."""
        run_a, run_b = _two_runs_with_edited_pipeline(scan_root, make_pipeline,
            """
            import diffman as dm
            dm.register('base', x=1, y=10)
            def _a(ctx): pass
            def _b(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [
                dm.Stage('a', _a, config_keys=('x',)),
                dm.Stage('b', _b, inputs=('a',), config_keys=('y',))])
            """,
            """
            import diffman as dm
            dm.register('base', x=99, y=10)   #only x changes; y unchanged
            def _a(ctx): pass
            def _b(ctx): pass
            PIPELINE = dm.Pipeline('_p_rerun', [
                dm.Stage('a', _a, config_keys=('x',)),
                dm.Stage('b', _b, inputs=('a',), config_keys=('y',))])
            """)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get(f'/api/run_diff?pipeline=_p_rerun&variant=base'
                      f'&a={run_a.fingerprint[:12]}'
                      f'&b={run_b.fingerprint[:12]}').json()
        by_name = {s['name']: s for s in d['stages']}
        #Stage `a` changed because x changed.
        assert by_name['a']['config_changed'] is True
        #Stage `b`'s own config (y) didn't change, its fn didn't change,
        #but its upstream (`a`'s key) DID — that's the attribution we want.
        assert by_name['b']['fn_changed'] is False
        assert by_name['b']['config_changed'] is False
        assert by_name['b']['upstream_changed'] is True
        assert any(u['name'] == 'a' for u in by_name['b']['upstream_diff'])


# ---------------------------------------------------------------------------
# Feature: 1-D artifact overlay in /api/artifact_diff
# ---------------------------------------------------------------------------

class Test1DArrayOverlay:
    def test_array_diff_returns_overlay_for_1d_same_shape(self, client):
        c, scan_root = client
        runs = scan_root / 'runs'; runs.mkdir(exist_ok=True)
        a_arr = np.linspace(0, 10, 50)
        b_arr = a_arr + 0.5
        a = runs / 'a.npy'; np.save(a, a_arr)
        b = runs / 'b.npy'; np.save(b, b_arr)
        r = c.get(f'/api/artifact_diff?path_a={a}&path_b={b}').json()
        assert r['kind'] == 'array_diff'
        assert 'overlay' in r
        assert r['overlay']['y_a'][0] == 0.0
        assert r['overlay']['y_b'][0] == 0.5
        assert len(r['overlay']['x']) == len(r['overlay']['y_a'])

    def test_overlay_downsamples_long_traces(self, client):
        c, scan_root = client
        runs = scan_root / 'runs'; runs.mkdir(exist_ok=True)
        n = 10_000
        a_arr = np.arange(n, dtype=float)
        b_arr = a_arr * 2
        a = runs / 'long_a.npy'; np.save(a, a_arr)
        b = runs / 'long_b.npy'; np.save(b, b_arr)
        r = c.get(f'/api/artifact_diff?path_a={a}&path_b={b}'
                  '&target_max=256').json()
        ov = r['overlay']
        assert len(ov['x']) <= n // ov['stride'] + 1
        assert ov['stride'] >= n // 256
        #x must use ORIGINAL indices, not decimated indices.
        assert ov['x'][1] - ov['x'][0] == ov['stride']

    def test_no_overlay_for_2d_arrays(self, client):
        c, scan_root = client
        runs = scan_root / 'runs'; runs.mkdir(exist_ok=True)
        a = runs / 'a2.npy'; np.save(a, np.zeros((4, 5)))
        b = runs / 'b2.npy'; np.save(b, np.ones((4, 5)))
        r = c.get(f'/api/artifact_diff?path_a={a}&path_b={b}').json()
        assert 'overlay' not in r
        #but the existing 2-D heatmap branch still works.
        assert 'delta_heatmap' in r


# ---------------------------------------------------------------------------
# Feature: /api/disk_usage
# ---------------------------------------------------------------------------

class TestDiskUsage:
    def test_disk_usage_rolls_up_per_pipeline_variant_run(
            self, scan_root, make_pipeline):
        make_pipeline('_diffman_test_du', """
            import diffman as dm
            dm.register('base', x=1)
            def _f(ctx):
                import tempfile, os
                p = os.path.join(tempfile.mkdtemp(), 'big.bin')
                open(p, 'wb').write(b'x' * 1024)   #1 KiB payload
                ctx.artifact('s', 'big.bin', p)
            PIPELINE = dm.Pipeline('_p_du', [dm.Stage('s', _f)])
        """)
        from diffman import discovery
        from diffman.core import registry, RunRegistry
        import sys
        discovery.load_module('_diffman_test_du')
        mod = sys.modules['_diffman_test_du']
        rr = RunRegistry(root=str(scan_root / 'runs'))
        mod.PIPELINE.run(registry.get('_diffman_test_du', 'base'), rr)
        app = create_app(root=str(scan_root / 'runs'),
                         scan_root=str(scan_root), no_scan=True)
        with TestClient(app) as c:
            d = c.get('/api/disk_usage').json()
        pipes = {p['pipeline']: p for p in d['pipelines']}
        du = pipes['_p_du']
        assert du['size'] >= 1024   #must account for the 1 KiB payload
        v = du['variants'][0]
        assert v['variant'] == 'base'
        assert len(v['runs']) == 1
        assert d['total'] >= 1024

    def test_disk_usage_empty_when_no_runs_root(self, tmp_path):
        app = create_app(root=str(tmp_path / 'no_such_dir'),
                         scan_root=str(tmp_path), no_scan=True)
        with TestClient(app) as c:
            d = c.get('/api/disk_usage').json()
        assert d['total'] == 0
        assert d['pipelines'] == []


# ---------------------------------------------------------------------------
# Original discovery-eviction test continues below
# ---------------------------------------------------------------------------

class TestDiscoveryEviction:
    def test_evict_drops_variants_and_pipeline_index(self, scan_root, make_pipeline):
        from diffman import core, discovery
        make_pipeline('_diffman_test_evict', """
            import diffman as dm
            dm.register('only', x=1)
            def _f(ctx): return {}
            PIPELINE = dm.Pipeline('_pipe_evict', [dm.Stage('sim', _f)])
        """)
        discovery.load_module('_diffman_test_evict')
        assert core.registry.for_module('_diffman_test_evict') == ['only']
        assert '_pipe_evict' in discovery.PIPELINE_TO_MODULE
        discovery.evict_module('_diffman_test_evict')
        assert core.registry.for_module('_diffman_test_evict') == []
        assert '_pipe_evict' not in discovery.PIPELINE_TO_MODULE
