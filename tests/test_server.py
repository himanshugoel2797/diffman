"""End-to-end tests for the FastAPI server endpoints."""

from __future__ import annotations

import json
import os

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
    forest = _build_forest(metas)
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
    forest = _build_forest(metas)
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
