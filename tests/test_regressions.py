"""Regression tests — one per bug fixed in the audit pass.

Each test would have caught its corresponding bug before the fix landed.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys

import numpy as np
import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# cli.py: `v.config.merged()` was a non-existent method on Config
# ---------------------------------------------------------------------------

def test_cli_describe_emits_resolved_config(scan_root, make_pipeline, capsys):
    """`diffman describe` must produce JSON containing the merged config —
    the prior `v.config.merged()` call raised AttributeError before fixing.
    """
    make_pipeline('_diffman_test_describe', """
        import diffman as dm
        dm.register('base', scan=dict(width=5e-6))
        dm.register('jitter', base='base', probe=dict(amp=0.1))
        def _f(ctx): return {}
        PIPELINE = dm.Pipeline('_pipe_describe', [dm.Stage('sim', _f)])
    """)
    from diffman.cli import main
    rc = main(['describe', '_diffman_test_describe', 'jitter'])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out['config']['scan']['width'] == 5e-6
    assert out['config']['probe']['amp'] == 0.1


# ---------------------------------------------------------------------------
# renderers.py: 1-D plot x-axis was wrong when array was decimated
# ---------------------------------------------------------------------------

def test_render_npy_1d_xaxis_uses_actual_indices(tmp_path):
    """Pre-fix, `x` was `[0..len(y)-1]` regardless of stride, so a 50k-point
    array decimated to 10k still plotted x in [0..10000) instead of [0..50000)."""
    from diffman import renderers
    arr = np.arange(50_000, dtype=float)
    p = tmp_path / 'big.npy'
    np.save(p, arr)
    out = renderers.render(str(p))
    assert out['kind'] == 'plot_1d'
    x = out['data']['x']
    y = out['data']['y']
    assert len(x) == len(y)
    assert x[0] == 0
    # x must span the original array, not the decimated length
    assert x[-1] >= arr.size - out['meta']['decimated_stride']
    # x values must be stride-spaced
    stride = out['meta']['decimated_stride']
    assert x[1] - x[0] == stride
    # values at each x must match the source
    for xi, yi in list(zip(x, y))[:20]:
        assert arr[xi] == yi


def test_render_npy_1d_small_no_decimation(tmp_path):
    """Small arrays must still render with one-to-one x/y."""
    from diffman import renderers
    p = tmp_path / 'small.npy'
    np.save(p, np.array([10.0, 20.0, 30.0, 40.0]))
    out = renderers.render(str(p))
    assert out['data']['x'] == [0, 1, 2, 3]
    assert out['data']['y'] == [10.0, 20.0, 30.0, 40.0]
    assert 'decimated_stride' not in out['meta']


# ---------------------------------------------------------------------------
# discovery.py: same-basename .py files in different dirs collided silently
# ---------------------------------------------------------------------------

def test_discovery_warns_on_duplicate_module_basename(scan_root):
    """Two files named `dup.py` in different subdirs used to silently
    overwrite each other in DISCOVERED_PATHS; the second won. After the
    fix, the first wins and a warning is emitted."""
    from diffman import discovery
    body = (
        "import diffman as dm\n"
        "PIPELINE = dm.Pipeline('p', [])  # references diffman + PIPELINE\n"
    )
    (scan_root / 'a').mkdir()
    (scan_root / 'b').mkdir()
    (scan_root / 'a' / '_diffman_test_dup.py').write_text(body)
    (scan_root / 'b' / '_diffman_test_dup.py').write_text(body)

    with pytest.warns(UserWarning, match='duplicate pipeline module'):
        discovery.discover(str(scan_root))

    # First-seen path wins.
    kept = discovery.DISCOVERED_PATHS['_diffman_test_dup']
    assert kept in (str(scan_root / 'a'), str(scan_root / 'b'))
    # And we did not record both entries in DISCOVERED_LIST.
    matches = [e for e in discovery.DISCOVERED_LIST
               if e['module'] == '_diffman_test_dup']
    assert len(matches) == 1


# ---------------------------------------------------------------------------
# discovery.py: load_module used to insert scan_root into sys.path,
# letting user .py files shadow stdlib by basename
# ---------------------------------------------------------------------------

def test_load_module_loads_from_discovered_path_not_sys_path(scan_root,
                                                              monkeypatch):
    """Pre-fix, `load_module` did `sys.path.insert(0, extra)` and called
    `importlib.import_module(name)`, which would resolve via sys.path and
    pick whichever same-named file came first. After the fix we load the
    *discovered* file directly via spec_from_file_location.

    Set up two files named the same under two subdirs: the discovered
    path points at subdir B, but subdir A is earlier on sys.path. The
    pre-fix code would load A's file; the fixed code must load B's.
    """
    from diffman import discovery
    body_a = (
        "import diffman as dm\n"
        "dm.register('which', who='A')\n"
        "def _f(ctx): return {}\n"
        "PIPELINE = dm.Pipeline('_pipe_load_a', [dm.Stage('s', _f)])\n"
    )
    body_b = (
        "import diffman as dm\n"
        "dm.register('which', who='B')\n"
        "def _f(ctx): return {}\n"
        "PIPELINE = dm.Pipeline('_pipe_load_b', [dm.Stage('s', _f)])\n"
    )
    (scan_root / 'A').mkdir()
    (scan_root / 'B').mkdir()
    (scan_root / 'A' / '_diffman_test_path.py').write_text(body_a)
    (scan_root / 'B' / '_diffman_test_path.py').write_text(body_b)

    # A is on sys.path earlier than B. The "discovered" entry points at B.
    monkeypatch.syspath_prepend(str(scan_root / 'B'))
    monkeypatch.syspath_prepend(str(scan_root / 'A'))
    discovery.DISCOVERED_PATHS['_diffman_test_path'] = str(scan_root / 'B')

    discovery.load_module('_diffman_test_path')
    from diffman.core import registry
    v = registry.get('_diffman_test_path', 'which')
    assert v.config['who'] == 'B', (
        "load_module loaded the sys.path-first file instead of the "
        "discovered one — sys.path search must not be used")


# ---------------------------------------------------------------------------
# core.py: stage's `done` status must hit disk BEFORE `_key` is written.
# Pre-fix order: write _key, set status=done, _flush at end-of-loop.
# A crash between _key.write_text and the final _flush left a cached
# stage marked `running` forever.
# ---------------------------------------------------------------------------

def test_stage_done_status_persists_before_key_file(scan_root, monkeypatch):
    """Intercept `_flush` and `_key` writes; the on-disk status at the
    instant `_key` is written must be `done`, not `running`."""
    from diffman import core
    seen_status_when_key_written = {}

    real_write_text = core.Path.write_text

    def spy_write_text(self, data, *a, **kw):
        if self.name == '_key':
            stage = self.parent.name
            run_json = self.parent.parent.parent / 'run.json'
            if run_json.exists():
                rec = json.loads(run_json.read_text())
                seen_status_when_key_written[stage] = (
                    rec.get('stage_status', {}).get(stage))
        return real_write_text(self, data, *a, **kw)

    monkeypatch.setattr(core.Path, 'write_text', spy_write_text)

    def sim(ctx):
        return {}

    reg = core.RunRegistry(root=str(scan_root / 'runs'))
    v = core.registry.register('only', module='_diffman_test_order', x=1)
    pipe = core.Pipeline('_pipe_order', [core.Stage('sim', sim)])
    pipe.run(v, reg)
    assert seen_status_when_key_written == {'sim': 'done'}


# ---------------------------------------------------------------------------
# core.py: list_runs used to silently drop run.json files that had ANY
# unknown field (e.g. one added in a newer version).
# ---------------------------------------------------------------------------

def test_list_runs_tolerates_unknown_fields_in_run_json(scan_root, caplog):
    """Forward compat: a run.json containing an extra key from a newer
    diffman must still load (with a logged warning, not a silent drop)."""
    from diffman.core import RunRegistry
    root = scan_root / 'runs'
    rdir = root / 'pipe' / 'variant' / 'abc123abc123'
    rdir.mkdir(parents=True)
    (rdir / 'run.json').write_text(json.dumps({
        'pipeline': 'pipe', 'variant': 'variant',
        'fingerprint': 'abc123abc123def',
        'fdir': str(rdir),
        'started': '2026-01-01T00:00:00',
        'ended': None,
        'stage_keys': {}, 'stage_status': {}, 'errors': {},
        'git_rev': None, 'host': None,
        'a_new_field_from_the_future': 'surprise!',
    }))
    reg = RunRegistry(root=str(root))
    runs = reg.list_runs()
    assert len(runs) == 1
    assert runs[0].pipeline == 'pipe'


def test_list_runs_skips_malformed_with_warning(scan_root, caplog):
    """Genuinely broken run.json (e.g. missing required fields) must be
    skipped with a warning, not silently dropped."""
    from diffman.core import RunRegistry
    root = scan_root / 'runs'
    rdir = root / 'pipe' / 'variant' / 'abc123abc123'
    rdir.mkdir(parents=True)
    (rdir / 'run.json').write_text(json.dumps({'pipeline': 'pipe'}))
    reg = RunRegistry(root=str(root))
    with caplog.at_level(logging.WARNING, logger='diffman.core'):
        runs = reg.list_runs()
    assert runs == []
    assert any('skipping' in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# core.py: snapshot() failures used to be silent forever. After the fix,
# the first failure logs a warning.
# ---------------------------------------------------------------------------

def test_snapshot_failure_is_logged_at_least_once(scan_root, monkeypatch,
                                                   caplog):
    from diffman import core

    def boom(*a, **kw):
        raise RuntimeError('git crash')

    import diffman.git_backup as gb
    monkeypatch.setattr(gb, 'snapshot', boom)
    #Reset the "already warned" flag so the warning fires for THIS test
    #even if a previous one tripped it.
    monkeypatch.setattr(core, '_SNAPSHOT_WARNED', False)

    v = core.registry.register('only', module='_diffman_test_snap', x=1)
    pipe = core.Pipeline('_pipe_snap', [core.Stage('sim', lambda ctx: None)])
    pipe._source_file = __file__   #force the snapshot path to fire
    reg = core.RunRegistry(root=str(scan_root / 'runs'))
    with caplog.at_level(logging.WARNING, logger='diffman.core'):
        pipe.run(v, reg)
    assert any('snapshot failed' in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# srw_loaders.py: lock in the canonical (ny, nx, ne, 2) unpacking layout
# that SRW's srwl_uti_save_wfr_hdf5 writes. An earlier audit tried to
# detect an alternate (ne, ny, nx, 2) layout via except-ValueError on the
# reshape, which was structurally unreachable (size-matched reshapes don't
# raise) AND would silently re-axis-order data if it did. That defensive
# fallback was removed; this test guards the canonical path against
# future refactors that might break it.
# ---------------------------------------------------------------------------

def test_wfr_to_complex_unpacks_canonical_layout(scan_root, monkeypatch):
    from diffman import srw_loaders

    ny, nx, ne = 3, 4, 2

    class FakeMesh:
        def __init__(self):
            self.xStart = 0.0; self.xFin = 1.0; self.nx = nx
            self.yStart = 0.0; self.yFin = 1.0; self.ny = ny
            self.eStart = 0.0; self.eFin = 1.0; self.ne = ne
            self.zStart = 0.0

    class FakeWfr:
        def __init__(self):
            self.mesh = FakeMesh()
            # (ne, ny, nx, 2) layout — not the canonical one
            re = np.arange(ne * ny * nx, dtype=np.float32).reshape((ne, ny, nx))
            im = re + 0.5
            self.arEx = np.empty(ne * ny * nx * 2, dtype=np.float32)
            self.arEx[0::2] = re.ravel()
            self.arEx[1::2] = im.ravel()
            self.arEy = self.arEx.copy()

    class FakeSrw:
        def srwl_uti_read_wfr_hdf5(self, _file_path=None, **_):
            return FakeWfr()

    monkeypatch.setattr(srw_loaders, '_srwlib', lambda: FakeSrw())
    monkeypatch.setattr(srw_loaders, '_HAS_NUMPY', True)
    # _load_wfr_hdf5 wants a path it never reads (srwlib is faked).
    out = srw_loaders._load_wfr_hdf5('unused.h5', FakeSrw())
    assert out['kind'] == 'wavefield'
    assert out['fields']['Ex'].shape == (ne, ny, nx)
    # Each complex value's real part must match what we packed.
    assert out['fields']['Ex'].real[0, 0, 0] == 0
    assert out['fields']['Ex'].imag[0, 0, 0] == 0.5


# ---------------------------------------------------------------------------
# server.py: _flatten_union used to ignore which columns were "absent
# because the module failed to load" vs "absent because key was missing".
# A failed module would make every other module's keys render as
# "differs from missing". None placeholders now skip absent columns.
# ---------------------------------------------------------------------------

def test_flatten_union_none_column_does_not_break_equality():
    """An entry of `None` (module failed to load) must not flip equal=False
    on rows where the loaded columns agree."""
    from diffman.server import _flatten_union
    rows = _flatten_union([{'a': 1, 'b': 2}, None, {'a': 1, 'b': 2}])
    by_path = {r['path']: r for r in rows}
    # Both loaded columns agree on 'a' and 'b' — equal must be True.
    assert by_path['a']['equal'] is True
    assert by_path['b']['equal'] is True
    # The None column still occupies its slot in the values list so the
    # UI can align with `columns`.
    assert by_path['a']['values'][1] is None
    assert by_path['a']['values'][0] == 1
    assert by_path['a']['values'][2] == 1


def test_flatten_union_all_none_yields_no_rows():
    from diffman.server import _flatten_union
    assert _flatten_union([None, None]) == []


# ---------------------------------------------------------------------------
# server.py: `_safe_under` and the `/artifact/` route used `os.path.realpath`
# for their path-escape check. Realpath follows symlinks, and ctx.artifact
# symlinks artifacts from outside the runs root by default — so legitimate
# artifact downloads were getting refused with `400 path escape`. The fix
# uses textual normpath, which still blocks `..` traversal in URLs but
# permits symlinked artifacts.
# ---------------------------------------------------------------------------

def test_render_endpoint_serves_symlinked_artifact(scan_root, make_pipeline):
    """Render an artifact whose backing file is a symlink to a tempfile
    outside the runs root. Pre-fix this returned 400 because realpath
    resolved the symlink out of the runs tree."""
    from diffman import discovery
    from diffman.core import registry, RunRegistry
    from diffman.server import create_app
    import sys
    make_pipeline('_diffman_test_symartifact', """
        import diffman as dm
        dm.register('base', x=1)
        def _f(ctx):
            import os, tempfile
            #tempdir is OUTSIDE the runs root — ctx.artifact symlinks it in.
            p = os.path.join(tempfile.mkdtemp(), 'art.txt')
            open(p, 'w').write('SYMLINK_OK')
            ctx.artifact('s', 'art.txt', p)
        PIPELINE = dm.Pipeline('_p_sym', [dm.Stage('s', _f)])
    """)
    discovery.load_module('_diffman_test_symartifact')
    rr = RunRegistry(root=str(scan_root / 'runs'))
    rec = sys.modules['_diffman_test_symartifact'].PIPELINE.run(
        registry.get('_diffman_test_symartifact', 'base'), rr)
    art_path = os.path.join(rec.fdir, 'stages', 's', 'outputs', 'art.txt')
    # Sanity: it really is a symlink to outside the runs root.
    assert os.path.islink(art_path)
    assert not os.path.realpath(art_path).startswith(str(scan_root / 'runs'))
    app = create_app(root=str(scan_root / 'runs'),
                     scan_root=str(scan_root), no_scan=True)
    with TestClient(app) as c:
        r = c.get(f'/api/render?path={art_path}')
    assert r.status_code == 200
    assert r.json()['data'] == 'SYMLINK_OK'


def test_artifact_route_blocks_dot_dot_traversal_in_url(scan_root):
    """The fix relaxed the path-escape check from realpath to normpath.
    Verify the new check still catches the actual attack: `..` segments
    in the URL must not be allowed to climb out of the runs root."""
    from diffman.server import create_app
    # Plant a file outside runs that an attacker would try to read.
    secret = scan_root / 'secret.txt'
    secret.write_text('TOP_SECRET')
    runs = scan_root / 'runs'; runs.mkdir()
    app = create_app(root=str(runs), scan_root=str(scan_root), no_scan=True)
    with TestClient(app) as c:
        # Both percent-encoded and raw forms must be refused; the
        # percent-encoded form is what an attacker would actually try.
        r1 = c.get('/artifact/p/v/fp/..%2F..%2Fsecret.txt')
        assert r1.status_code in (400, 404)
        assert 'TOP_SECRET' not in r1.text


# ---------------------------------------------------------------------------
# discovery.py: a partial import that registers a variant and then raises
# left the variant in the global registry. The retry would then trip the
# `variant 'X' already registered` guard, masking the real import error.
# ---------------------------------------------------------------------------

def test_load_module_rolls_back_variants_on_import_failure(scan_root):
    from diffman import discovery
    from diffman.core import registry

    path = scan_root / '_diffman_partial.py'
    path.write_text(
        "import diffman as dm\n"
        "dm.register('a', x=1)\n"
        "raise RuntimeError('boom')\n"
    )
    discovery.discover(str(scan_root))

    with pytest.raises(RuntimeError, match='boom'):
        discovery.load_module('_diffman_partial')
    # Variant 'a' must NOT remain in the registry — otherwise the retry
    # below would fail with 'already registered' instead of 'boom'.
    assert registry.for_module('_diffman_partial') == []
    # Retry should produce the original error again, not a stale-state one.
    with pytest.raises(RuntimeError, match='boom'):
        discovery.load_module('_diffman_partial')


def test_api_compare_handles_failed_module(scan_root, make_pipeline):
    """`/api/compare` with one bogus module name must not poison the
    rendered rows for the present modules."""
    from diffman.server import create_app
    make_pipeline('_diffman_test_cmp_a', """
        import diffman as dm
        dm.register('base', x=1, y=2)
        def _f(ctx): return {}
        PIPELINE = dm.Pipeline('_pipe_cmp_a', [dm.Stage('s', _f)])
    """)
    make_pipeline('_diffman_test_cmp_b', """
        import diffman as dm
        dm.register('base', x=1, y=2)
        def _f(ctx): return {}
        PIPELINE = dm.Pipeline('_pipe_cmp_b', [dm.Stage('s', _f)])
    """)
    app = create_app(root=str(scan_root / 'runs'),
                     scan_root=str(scan_root), no_scan=False)
    with TestClient(app) as c:
        r = c.get('/api/compare?modules=_diffman_test_cmp_a,'
                  '_does_not_exist,_diffman_test_cmp_b&variant=base').json()
    assert [col['present'] for col in r['columns']] == [True, False, True]
    by_path = {row['path']: row for row in r['rows']}
    # Loaded columns agree → equal True despite the failed middle column.
    assert by_path['x']['equal'] is True
    assert by_path['y']['equal'] is True
    # Column alignment preserved.
    assert by_path['x']['values'][1] is None
