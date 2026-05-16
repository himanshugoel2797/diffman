"""Direct tests for renderers.render() dispatch + per-type payloads.

The /api/render endpoint indirectly covers some of this, but the
dispatch decisions (extension routing, optional-dep fallbacks, error
payloads) deserve unit-level coverage so failures point at the right
file. test_regressions covers the 1-D decimation x-axis path.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from diffman import renderers


# ---------------------------------------------------------------------------
# Dispatch + error payloads
# ---------------------------------------------------------------------------

class TestRenderDispatch:
    def test_missing_file_returns_error_kind(self, tmp_path):
        out = renderers.render(str(tmp_path / 'nope.txt'))
        assert out['kind'] == 'error'

    def test_unknown_extension_returns_binary(self, tmp_path):
        p = tmp_path / 'mystery.xyz'
        p.write_bytes(b'\x00\x01\x02')
        out = renderers.render(str(p))
        assert out['kind'] == 'binary'

    def test_unhandled_exception_caught_and_returned_as_error(self, tmp_path,
                                                              monkeypatch):
        """The dispatch try/except must catch unexpected loader failures
        so a bad file in one stage doesn't 500 the whole UI."""
        p = tmp_path / 'a.json'
        p.write_text('{}')   #valid JSON
        def boom(*a, **kw): raise RuntimeError('synthetic')
        monkeypatch.setattr(renderers, '_render_json', boom)
        out = renderers.render(str(p))
        assert out['kind'] == 'error'
        assert 'synthetic' in out['data']


# ---------------------------------------------------------------------------
# JSON renderer
# ---------------------------------------------------------------------------

class TestRenderJson:
    def test_valid_json_returns_parsed_object(self, tmp_path):
        p = tmp_path / 'c.json'
        p.write_text(json.dumps({'k': 1, 'nested': {'v': 'x'}}))
        out = renderers.render(str(p))
        assert out['kind'] == 'json'
        assert out['data'] == {'k': 1, 'nested': {'v': 'x'}}

    def test_malformed_json_falls_back_to_text(self, tmp_path):
        p = tmp_path / 'broken.json'
        p.write_text('{not valid')
        out = renderers.render(str(p))
        assert out['kind'] == 'text'
        assert '{not valid' in out['data']


# ---------------------------------------------------------------------------
# Text renderer truncation
# ---------------------------------------------------------------------------

class TestRenderText:
    def test_text_returns_full_content_when_under_cap(self, tmp_path):
        p = tmp_path / 'small.txt'
        p.write_text('hello world')
        out = renderers.render(str(p))
        assert out['kind'] == 'text'
        assert out['data'] == 'hello world'
        assert out['meta']['truncated'] is False

    def test_text_truncates_and_flags_when_over_cap(self, tmp_path):
        p = tmp_path / 'huge.log'
        p.write_text('x' * 5000)
        out = renderers.render(str(p), max_bytes=1024)
        assert out['kind'] == 'text'
        assert len(out['data']) == 1024
        assert out['meta']['truncated'] is True


# ---------------------------------------------------------------------------
# Numpy renderer + 2-D heatmap path (1-D is in test_regressions)
# ---------------------------------------------------------------------------

class TestRenderNpy:
    def test_2d_returns_plot_2d_with_stats(self, tmp_path):
        p = tmp_path / 'a.npy'
        np.save(p, np.arange(20, dtype=float).reshape(4, 5))
        out = renderers.render(str(p))
        assert out['kind'] == 'plot_2d'
        assert out['meta']['stats']['min'] == 0.0
        assert out['meta']['stats']['max'] == 19.0

    def test_2d_with_non_finite_falls_back_gracefully(self, tmp_path):
        p = tmp_path / 'nans.npy'
        np.save(p, np.full((3, 3), np.nan))
        out = renderers.render(str(p))
        assert out['kind'] == 'plot_2d'
        assert out['meta']['stats']['min'] is None

    def test_2d_huge_array_gets_downsampled(self, tmp_path):
        p = tmp_path / 'huge.npy'
        np.save(p, np.ones((2048, 2048), dtype=np.float32))
        out = renderers.render(str(p))
        assert out['kind'] == 'plot_2d'
        # downsampling factor recorded as [sy, sx]
        assert 'downsampled' in out['meta']
        assert max(out['meta']['downsampled']) >= 4

    def test_3d_array_shows_first_slice_with_note(self, tmp_path):
        p = tmp_path / 'cube.npy'
        np.save(p, np.arange(24, dtype=float).reshape(2, 3, 4))
        out = renderers.render(str(p))
        # Recursion bottoms out at the 2-D slice.
        assert out['kind'] == 'plot_2d'
        assert 'note' in out['meta']

    def test_non_numeric_2d_returns_array_summary(self, tmp_path):
        p = tmp_path / 'strs.npy'
        np.save(p, np.array([['a', 'b'], ['c', 'd']]), allow_pickle=False)
        out = renderers.render(str(p))
        assert out['kind'] == 'array_summary'

    def test_npy_without_numpy_returns_binary(self, tmp_path, monkeypatch):
        p = tmp_path / 'a.npy'
        np.save(p, np.array([1.0, 2.0]))
        monkeypatch.setattr(renderers, '_HAS_NUMPY', False)
        out = renderers.render(str(p))
        assert out['kind'] == 'binary'


# ---------------------------------------------------------------------------
# HDF5 renderer + dataset preview
# ---------------------------------------------------------------------------

class TestRenderH5:
    @pytest.fixture(autouse=True)
    def _require_h5py(self):
        pytest.importorskip('h5py')

    def test_h5_returns_tree_of_groups_and_datasets(self, tmp_path):
        import h5py
        p = tmp_path / 'tree.h5'
        with h5py.File(p, 'w') as f:
            g = f.create_group('group1')
            g.create_dataset('ds', data=np.arange(10))
            f.create_dataset('top', data=np.ones((2, 3)))
        out = renderers.render(str(p))
        assert out['kind'] == 'h5_tree'
        kinds = {e['name']: e['kind'] for e in out['data']}
        # Both the group and its child dataset should be listed.
        assert kinds.get('group1') == 'group'
        assert kinds.get('group1/ds') == 'dataset'
        assert kinds.get('top') == 'dataset'

    def test_render_dataset_returns_payload_for_known_dataset(self, tmp_path):
        import h5py
        p = tmp_path / 'pick.h5'
        with h5py.File(p, 'w') as f:
            f.create_dataset('arr', data=np.arange(20, dtype=float))
        out = renderers.render_h5_dataset(str(p), 'arr')
        assert out['kind'] == 'plot_1d'
        assert len(out['data']['y']) == 20

    def test_render_dataset_404_payload_for_missing_dataset(self, tmp_path):
        import h5py
        p = tmp_path / 'pick.h5'
        with h5py.File(p, 'w') as f:
            f.create_dataset('arr', data=np.zeros(3))
        out = renderers.render_h5_dataset(str(p), 'ghost')
        assert out['kind'] == 'error'
        assert 'ghost' in out['data']

    def test_render_dataset_returns_scalar_for_0d_array(self, tmp_path):
        import h5py
        p = tmp_path / 'scalar.h5'
        with h5py.File(p, 'w') as f:
            f.create_dataset('x', data=np.float64(3.14))
        out = renderers.render_h5_dataset(str(p), 'x')
        assert out['kind'] == 'scalar'
        assert abs(out['data'] - 3.14) < 1e-9

    def test_render_dataset_decimates_long_1d(self, tmp_path):
        import h5py
        p = tmp_path / 'big.h5'
        with h5py.File(p, 'w') as f:
            f.create_dataset('arr', data=np.arange(1_000_000, dtype=float))
        out = renderers.render_h5_dataset(str(p), 'arr', max_points=10_000)
        assert out['kind'] == 'plot_1d'
        assert 'decimated_stride' in out['meta']

    def test_h5_without_h5py_returns_binary(self, tmp_path, monkeypatch):
        p = tmp_path / 'a.h5'; p.write_bytes(b'')
        monkeypatch.setattr(renderers, '_HAS_H5PY', False)
        out = renderers.render(str(p))
        assert out['kind'] == 'binary'

    def test_render_dataset_without_h5py_returns_error(self, tmp_path,
                                                      monkeypatch):
        monkeypatch.setattr(renderers, '_HAS_H5PY', False)
        out = renderers.render_h5_dataset(str(tmp_path / 'x.h5'), 'foo')
        assert out['kind'] == 'error'


# ---------------------------------------------------------------------------
# Image renderer
# ---------------------------------------------------------------------------

class TestRenderImage:
    def test_image_extension_returns_image_kind_with_size_meta(self, tmp_path):
        PIL = pytest.importorskip('PIL.Image')
        p = tmp_path / 'pic.png'
        PIL.new('RGB', (24, 16), color=(255, 0, 0)).save(p)
        out = renderers.render(str(p))
        assert out['kind'] == 'image'
        assert out['meta']['width'] == 24
        assert out['meta']['height'] == 16
        # The raw image is served via /artifact/, not embedded.
        assert out['data'] is None
