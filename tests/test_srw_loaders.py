"""Unit tests for srw_loaders: is_srw_file sniffing, project, cuts, downsample.

The real SRW reader is mocked — we just need to verify the post-load
projection / utility math is correct. The `_load_wfr_hdf5` canonical
path is already covered in test_regressions.
"""

from __future__ import annotations

import os

import numpy as np
import pytest

from diffman import srw_loaders


# ---------------------------------------------------------------------------
# is_srw_file content sniff
# ---------------------------------------------------------------------------

class TestIsSrwFile:
    def test_returns_false_for_missing_file(self, tmp_path):
        assert srw_loaders.is_srw_file(str(tmp_path / 'nope.dat')) is False

    def test_returns_false_for_unknown_extension(self, tmp_path):
        p = tmp_path / 'a.npy'; p.write_bytes(b'')
        assert srw_loaders.is_srw_file(str(p)) is False

    def test_recognizes_srw_dat_intensity_header(self, tmp_path):
        p = tmp_path / 'i.dat'
        p.write_text('# Intensity\n# Horizontal Position [m]\n1 2 3\n')
        assert srw_loaders.is_srw_file(str(p)) is True

    def test_recognizes_srw_dat_photon_energy_header(self, tmp_path):
        p = tmp_path / 'p.dat'
        p.write_text('# Photon Energy [eV]\n1 2 3\n')
        assert srw_loaders.is_srw_file(str(p)) is True

    def test_rejects_plain_dat_without_srw_keywords(self, tmp_path):
        p = tmp_path / 'plain.dat'
        p.write_text('# Some other ASCII\n1 2 3\n')
        assert srw_loaders.is_srw_file(str(p)) is False

    def test_recognizes_srw_h5_intensity_group(self, tmp_path):
        h5py = pytest.importorskip('h5py')
        p = tmp_path / 'i.h5'
        with h5py.File(p, 'w') as f:
            f.create_group('intensity')
        assert srw_loaders.is_srw_file(str(p)) is True

    def test_recognizes_srw_h5_wfr_group(self, tmp_path):
        h5py = pytest.importorskip('h5py')
        p = tmp_path / 'w.h5'
        with h5py.File(p, 'w') as f:
            f.create_group('wfr')
        assert srw_loaders.is_srw_file(str(p)) is True

    def test_rejects_h5_with_no_srw_groups(self, tmp_path):
        h5py = pytest.importorskip('h5py')
        p = tmp_path / 'other.h5'
        with h5py.File(p, 'w') as f:
            f.create_dataset('whatever', data=[1, 2, 3])
        assert srw_loaders.is_srw_file(str(p)) is False


# ---------------------------------------------------------------------------
# downsample
# ---------------------------------------------------------------------------

class TestDownsample:
    def test_2d_within_target_returns_unchanged_and_unit_stride(self):
        a = np.arange(64, dtype=float).reshape(8, 8)
        out, (sy, sx) = srw_loaders.downsample(a, target_max=16)
        assert (sy, sx) == (1, 1)
        assert out.shape == a.shape

    def test_2d_exceeding_target_decimates(self):
        a = np.arange(4096, dtype=float).reshape(64, 64)
        out, (sy, sx) = srw_loaders.downsample(a, target_max=16)
        assert sy >= 4 and sx >= 4
        assert max(out.shape) <= 16

    def test_non_2d_input_is_returned_as_is(self):
        a = np.arange(10, dtype=float)
        out, strides = srw_loaders.downsample(a, target_max=4)
        assert strides == (1, 1)
        assert out.shape == a.shape


# ---------------------------------------------------------------------------
# cuts
# ---------------------------------------------------------------------------

class TestCuts:
    def test_default_row_col_uses_center(self):
        a = np.arange(25, dtype=float).reshape(5, 5)
        out = srw_loaders.cuts(a)
        # 5//2 = 2 — center row/col
        assert out['row'] == 2 and out['col'] == 2
        assert out['h'] == a[2].tolist()
        assert out['v'] == a[:, 2].tolist()

    def test_explicit_row_col(self):
        a = np.arange(25, dtype=float).reshape(5, 5)
        out = srw_loaders.cuts(a, row=0, col=4)
        assert out['row'] == 0 and out['col'] == 4
        assert out['h'] == a[0].tolist()
        assert out['v'] == a[:, 4].tolist()

    def test_out_of_range_row_falls_back_to_center(self):
        a = np.zeros((3, 3))
        out = srw_loaders.cuts(a, row=99, col=99)
        assert out['row'] == 1 and out['col'] == 1   # center


# ---------------------------------------------------------------------------
# project — synthesize a wavefield by hand and exercise each branch
# ---------------------------------------------------------------------------

def _fake_wavefield(ne=2, ny=3, nx=4):
    """Return a `loaded` dict shaped like _load_wfr_hdf5 produces.

    Ex / Ey are (ne, ny, nx) complex arrays with distinct, predictable
    values per polarization so the polarization-selection logic is
    observable in the output.
    """
    base = np.arange(ne * ny * nx, dtype=float).reshape(ne, ny, nx)
    Ex = (base + 0.0) + 1j * (base + 0.5)
    Ey = (base + 10.0) + 1j * (base + 10.5)
    return {
        'kind': 'wavefield',
        'mesh': {'xStart': 0, 'xFin': 1, 'nx': nx,
                 'yStart': 0, 'yFin': 1, 'ny': ny,
                 'eStart': 0, 'eFin': 1, 'ne': ne, 'zStart': 0},
        'fields': {'Ex': Ex, 'Ey': Ey},
        'available': ('intensity', 'amplitude', 'phase', 'real', 'imag'),
    }


class TestProjectIntensityFile:
    def test_intensity_passes_data_through(self):
        loaded = {'kind': 'intensity',
                  'mesh': {'nx': 2, 'ny': 2},
                  'data': np.array([[1.0, 2.0], [3.0, 4.0]])}
        out = srw_loaders.project(loaded, 'intensity')
        assert out['repr'] == 'intensity'
        assert out['data'].tolist() == [[1.0, 2.0], [3.0, 4.0]]


class TestProjectWavefield:
    def test_intensity_sums_polarizations_and_energy(self):
        wf = _fake_wavefield()
        out = srw_loaders.project(wf, 'intensity', polarization='both',
                                  energy_slice=-1)
        Ix = (np.abs(wf['fields']['Ex']) ** 2).sum(axis=0)
        Iy = (np.abs(wf['fields']['Ey']) ** 2).sum(axis=0)
        assert np.allclose(out['data'], Ix + Iy)

    def test_intensity_single_polarization_omits_the_other(self):
        wf = _fake_wavefield()
        out = srw_loaders.project(wf, 'intensity', polarization='Ex',
                                  energy_slice=-1)
        Ix = (np.abs(wf['fields']['Ex']) ** 2).sum(axis=0)
        assert np.allclose(out['data'], Ix)

    def test_amplitude_returns_real_array(self):
        wf = _fake_wavefield()
        out = srw_loaders.project(wf, 'amplitude', polarization='Ex',
                                  energy_slice=-1)
        assert np.iscomplexobj(out['data']) is False
        assert (out['data'] >= 0).all()

    def test_phase_uses_middle_energy_slice_when_unspecified(self):
        wf = _fake_wavefield(ne=3, ny=2, nx=2)
        out = srw_loaders.project(wf, 'phase', polarization='Ex',
                                  energy_slice=-1)
        # Middle slice = index 1.
        expected = np.angle(wf['fields']['Ex'][1])
        assert np.allclose(out['data'], expected)

    def test_real_uses_explicit_energy_slice(self):
        wf = _fake_wavefield(ne=3)
        out = srw_loaders.project(wf, 'real', polarization='Ex',
                                  energy_slice=2)
        assert np.allclose(out['data'], wf['fields']['Ex'][2].real)

    def test_imag_with_ey_polarization(self):
        wf = _fake_wavefield()
        out = srw_loaders.project(wf, 'imag', polarization='Ey',
                                  energy_slice=0)
        assert np.allclose(out['data'], wf['fields']['Ey'][0].imag)

    def test_unknown_repr_returns_error(self):
        wf = _fake_wavefield()
        out = srw_loaders.project(wf, 'bogus', polarization='Ex')
        assert 'error' in out

    def test_propagates_loader_error_for_non_previewable(self):
        out = srw_loaders.project({'error': 'broken'}, 'intensity')
        assert out['error'] == 'broken'


# ---------------------------------------------------------------------------
# load: error paths when SRW or numpy is unavailable
# ---------------------------------------------------------------------------

class TestLoadGuards:
    def test_returns_error_when_numpy_missing(self, monkeypatch):
        monkeypatch.setattr(srw_loaders, '_HAS_NUMPY', False)
        out = srw_loaders.load('whatever.h5')
        assert 'numpy' in out['error']

    def test_returns_error_when_srwlib_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr(srw_loaders, '_srwlib', lambda: None)
        out = srw_loaders.load(str(tmp_path / 'x.h5'))
        assert 'srwlib' in out['error']

    def test_returns_error_for_unknown_extension(self, monkeypatch, tmp_path):
        monkeypatch.setattr(srw_loaders, '_srwlib', lambda: object())
        out = srw_loaders.load(str(tmp_path / 'x.bin'))
        assert 'unknown extension' in out['error']
