"""Sanity tests for diffman.ptypy_loaders against a minimal synthetic .ptyr."""

import os

import h5py
import numpy as np
import pytest

from diffman import ptypy_loaders as pl


@pytest.fixture
def fake_ptyr(tmp_path):
    """Write a minimal but realistic .ptyr — one obj + one probe storage
    with two modes, plus a short iter_info trail."""
    p = tmp_path / 'fake.ptyr'
    ny, nx = 16, 20
    obj = (np.random.rand(1, ny, nx) +
           1j * np.random.rand(1, ny, nx)).astype(np.complex64)
    prb = (np.random.rand(2, ny, nx) +
           1j * np.random.rand(2, ny, nx)).astype(np.complex64)
    with h5py.File(p, 'w') as f:
        og = f.create_group('content/obj/SMFG00')
        og['data'] = obj
        og['_psize'] = np.array([5e-9, 5e-9])
        og['_origin'] = np.array([-1e-7, -1e-7])
        pg = f.create_group('content/probe/SMFG00')
        pg['data'] = prb
        pg['_psize'] = np.array([5e-9, 5e-9])
        pg['_origin'] = np.array([-2e-7, -2e-7])
        for i in range(3):
            r = f.create_group(f'content/runtime/iter_info/{i:05d}')
            r['iteration'] = np.int64(i * 50)
            r['error'] = np.array([1.0 / (i + 1), 0.0, 2.0 / (i + 1)])
            r['duration'] = np.float64(0.1 + i)
    return str(p)


def test_is_ptyr_file(fake_ptyr, tmp_path):
    assert pl.is_ptyr_file(fake_ptyr) is True
    assert pl.is_ptyr_file(str(tmp_path / 'nope.ptyr')) is False
    plain = tmp_path / 'plain.h5'
    with h5py.File(plain, 'w') as f:
        f['a'] = [1, 2, 3]
    assert pl.is_ptyr_file(str(plain)) is False


def test_summarize(fake_ptyr):
    s = pl.summarize(fake_ptyr)
    assert 'error' not in s
    assert {x['id'] for x in s['storages']['obj']} == {'SMFG00'}
    assert s['storages']['probe'][0]['shape'] == [2, 16, 20]
    assert s['storages']['probe'][0]['psize'] == [5e-9, 5e-9]
    ii = s['iter_info']
    assert ii['iteration'] == [0, 50, 100]
    assert ii['error_fourier'][0] == pytest.approx(1.0)
    assert ii['error_overlap'][0] == pytest.approx(2.0)


@pytest.mark.parametrize('repr_,shape_check', [
    ('amplitude', True), ('phase', True),
    ('real', True), ('imag', True), ('intensity', True),
])
def test_project_obj(fake_ptyr, repr_, shape_check):
    out = pl.project(fake_ptyr, kind='obj', repr_=repr_)
    assert 'error' not in out, out
    assert out['data'].shape == (16, 20)
    assert out['meta']['kind'] == 'obj'
    assert out['meta']['repr'] == repr_
    assert out['meta']['mode'] == 0
    assert out['meta']['nmodes'] == 1


def test_project_probe_mode_clamped(fake_ptyr):
    out = pl.project(fake_ptyr, kind='probe', mode=99)
    assert out['meta']['mode'] == 1   # clamped to nmodes-1
    assert out['meta']['nmodes'] == 2


def test_project_rejects_garbage(fake_ptyr):
    assert 'error' in pl.project(fake_ptyr, kind='bogus')
    assert 'error' in pl.project(fake_ptyr, repr_='ultraviolet')


def test_downsample_and_cuts(fake_ptyr):
    out = pl.project(fake_ptyr, kind='obj', repr_='amplitude')
    a2, (sy, sx) = pl.downsample(out['data'], target_max=8)
    assert max(a2.shape) <= 8
    assert sy >= 2 and sx >= 2
    c = pl.cuts(a2, row=-1, col=-1)
    assert len(c['h']) == a2.shape[1]
    assert len(c['v']) == a2.shape[0]
