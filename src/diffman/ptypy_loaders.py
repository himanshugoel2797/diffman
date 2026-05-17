"""PtyPy `.ptyr` reconstruction loaders for preview.

`.ptyr` is a plain HDF5 container that PtyPy writes from `IO.save()`.
Reading it requires only `h5py`; PtyPy itself is *not* a dependency of
diffman's previewer. The interesting datasets live under:

    content/obj/<sid>/data         (layers, ny, nx) complex
    content/obj/<sid>/_psize       (2,)             float (px size in m)
    content/obj/<sid>/_origin      (2,)             float (origin in m)
    content/probe/<sid>/data       (modes,  ny, nx) complex
    content/probe/<sid>/_psize, _origin
    content/runtime/iter_info/<NNNNN>/{iteration, error, duration}

Public surface:

    is_ptyr_file(path)     -> bool
    summarize(path)        -> dict (storages, iter_info, note)
    project(path, ...)     -> dict (2D real array suitable for plotting)
    cuts(data2d, row, col) -> dict (h/v 1D cuts through (row, col))
    downsample(arr, n)     -> (decimated 2D, (sy, sx))

A `summarize` result looks like::

    {
      'storages': {
        'obj':   [{'id': 'SMFG00', 'shape': [1, 2303, 2315],
                   'psize': [5.5e-9, 5.5e-9], 'origin': [...]}],
        'probe': [{'id': 'SMFG00', 'shape': [2, 1411, 1411], ...}],
      },
      'iter_info': {'iteration': [...], 'error_total': [...],
                    'error_fourier': [...], 'error_overlap': [...]},
      'note': 'ptyr file',
    }
"""

from __future__ import annotations

import os
from typing import Optional

import numpy as np

#h5py is the only hard dep for ptyr preview; it's already optional in
#renderers.py, so we mirror that pattern.
try:
    import h5py
    _HAS_H5PY = True
except ImportError:
    h5py = None
    _HAS_H5PY = False


# ---------------------------------------------------------------------------
# Sniffing
# ---------------------------------------------------------------------------

def is_ptyr_file(path: str) -> bool:
    """Recognize a ptyr by extension + presence of a `content/{obj,probe}` group."""
    if not os.path.isfile(path):
        return False
    if os.path.splitext(path)[1].lower() != '.ptyr':
        return False
    if not _HAS_H5PY:
        #Extension-only fallback: a .ptyr without h5py available is still
        #worth flagging as a ptyr so the UI can show "h5py needed".
        return True
    try:
        with h5py.File(path, 'r') as f:
            return 'content' in f and (
                'obj' in f['content'] or 'probe' in f['content'])
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Summarize
# ---------------------------------------------------------------------------

def _storage_meta(grp) -> dict:
    """Lightweight metadata for one storage (no data load)."""
    out: dict = {'id': grp.name.rsplit('/', 1)[-1]}
    if 'data' in grp:
        ds = grp['data']
        out['shape'] = list(ds.shape)
        out['dtype'] = str(ds.dtype)
    for k in ('_psize', '_origin', '_center'):
        if k in grp:
            v = grp[k][...]
            out[k.lstrip('_')] = v.tolist() if hasattr(v, 'tolist') else v
    return out


def _iter_info(f) -> dict:
    """Stack per-iteration records into 1D arrays. PtyPy stores them as
    one group per iteration (`00000`, `00001`, ...) — we flatten."""
    g = f.get('content/runtime/iter_info')
    if g is None:
        return {}
    keys = sorted(g.keys())
    if not keys:
        return {}
    iters, totals, fouriers, overlaps, durations = [], [], [], [], []
    for k in keys:
        rec = g[k]
        it = int(rec['iteration'][()]) if 'iteration' in rec else None
        if it is None:
            continue
        iters.append(it)
        err = rec['error'][...] if 'error' in rec else None
        #PtyPy's `error` is typically (3,) = [fourier, photon, overlap]; we
        #expose all three and a "total" (mean) as a robust display default.
        if err is not None and err.size >= 3:
            fouriers.append(float(err[0]))
            overlaps.append(float(err[2]))
            totals.append(float(err.mean()))
        elif err is not None and err.size:
            v = float(np.asarray(err).flatten()[0])
            fouriers.append(v); overlaps.append(v); totals.append(v)
        else:
            fouriers.append(float('nan'))
            overlaps.append(float('nan'))
            totals.append(float('nan'))
        durations.append(float(rec['duration'][()])
                         if 'duration' in rec else float('nan'))
    return {
        'iteration': iters,
        'error_total': totals,
        'error_fourier': fouriers,
        'error_overlap': overlaps,
        'duration': durations,
    }


def summarize(path: str) -> dict:
    """Cheap metadata pass — no array loads."""
    if not _HAS_H5PY:
        return {'error': 'h5py not installed; cannot preview .ptyr'}
    try:
        with h5py.File(path, 'r') as f:
            obj_grp = f.get('content/obj')
            prb_grp = f.get('content/probe')
            obj = ([_storage_meta(obj_grp[k]) for k in obj_grp.keys()]
                   if obj_grp is not None else [])
            prb = ([_storage_meta(prb_grp[k]) for k in prb_grp.keys()]
                   if prb_grp is not None else [])
            ii = _iter_info(f)
    except Exception as e:
        return {'error': 'cannot read ptyr: %s' % e}
    return {
        'storages': {'obj': obj, 'probe': prb},
        'iter_info': ii,
        'note': 'ptyr file',
    }


# ---------------------------------------------------------------------------
# Project a single (kind, storage, mode, repr) to 2D real
# ---------------------------------------------------------------------------

_REPRS = ('amplitude', 'phase', 'real', 'imag', 'intensity')


def _resolve_path(f, kind: str, storage: Optional[str]) -> str:
    """Return the HDF5 path of the requested storage group, raising if absent."""
    base = f'content/{kind}'
    if base not in f:
        raise KeyError(f'{base} not in file')
    g = f[base]
    if storage and storage in g:
        return f'{base}/{storage}'
    #Default to the first storage in deterministic order.
    keys = sorted(g.keys())
    if not keys:
        raise KeyError(f'no storages under {base}')
    return f'{base}/{keys[0]}'


def project(path: str, *,
            kind: str = 'obj',
            storage: Optional[str] = None,
            mode: int = 0,
            repr_: str = 'amplitude') -> dict:
    """Read one (kind, storage, mode) slice and reduce to a 2D real array.

    :param kind: 'obj' or 'probe'.
    :param storage: storage ID (e.g. 'SMFG00'); defaults to first sorted.
    :param mode: layer/mode index along axis 0.
    :param repr_: one of 'amplitude', 'phase', 'real', 'imag', 'intensity'.
    """
    if not _HAS_H5PY:
        return {'error': 'h5py not installed; cannot preview .ptyr'}
    if kind not in ('obj', 'probe'):
        return {'error': "kind must be 'obj' or 'probe'"}
    if repr_ not in _REPRS:
        return {'error': 'unknown repr %r; want one of %s' % (repr_, _REPRS)}
    try:
        with h5py.File(path, 'r') as f:
            gpath = _resolve_path(f, kind, storage)
            g = f[gpath]
            if 'data' not in g:
                return {'error': f'{gpath} has no data'}
            data = g['data']
            if data.ndim != 3:
                return {'error':
                        f'{gpath}/data has ndim={data.ndim}, expected 3'}
            nmodes = data.shape[0]
            m = max(0, min(int(mode), nmodes - 1))
            #Read only the requested slice — avoids loading huge object
            #grids when only one layer is needed.
            arr = data[m]
            psize = (g['_psize'][...].tolist()
                     if '_psize' in g else [1.0, 1.0])
            origin = (g['_origin'][...].tolist()
                      if '_origin' in g else [0.0, 0.0])
            sid = gpath.rsplit('/', 1)[-1]
    except KeyError as e:
        return {'error': str(e)}
    except Exception as e:
        return {'error': 'cannot read ptyr: %s' % e}

    if repr_ == 'amplitude':
        out = np.abs(arr)
    elif repr_ == 'intensity':
        out = np.abs(arr) ** 2
    elif repr_ == 'phase':
        out = np.angle(arr)
    elif repr_ == 'real':
        out = arr.real
    else:  # 'imag'
        out = arr.imag
    return {
        'data': np.asarray(out, dtype=float),
        'meta': {
            'kind': kind, 'storage': sid, 'mode': m,
            'nmodes': nmodes, 'shape': list(arr.shape),
            'repr': repr_, 'psize': psize, 'origin': origin,
        },
    }


# ---------------------------------------------------------------------------
# Display helpers (shared shape with srw_loaders)
# ---------------------------------------------------------------------------

def cuts(data2d, row: int = -1, col: int = -1) -> dict:
    """Return horizontal + vertical 1D cuts through (row, col). Center on -1."""
    a = np.asarray(data2d)
    ny, nx = a.shape
    r = ny // 2 if row < 0 or row >= ny else row
    c = nx // 2 if col < 0 or col >= nx else col
    return {
        'row': int(r), 'col': int(c),
        'h': a[r].tolist(),
        'v': a[:, c].tolist(),
    }


def downsample(arr, target_max: int = 512):
    """Decimate a 2D array so max(shape) <= target_max."""
    a = np.asarray(arr)
    if a.ndim != 2:
        return a, (1, 1)
    ny, nx = a.shape
    sy = max(1, int(np.ceil(ny / target_max)))
    sx = max(1, int(np.ceil(nx / target_max)))
    return a[::sy, ::sx], (sy, sx)
