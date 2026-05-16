"""SRW-aware loaders + representation conversion for preview.

Uses SRW's own ``srwl_uti_read_*`` functions where possible so files written
by ``srwl_uti_save_*`` round-trip cleanly. Falls back gracefully when SRW
isn't installed: the file is reported as binary in that case.

Public surface:

    is_srw_file(path)         -> bool   #content-sniff
    load(path)                -> dict   #unified representation
    project(load_result, repr_) -> dict #2D real array suitable for plotting

A ``load`` result looks like::

    {
      'kind':       'wavefield' | 'intensity',
      'mesh':       {xStart, xFin, nx, yStart, yFin, ny,
                     eStart, eFin, ne},
      'fields':     {'Ex': complex2D, 'Ey': complex2D}    # wavefield
      'data':       float2D                               # intensity
      'available':  ('intensity', 'amplitude', 'phase',
                     'real', 'imag')                      # wavefield
                  | ('intensity',)                        # intensity
      'note':       free-form provenance string,
    }
"""

from __future__ import annotations

import os
from typing import Any, Optional

import numpy as np

#SRW import is optional — only required when previewing SRW files.
_SRWLIB = None
_SRWLIB_TRIED = False


def _srwlib():
    global _SRWLIB, _SRWLIB_TRIED
    if _SRWLIB is not None or _SRWLIB_TRIED:
        return _SRWLIB
    _SRWLIB_TRIED = True
    for name in ('srwpy.srwlib', 'srwlib'):
        try:
            mod = __import__(name, fromlist=['*'])
            _SRWLIB = mod
            return mod
        except ImportError:
            continue
    return None


# ---------------------------------------------------------------------------
# Sniffing
# ---------------------------------------------------------------------------

def is_srw_file(path: str) -> bool:
    """Heuristic: recognize SRW intensity/wavefield files by extension+content."""
    if not os.path.isfile(path):
        return False
    ext = os.path.splitext(path)[1].lower()
    if ext == '.dat':
        try:
            with open(path, 'rb') as f:
                head = f.read(2048)
        except OSError:
            return False
        return b'#' in head and (b'Horizontal Position' in head
                                 or b'Photon Energy' in head
                                 or b'Intensity' in head
                                 or b'Fluence' in head)
    if ext in {'.h5', '.hdf5'}:
        #SRW HDF5 files have either a top-level 'intensity' or 'wfr' group.
        try:
            import h5py
        except ImportError:
            return False
        try:
            with h5py.File(path, 'r') as f:
                top = set(f.keys())
        except Exception:
            return False
        return bool(top & {'intensity', 'wfr', 'arEx', 'arEy', 'mesh'})
    return False


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load(path: str) -> dict:
    """Load an SRW intensity or wavefield file via SRW's own readers.

    The return shape is described in the module docstring.
    """
    srw = _srwlib()
    if srw is None:
        return {'error': 'srwlib not importable; install SRW to enable SRW previews'}

    ext = os.path.splitext(path)[1].lower()

    #--- Wavefield (HDF5 with arEx/arEy) ----------------------------------
    if ext in {'.h5', '.hdf5'}:
        try:
            import h5py
            with h5py.File(path, 'r') as f:
                top = set(f.keys())
        except Exception as e:
            return {'error': 'cannot inspect HDF5: %s' % e}
        if {'arEx', 'arEy'} <= top or 'wfr' in top:
            return _load_wfr_hdf5(path, srw)
        if 'intensity' in top:
            return _load_intens_hdf5(path, srw)
        return _load_intens_hdf5(path, srw)

    #--- Intensity ASCII --------------------------------------------------
    if ext == '.dat':
        return _load_intens_ascii(path, srw)

    return {'error': 'unknown extension: %s' % ext}


def _mesh_dict(mesh) -> dict:
    return {
        'xStart': getattr(mesh, 'xStart', 0.0),
        'xFin':   getattr(mesh, 'xFin', 0.0),
        'nx':     int(getattr(mesh, 'nx', 1)),
        'yStart': getattr(mesh, 'yStart', 0.0),
        'yFin':   getattr(mesh, 'yFin', 0.0),
        'ny':     int(getattr(mesh, 'ny', 1)),
        'eStart': getattr(mesh, 'eStart', 0.0),
        'eFin':   getattr(mesh, 'eFin', 0.0),
        'ne':     int(getattr(mesh, 'ne', 1)),
        'zStart': getattr(mesh, 'zStart', 0.0),
    }


def _load_intens_hdf5(path, srw):
    """Intensity HDF5 produced by srwl_uti_save_intens_hdf5."""
    data, mesh, _, _ = srw.srwl_uti_read_intens_hdf5(_file_path=path)
    arr = np.asarray(data, dtype=float)
    if arr.ndim == 1:
        arr = arr.reshape((int(mesh.ny), int(mesh.nx)))
    return {
        'kind': 'intensity',
        'mesh': _mesh_dict(mesh),
        'data': arr,
        'available': ('intensity',),
        'note': 'srwl_uti_read_intens_hdf5',
    }


def _load_intens_ascii(path, srw):
    """Intensity ASCII produced by srwl_uti_save_intens_ascii."""
    data, mesh = srw.srwl_uti_read_intens_ascii(_file_path=path)
    arr = np.asarray(data, dtype=float)
    nx = int(mesh.nx); ny = int(mesh.ny)
    if arr.ndim == 1 and nx * ny == arr.size:
        arr = arr.reshape((ny, nx))
    return {
        'kind': 'intensity',
        'mesh': _mesh_dict(mesh),
        'data': arr,
        'available': ('intensity',),
        'note': 'srwl_uti_read_intens_ascii',
    }


def _load_wfr_hdf5(path, srw):
    """Wavefront HDF5 produced by srwl_uti_save_wfr_hdf5."""
    wfr = srw.srwl_uti_read_wfr_hdf5(_file_path=path)
    mesh = wfr.mesh
    nx = int(mesh.nx); ny = int(mesh.ny); ne = int(getattr(mesh, 'ne', 1))

    arEx = np.asarray(wfr.arEx, dtype=np.float32)
    arEy = np.asarray(wfr.arEy, dtype=np.float32)
    #SRW interleaves real/imag along the fastest-varying axis. The layout
    #written by srwl_uti_save_wfr_hdf5 is (ny, nx, ne, 2) and that's what
    #the reader produces — there's no alternate ordering to detect.
    def to_complex(a):
        if a.size != ny * nx * ne * 2:
            raise ValueError(
                f'wavefield array size {a.size} does not match '
                f'ny*nx*ne*2 = {ny*nx*ne*2}')
        re = a[0::2].reshape((ny, nx, ne))
        im = a[1::2].reshape((ny, nx, ne))
        return (re + 1j * im).transpose(2, 0, 1)  # -> (ne, ny, nx)

    Ex = to_complex(arEx)
    Ey = to_complex(arEy)
    return {
        'kind': 'wavefield',
        'mesh': _mesh_dict(mesh),
        'fields': {'Ex': Ex, 'Ey': Ey},   # (ne, ny, nx) complex
        'available': ('intensity', 'amplitude', 'phase', 'real', 'imag'),
        'note': 'srwl_uti_read_wfr_hdf5',
    }


# ---------------------------------------------------------------------------
# Representations
# ---------------------------------------------------------------------------

def project(loaded: dict, repr_: str = 'intensity',
            *, polarization: str = 'both', energy_slice: int = -1) -> dict:
    """Reduce a `load` result to a single 2D real array for display.

    :param repr_: one of 'intensity', 'amplitude', 'phase', 'real', 'imag'.
        For intensity files this is forced to 'intensity'.
    :param polarization: 'both', 'Ex' or 'Ey'. Wavefield only.
    :param energy_slice: which energy slice to display, or -1 to sum over E.
    :return: dict {'mesh': ..., 'data': float2D, 'repr': repr_}
    """
    if loaded.get('kind') == 'intensity':
        return {'mesh': loaded['mesh'], 'data': loaded['data'],
                'repr': 'intensity'}
    if loaded.get('kind') != 'wavefield':
        return {'error': loaded.get('error', 'not a previewable SRW file')}

    Ex = loaded['fields']['Ex']
    Ey = loaded['fields']['Ey']
    ne = Ex.shape[0]
    sel = slice(None) if energy_slice < 0 or energy_slice >= ne else energy_slice

    def reduce_e(arr):
        a = arr[sel]
        if a.ndim == 3:
            return a.sum(axis=0)  # sum over energy (for intensity-like reprs)
        return a

    if repr_ == 'intensity':
        Ix = reduce_e(np.abs(Ex) ** 2) if polarization != 'Ey' else 0
        Iy = reduce_e(np.abs(Ey) ** 2) if polarization != 'Ex' else 0
        data = np.asarray(Ix) + np.asarray(Iy)
    elif repr_ == 'amplitude':
        Ax = reduce_e(np.abs(Ex)) if polarization != 'Ey' else 0
        Ay = reduce_e(np.abs(Ey)) if polarization != 'Ex' else 0
        data = np.asarray(Ax) + np.asarray(Ay)
    else:
        #phase / real / imag operate on one polarization at a time; default Ex.
        E = Ex if polarization != 'Ey' else Ey
        slc = E[sel] if energy_slice >= 0 else E[E.shape[0] // 2]
        if slc.ndim == 3:
            slc = slc[slc.shape[0] // 2]
        if repr_ == 'phase':
            data = np.angle(slc)
        elif repr_ == 'real':
            data = slc.real
        elif repr_ == 'imag':
            data = slc.imag
        else:
            return {'error': 'unknown repr %r' % repr_}

    return {'mesh': loaded['mesh'], 'data': np.asarray(data),
            'repr': repr_, 'polarization': polarization,
            'energy_slice': energy_slice}


def cuts(data2d, row: int = -1, col: int = -1) -> dict:
    """Return horizontal + vertical 1D cuts through (row, col).

    Negative row/col defaults to the center.
    """
    a = np.asarray(data2d)
    ny, nx = a.shape
    r = ny // 2 if row < 0 or row >= ny else row
    c = nx // 2 if col < 0 or col >= nx else col
    return {
        'row': int(r), 'col': int(c),
        'h': a[r].tolist(),   # horizontal cut (length nx)
        'v': a[:, c].tolist(),  # vertical cut (length ny)
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
