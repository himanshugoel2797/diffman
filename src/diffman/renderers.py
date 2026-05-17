"""Stage-artifact preview renderers.

Each renderer takes a file path and returns a typed payload dict:

    {'kind': 'srw' | 'image' | 'text' | 'json' | 'h5_tree' | 'scalar'
              | 'plot_1d' | 'plot_2d' | 'array_summary' | 'binary'
              | 'error',
     'data': <renderer-specific>,
     'meta': {...}}

The web server adds a content-type header and forwards the JSON to the
client, which selects a renderer (Plotly, <img>, <pre>, etc.). Heavy deps
(h5py, plotly) are optional — files of those types fall back to a generic
'binary' kind with a download link if the dep is missing.
"""

from __future__ import annotations

import base64
import io
import json
import os
import struct
from pathlib import Path
from typing import Optional

import numpy as np
from PIL import Image

try:
    import h5py
    _HAS_H5PY = True
except ImportError:
    h5py = None
    _HAS_H5PY = False


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def render(path: str, *, max_bytes: int = 4_000_000) -> dict:
    """Render a single artifact file into a UI-consumable payload.

    :param path: absolute path to the file.
    :param max_bytes: cap for raw text/binary preview size.
    """
    if not os.path.isfile(path):
        return {'kind': 'error', 'data': 'file not found', 'meta': {'path': path}}

    ext = os.path.splitext(path)[1].lower()
    size = os.path.getsize(path)
    meta = {'path': path, 'name': os.path.basename(path), 'size': size, 'ext': ext}

    try:
        # SRW-aware sniff first: catches .dat intensity files + SRW .h5
        # wavefields/intensities and surfaces a 'srw' payload so the UI
        # offers the repr selector + cuts.
        from . import srw_loaders
        if srw_loaders.is_srw_file(path):
            meta['srw'] = True
            return {'kind': 'srw', 'data': None, 'meta': meta}

        # PtyPy .ptyr reconstruction sniff — same lazy pattern as SRW: a
        # marker payload here, with /api/ptyr_preview doing the actual
        # array read on demand.
        from . import ptypy_loaders
        if ptypy_loaders.is_ptyr_file(path):
            meta['ptyr'] = True
            return {'kind': 'ptyr', 'data': None, 'meta': meta}

        if ext in {'.png', '.jpg', '.jpeg', '.gif', '.svg'}:
            return _render_image(path, meta)
        if ext == '.npy':
            return _render_npy(path, meta)
        if ext in {'.h5', '.hdf5', '.ptyd'}:
            return _render_h5(path, meta)
        if ext == '.json':
            return _render_json(path, meta, max_bytes)
        if ext in {'.txt', '.log', '.dat', '.csv', '.md', '.py', ''}:
            return _render_text(path, meta, max_bytes)
        return {'kind': 'binary', 'data': None, 'meta': meta}
    except Exception as e:
        return {'kind': 'error', 'data': repr(e), 'meta': meta}


# ---------------------------------------------------------------------------
# Per-type renderers
# ---------------------------------------------------------------------------

def _render_image(path, meta):
    with Image.open(path) as im:
        meta['width'], meta['height'] = im.size
        meta['mode'] = im.mode
    #The server serves the file via the /artifact/ route; the UI loads it
    #with an <img src=...> so we don't have to base64-embed here.
    return {'kind': 'image', 'data': None, 'meta': meta}


def _render_text(path, meta, max_bytes):
    with open(path, 'rb') as f:
        raw = f.read(max_bytes + 1)
    truncated = len(raw) > max_bytes
    text = raw[:max_bytes].decode('utf-8', errors='replace')
    return {'kind': 'text', 'data': text,
            'meta': {**meta, 'truncated': truncated}}


def _render_json(path, meta, max_bytes):
    try:
        with open(path) as f:
            obj = json.load(f)
    except json.JSONDecodeError:
        return _render_text(path, meta, max_bytes)
    return {'kind': 'json', 'data': obj, 'meta': meta}


def _render_npy(path, meta):
    arr = np.load(path, mmap_mode='r', allow_pickle=False)
    return _array_payload(arr, meta)


def _render_h5(path, meta):
    if not _HAS_H5PY:
        return {'kind': 'binary', 'data': None,
                'meta': {**meta, 'note': 'h5py not installed'}}
    out = {'kind': 'h5_tree', 'data': [], 'meta': meta}
    with h5py.File(path, 'r') as f:
        def visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                out['data'].append({
                    'name': name, 'kind': 'dataset',
                    'shape': list(obj.shape), 'dtype': str(obj.dtype),
                    'size': int(obj.size),
                })
            else:
                out['data'].append({'name': name, 'kind': 'group'})
        f.visititems(visit)
    return out


def render_h5_dataset(path: str, dataset: str, *,
                      max_points: int = 200_000) -> dict:
    """Render a specific HDF5 dataset."""
    if not _HAS_H5PY:
        return {'kind': 'error', 'data': 'h5py not installed',
                'meta': {'path': path, 'dataset': dataset}}
    with h5py.File(path, 'r') as f:
        if dataset not in f:
            return {'kind': 'error', 'data': f'dataset {dataset!r} not found',
                    'meta': {'path': path}}
        ds = f[dataset]
        meta = {'path': path, 'dataset': dataset,
                'shape': list(ds.shape), 'dtype': str(ds.dtype)}
        if ds.ndim == 0:
            return {'kind': 'scalar', 'data': ds[()].item()
                    if hasattr(ds[()], 'item') else ds[()],
                    'meta': meta}
        if ds.size > max_points and ds.ndim != 2:
            #Decimate 1D to keep transfer small.
            stride = int(np.ceil(ds.size / max_points))
            data = ds[::stride]
            meta['decimated_stride'] = stride
        else:
            data = ds[...]
        return _array_payload(data, meta)


def _array_payload(arr, meta: dict) -> dict:
    """Convert a numpy array into a plot-friendly payload.

    Heuristics:
      - 1D       -> 'plot_1d'
      - 2D       -> 'plot_2d' (heatmap), with downsampling if huge
      - >=3D     -> first slice (arr[0]) + note
      - non-num  -> 'array_summary'
    """
    a = np.asarray(arr)
    meta = {**meta, 'shape': list(a.shape), 'dtype': str(a.dtype)}
    if a.ndim == 1:
        if a.size <= 10_000:
            stride = 1
            y = a.tolist()
        else:
            stride = int(np.ceil(a.size / 10_000))
            y = a[::stride].tolist()
        x = list(range(0, stride * len(y), stride))
        if stride != 1:
            meta['decimated_stride'] = stride
        return {'kind': 'plot_1d', 'data': {'x': x, 'y': y}, 'meta': meta}
    if a.ndim == 2:
        target_max = 512
        if max(a.shape) > target_max:
            sy = int(np.ceil(a.shape[0] / target_max))
            sx = int(np.ceil(a.shape[1] / target_max))
            a2 = a[::sy, ::sx]
            meta['downsampled'] = [sy, sx]
        else:
            a2 = a
        if not np.issubdtype(a2.dtype, np.number):
            return {'kind': 'array_summary',
                    'data': {'preview': str(a.flatten()[:8].tolist())},
                    'meta': meta}
        finite = np.isfinite(a2)
        meta['stats'] = {
            'min': float(a2[finite].min()) if finite.any() else None,
            'max': float(a2[finite].max()) if finite.any() else None,
            'mean': float(a2[finite].mean()) if finite.any() else None,
        }
        return {'kind': 'plot_2d', 'data': a2.tolist(), 'meta': meta}
    #ndim >= 3 -> show first slice as 2D heatmap.
    meta['note'] = f'showing arr[0] of shape {list(a.shape)}'
    return _array_payload(a[0], meta)
