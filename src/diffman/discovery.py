"""Grep-based module discovery + path-aware module loader."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


DISCOVERED_PATHS: dict[str, str] = {}     # module name -> containing dir
DISCOVERED_LIST: list[dict] = []           # ordered for UI listing

# Populated lazily by load_module() when a module's PIPELINE name is known.
# Keyed on the *pipeline name* (the string in `Pipeline('name', ...)`).
PIPELINE_TO_MODULE: dict[str, str] = {}

# Reverse lookup: module path -> module name. Used by the file-watcher
# to know which module to evict when a .py changes.
PATH_TO_MODULE: dict[str, str] = {}

_EXCLUDE = {'__pycache__', '.git', 'runs', 'tests',
            'node_modules', 'venv', '.venv', 'build', 'dist'}
_SKIP_MODULE_NAMES = {'diffman', 'core', 'cli', 'server',
                      'discovery', 'renderers', 'git_backup'}


def discover(root: str = '.') -> list[dict]:
    """Walk `root` for .py files mentioning both `diffman` and `PIPELINE`.

    No code is executed. Returns and caches a sorted list of
    `{module, path, dir}` entries; populates `DISCOVERED_PATHS` so
    `load_module()` can find them later.
    """
    root_abs = os.path.abspath(root)
    found: list[dict] = []
    for dirpath, dirnames, filenames in os.walk(root_abs):
        dirnames[:] = [d for d in dirnames
                       if d not in _EXCLUDE and not d.startswith('.')]
        for fn in filenames:
            if not fn.endswith('.py'):
                continue
            full = os.path.join(dirpath, fn)
            try:
                text = Path(full).read_text(errors='ignore')
            except OSError:
                continue
            if 'diffman' not in text or 'PIPELINE' not in text:
                continue
            mod = os.path.splitext(fn)[0]
            if mod in _SKIP_MODULE_NAMES:
                continue
            DISCOVERED_PATHS[mod] = dirpath
            PATH_TO_MODULE[full] = mod
            rel = os.path.relpath(full, root_abs)
            found.append({'module': mod, 'path': rel,
                          'dir': os.path.dirname(rel) or '.'})
    found.sort(key=lambda x: (x['dir'], x['module']))
    DISCOVERED_LIST[:] = found
    return found


def load_module(name: str):
    """Import a discovered pipeline module by name (idempotent).

    Adds its directory to `sys.path` if not already there, then tags the
    module's `PIPELINE` with its source path so `Pipeline.run()` can
    git-snapshot it. Tagging happens whether or not the module was
    already imported, so it works even if the user imported the module
    directly before calling `load_module`.
    """
    if name not in sys.modules:
        extra = DISCOVERED_PATHS.get(name)
        if extra and extra not in sys.path:
            sys.path.insert(0, extra)
        importlib.import_module(name)
    mod = sys.modules[name]
    pipe = getattr(mod, 'PIPELINE', None)
    if pipe is not None:
        if getattr(pipe, '_source_file', None) is None:
            try:
                pipe._source_file = getattr(mod, '__file__', None)
            except Exception:
                pass
        PIPELINE_TO_MODULE[pipe.name] = name
    return mod


def evict_module(name: str) -> None:
    """Drop cached state for `name` so the next `load_module` re-imports it.

    Used by the file watcher when a pipeline `.py` is edited. Removes
    sys.modules entry, the pipeline-name reverse mapping, and any
    variants attributed to this module in the global registry.
    """
    import sys
    from .core import registry as _reg
    sys.modules.pop(name, None)
    for pn, mn in list(PIPELINE_TO_MODULE.items()):
        if mn == name:
            del PIPELINE_TO_MODULE[pn]
    _reg.drop_module(name)
