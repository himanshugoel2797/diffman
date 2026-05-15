"""Grep-based module discovery + path-aware module loader."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

from .core import registry

DISCOVERED_PATHS: dict[str, str] = {}
DISCOVERED_LIST: list[dict] = []
_module_variants: dict[str, set[str]] = {}


def discover(root: str = '.', exclude=None) -> list[dict]:
    """Walk `root` looking for diffman pipeline modules.

    A file qualifies if its source mentions `diffman` (the import) and
    `PIPELINE` (the assignment). No code is executed.

    Returns a sorted list of {module, path, dir}; side-effects DISCOVERED_PATHS.
    """
    exclude = set(exclude or ('__pycache__', '.git', 'runs', 'tests',
                              'node_modules', 'venv', '.venv', 'build', 'dist'))
    root_abs = os.path.abspath(root)
    found: list[dict] = []
    for dirpath, dirnames, filenames in os.walk(root_abs):
        dirnames[:] = [d for d in dirnames
                       if d not in exclude and not d.startswith('.')]
        for fn in filenames:
            if not fn.endswith('.py'):
                continue
            full = os.path.join(dirpath, fn)
            try:
                text = Path(full).read_text(errors='ignore')
            except OSError:
                continue
            if 'diffman' not in text:
                continue
            if 'PIPELINE' not in text:
                continue
            mod_name = os.path.splitext(fn)[0]
            if mod_name in {'diffman', 'core', 'cli', 'server', 'submitters',
                            'discovery', 'renderers', 'git_backup'}:
                #Skip our own modules if someone scans the install dir.
                continue
            rel_path = os.path.relpath(full, root_abs)
            rel_dir = os.path.dirname(rel_path) or '.'
            DISCOVERED_PATHS[mod_name] = os.path.dirname(full)
            found.append({'module': mod_name, 'path': rel_path, 'dir': rel_dir})
    found.sort(key=lambda x: (x['dir'], x['module']))
    DISCOVERED_LIST[:] = found
    return found


def load_module(name: str):
    """Import a pipeline module by name; tag its PIPELINE with `_source_file`.

    Adds the discovered directory to sys.path if known.
    """
    if name in _module_variants and name in sys.modules:
        return sys.modules[name]
    extra = DISCOVERED_PATHS.get(name)
    if extra and extra not in sys.path:
        sys.path.insert(0, extra)
    before = set(registry.names())
    mod = importlib.import_module(name)
    _module_variants[name] = set(registry.names()) - before
    if hasattr(mod, 'PIPELINE'):
        try:
            mod.PIPELINE._source_file = getattr(mod, '__file__', None)
        except Exception:
            pass
    return mod


def module_variants(name: str) -> list[str]:
    """Return the variant names registered when `name` was loaded."""
    return sorted(_module_variants.get(name, set()))
