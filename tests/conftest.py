"""Shared pytest fixtures for the diffman test suite."""

from __future__ import annotations

import os
import sys
import tempfile
import textwrap

import pytest


@pytest.fixture
def scan_root(tmp_path, monkeypatch):
    """A clean scan root with an isolated diffman variant registry.

    The variant registry is global, so we wipe it between tests by
    re-importing `diffman.core` into a fresh module each time and
    re-binding it. Returns the directory tmp_path.
    """
    #Drop any cached pipeline modules from previous tests so register()
    #doesn't ValueError on a repeat name.
    from diffman import core, discovery
    core.registry._variants.clear()
    discovery.DISCOVERED_PATHS.clear()
    discovery.DISCOVERED_LIST.clear()
    discovery.PIPELINE_TO_MODULE.clear()
    discovery.PATH_TO_MODULE.clear()
    for k in list(sys.modules):
        if k.startswith('_diffman_test_'):
            del sys.modules[k]
    monkeypatch.syspath_prepend(str(tmp_path))
    return tmp_path


def write_pipeline(scan_root, module_name: str, body: str) -> str:
    """Write a pipeline module under scan_root and return its path."""
    path = os.path.join(scan_root, f'{module_name}.py')
    with open(path, 'w') as f:
        f.write(textwrap.dedent(body).lstrip())
    return path


@pytest.fixture
def make_pipeline(scan_root):
    """Factory: write_pipeline(name, body) and call discover()."""
    from diffman import discovery

    def _make(name: str, body: str) -> str:
        p = write_pipeline(scan_root, name, body)
        discovery.discover(str(scan_root))
        return p

    return _make
