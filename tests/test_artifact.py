"""Tests for RunContext.artifact(): symlink/copy semantics, dirs, errors.

artifact() is the only way pipelines register outputs, so its branches
all need direct coverage. Going through Pipeline.run for every case
would obscure which branch fired — these tests construct a RunContext
directly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import diffman as dm
from diffman.core import RunContext, RunRecord


def _ctx(tmp_path):
    """Build a bare RunContext rooted at tmp_path — no pipeline needed."""
    fdir = str(tmp_path / 'run')
    os.makedirs(fdir, exist_ok=True)
    variant = dm.Variant('v', None, {})
    record = RunRecord(pipeline='p', variant='v', fingerprint='fp',
                       fdir=fdir, started='now')
    return RunContext(fdir, variant, record)


class TestArtifactFiles:
    def test_symlink_path_is_taken_when_supported(self, tmp_path):
        ctx = _ctx(tmp_path)
        src = tmp_path / 'src.txt'
        src.write_text('hello')
        dest = ctx.artifact('s', 'out.txt', str(src))
        assert os.path.islink(dest)
        assert os.readlink(dest) == str(src.resolve())
        assert Path(dest).read_text() == 'hello'

    def test_falls_back_to_copy_when_symlink_raises(self, tmp_path,
                                                     monkeypatch):
        """OSError from os.symlink (Windows-without-priv, refusing FS,
        etc.) must transparently fall through to shutil.copy2."""
        ctx = _ctx(tmp_path)
        src = tmp_path / 'src.txt'
        src.write_text('payload')
        monkeypatch.setattr(os, 'symlink',
                            lambda *a, **k: (_ for _ in ()).throw(OSError('nope')))
        dest = ctx.artifact('s', 'out.txt', str(src))
        assert not os.path.islink(dest)
        assert os.path.isfile(dest)
        assert Path(dest).read_text() == 'payload'

    def test_returns_destination_path_under_stages_outputs(self, tmp_path):
        ctx = _ctx(tmp_path)
        src = tmp_path / 'x.txt'; src.write_text('.')
        dest = ctx.artifact('mystage', 'sub/nested.txt', str(src))
        rel = os.path.relpath(dest, ctx.fdir)
        assert rel == os.path.join('stages', 'mystage', 'outputs',
                                   'sub', 'nested.txt')

    def test_creates_intermediate_dirs_for_nested_relpath(self, tmp_path):
        ctx = _ctx(tmp_path)
        src = tmp_path / 'src.txt'; src.write_text('.')
        dest = ctx.artifact('s', 'a/b/c/leaf.txt', str(src))
        assert os.path.isfile(dest)
        assert os.path.isdir(os.path.dirname(dest))


class TestArtifactDirectories:
    def test_symlinks_a_directory(self, tmp_path):
        ctx = _ctx(tmp_path)
        srcdir = tmp_path / 'tree'
        (srcdir / 'sub').mkdir(parents=True)
        (srcdir / 'sub' / 'a.txt').write_text('hi')
        dest = ctx.artifact('s', 'tree', str(srcdir))
        assert os.path.islink(dest)
        assert Path(dest, 'sub', 'a.txt').read_text() == 'hi'

    def test_copytree_when_symlink_fails(self, tmp_path, monkeypatch):
        """When the symlink path errors and source is a directory, the
        fallback must be shutil.copytree (not shutil.copy2)."""
        ctx = _ctx(tmp_path)
        srcdir = tmp_path / 'tree'
        (srcdir / 'sub').mkdir(parents=True)
        (srcdir / 'sub' / 'a.txt').write_text('hello')
        monkeypatch.setattr(os, 'symlink',
                            lambda *a, **k: (_ for _ in ()).throw(OSError('nope')))
        dest = ctx.artifact('s', 'tree', str(srcdir))
        assert not os.path.islink(dest)
        assert os.path.isdir(dest)
        assert Path(dest, 'sub', 'a.txt').read_text() == 'hello'


class TestArtifactReplacesExistingDest:
    def test_replaces_existing_file(self, tmp_path):
        ctx = _ctx(tmp_path)
        old = tmp_path / 'old.txt'; old.write_text('OLD')
        new = tmp_path / 'new.txt'; new.write_text('NEW')
        ctx.artifact('s', 'out.txt', str(old))
        dest = ctx.artifact('s', 'out.txt', str(new))
        assert Path(dest).read_text() == 'NEW'

    def test_replaces_existing_symlink(self, tmp_path):
        ctx = _ctx(tmp_path)
        a = tmp_path / 'a.txt'; a.write_text('A')
        b = tmp_path / 'b.txt'; b.write_text('B')
        ctx.artifact('s', 'out.txt', str(a))           #symlink → a
        dest = ctx.artifact('s', 'out.txt', str(b))    #must overwrite
        assert os.readlink(dest) == str(b.resolve())

    def test_replaces_existing_directory(self, tmp_path):
        ctx = _ctx(tmp_path)
        old = tmp_path / 'old_dir'
        old.mkdir(); (old / 'x.txt').write_text('old contents')
        new = tmp_path / 'new_dir'
        new.mkdir(); (new / 'y.txt').write_text('new contents')
        #Stage the old directory as a real copy first so the dest is a
        #directory (not a symlink) to force the rmtree branch.
        import shutil
        outdir = os.path.join(ctx.fdir, 'stages', 's', 'outputs')
        os.makedirs(outdir, exist_ok=True)
        shutil.copytree(str(old), os.path.join(outdir, 'tree'))
        dest = ctx.artifact('s', 'tree', str(new))
        assert not os.path.exists(os.path.join(dest, 'x.txt'))
        assert Path(dest, 'y.txt').read_text() == 'new contents'


class TestArtifactErrors:
    def test_missing_source_raises_FileNotFoundError(self, tmp_path):
        ctx = _ctx(tmp_path)
        with pytest.raises(FileNotFoundError, match='artifact source'):
            ctx.artifact('s', 'out.txt', str(tmp_path / 'nope.txt'))


class TestUpstreamArtifactHelper:
    def test_returns_full_path_inside_upstream_fdir(self, tmp_path):
        upstream = RunRecord(pipeline='u', variant='v', fingerprint='abc',
                             fdir=str(tmp_path / 'up'), started='t')
        os.makedirs(upstream.fdir)
        ctx = _ctx(tmp_path)
        ctx.upstream['prev'] = upstream
        assert ctx.upstream_artifact('prev') == upstream.fdir
        assert ctx.upstream_artifact('prev', 'stages/x/outputs/d.txt') == \
            os.path.join(upstream.fdir, 'stages/x/outputs/d.txt')

    def test_unknown_step_raises_KeyError(self, tmp_path):
        ctx = _ctx(tmp_path)
        with pytest.raises(KeyError):
            ctx.upstream_artifact('not_a_step')
