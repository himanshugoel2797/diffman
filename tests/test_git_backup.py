"""Direct tests for `diffman.git_backup.snapshot`.

The pipeline-level tests mock `gb.snapshot` out entirely; nothing
actually exercised the real subprocess-driven snapshot path. These
tests run the real git binary against a fresh temp dir so the snapshot
behavior (init, copy-in, commit, multiple-commit history) is covered.

If `git` isn't on PATH the tests skip — the production code is a no-op
in that environment, and there's no point asserting "no-op happened".
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from diffman.git_backup import snapshot


pytestmark = pytest.mark.skipif(
    shutil.which('git') is None, reason='git not available on PATH')


def _git(*args, cwd: str) -> str:
    """Run `git ...` and return decoded stdout. Fails the test on nonzero."""
    return subprocess.check_output(['git', *args], cwd=cwd,
                                   stderr=subprocess.DEVNULL).decode()


class TestSnapshotInitializes:
    def test_first_call_inits_repo_and_commits(self, tmp_path):
        src = tmp_path / 'pipe.py'
        src.write_text('PIPELINE = "v1"\n')
        runs_root = tmp_path / 'runs'
        snapshot(str(runs_root), str(src), 'first commit')
        repo = runs_root / '_scripts'
        assert (repo / '.git').is_dir(), 'snapshot must init the repo'
        assert (repo / 'pipe.py').exists(), 'source must be copied in'
        assert (repo / 'pipe.py').read_text() == 'PIPELINE = "v1"\n'
        log = _git('log', '--format=%s', cwd=str(repo)).splitlines()
        assert log == ['first commit']

    def test_uses_diffman_author(self, tmp_path):
        """The snapshot must commit as 'diffman' regardless of the user's
        ambient git config — otherwise a system without a configured
        user.email would refuse to commit, and tests would be flaky."""
        src = tmp_path / 'pipe.py'
        src.write_text('x = 1\n')
        runs_root = tmp_path / 'runs'
        snapshot(str(runs_root), str(src), 'm')
        author = _git('log', '--format=%an <%ae>',
                      cwd=str(runs_root / '_scripts')).strip()
        assert author == 'diffman <diffman@local>'


class TestSnapshotRecordsHistory:
    def test_subsequent_call_adds_another_commit(self, tmp_path):
        src = tmp_path / 'pipe.py'
        src.write_text('v = 1\n')
        runs_root = tmp_path / 'runs'
        snapshot(str(runs_root), str(src), 'msg one')
        src.write_text('v = 2\n')
        snapshot(str(runs_root), str(src), 'msg two')
        log = _git('log', '--format=%s', cwd=str(runs_root / '_scripts'))
        # Most-recent first in `git log`.
        assert log.splitlines() == ['msg two', 'msg one']
        # Latest commit's tree has the new content.
        head_content = _git('show', 'HEAD:pipe.py',
                            cwd=str(runs_root / '_scripts'))
        assert head_content == 'v = 2\n'

    def test_unchanged_source_still_records_commit(self, tmp_path):
        """When the .py hasn't changed between two runs, snapshot uses
        --allow-empty so we still get a commit per run — that's how
        runs are correlated back to their source state."""
        src = tmp_path / 'pipe.py'
        src.write_text('static\n')
        runs_root = tmp_path / 'runs'
        snapshot(str(runs_root), str(src), 'r1')
        snapshot(str(runs_root), str(src), 'r2')
        log = _git('log', '--format=%s',
                   cwd=str(runs_root / '_scripts')).splitlines()
        assert log == ['r2', 'r1']


class TestSnapshotNoOps:
    def test_no_op_when_source_file_missing(self, tmp_path):
        runs_root = tmp_path / 'runs'
        # Nothing should happen — no repo should even be created.
        snapshot(str(runs_root), str(tmp_path / 'nope.py'), 'm')
        assert not (runs_root / '_scripts' / '.git').exists()

    def test_no_op_when_source_file_is_empty_string(self, tmp_path):
        runs_root = tmp_path / 'runs'
        snapshot(str(runs_root), '', 'm')
        assert not (runs_root / '_scripts' / '.git').exists()


class TestSnapshotSurfacesGitFailures:
    """Pre-fix, git add / commit ran with check=False and devnulled stderr,
    so a corrupted repo would happily produce wrong history with no
    diagnostic. Now: nonzero git returncode raises CalledProcessError
    and the captured stderr lands in the WARNING log."""

    def test_corrupt_repo_raises_with_stderr_detail(self, tmp_path, caplog):
        import logging
        import subprocess

        src = tmp_path / 'pipe.py'
        src.write_text('x = 1\n')
        runs_root = tmp_path / 'runs'
        repo = runs_root / '_scripts'
        repo.mkdir(parents=True)
        # Plant a directory that looks like a repo to bypass `git init`,
        # but contains a bogus HEAD so `git add` will fail.
        (repo / '.git').mkdir()
        (repo / '.git' / 'HEAD').write_text('not a valid ref')

        with caplog.at_level(logging.WARNING, logger='diffman.git_backup'):
            with pytest.raises(subprocess.CalledProcessError) as ei:
                snapshot(str(runs_root), str(src), 'm')
        # The original git stderr lands in our warning text so the user
        # can debug what went wrong, instead of "snapshot failed" with no
        # explanation.
        assert ei.value.returncode != 0
        log_text = ' '.join(rec.message for rec in caplog.records)
        assert 'git_backup' in log_text
        assert 'rc=' in log_text

    def test_copy_failure_raises_oserror_logged(self, tmp_path, monkeypatch,
                                                  caplog):
        import logging
        from diffman import git_backup as gb

        src = tmp_path / 'pipe.py'
        src.write_text('x = 1\n')
        runs_root = tmp_path / 'runs'
        # Pre-init the repo so snapshot() skips `git init` and goes
        # straight to the copy step.
        (runs_root / '_scripts').mkdir(parents=True)
        subprocess.run(['git', 'init', '-q'],
                       cwd=str(runs_root / '_scripts'), check=True)

        def explode(*a, **kw):
            raise OSError('disk full simulation')
        monkeypatch.setattr(gb.shutil, 'copy2', explode)

        with caplog.at_level(logging.WARNING, logger='diffman.git_backup'):
            with pytest.raises(OSError, match='disk full'):
                snapshot(str(runs_root), str(src), 'm')
        assert any('copy' in rec.message for rec in caplog.records)


class TestSnapshotRespectsGpgSign:
    def test_snapshot_succeeds_when_global_gpgsign_is_enabled(
            self, tmp_path, monkeypatch):
        """Some users have `commit.gpgsign=true` set globally. The
        snapshot code passes `--no-gpg-sign` to avoid a hard failure on
        machines without a signing key; verify the path actually works
        when the ambient config requests signing."""
        # Use a fake HOME so we can stage a global git config that
        # requires signing — without ever touching the real user's config.
        fake_home = tmp_path / 'fake_home'
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))
        monkeypatch.setenv('XDG_CONFIG_HOME', str(fake_home / '.config'))
        (fake_home / '.gitconfig').write_text(
            '[commit]\n\tgpgsign = true\n'
            '[user]\n\tsigningkey = nonexistent\n')
        src = tmp_path / 'pipe.py'
        src.write_text('p = 1\n')
        runs_root = tmp_path / 'runs'
        # Must not raise even though the user has gpgsign=true and no key.
        snapshot(str(runs_root), str(src), 'm')
        log = _git('log', '--format=%s',
                   cwd=str(runs_root / '_scripts')).splitlines()
        assert log == ['m']
