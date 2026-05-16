"""Best-effort git-backed snapshot of pipeline scripts.

Maintains <runs_root>/_scripts/.git/ with one commit per Pipeline.run().
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess

_log = logging.getLogger(__name__)


def _run_git(args, *, cwd: str, env: dict) -> subprocess.CompletedProcess:
    """Run git with stderr/stdout captured, raise on nonzero returncode."""
    return subprocess.run(['git', *args], cwd=cwd, env=env,
                          check=True, capture_output=True, text=True)


def _format_failure(e: subprocess.CalledProcessError) -> str:
    """One-line description with returncode + captured stderr/stdout."""
    detail = (e.stderr or '').strip() or (e.stdout or '').strip() or '<no output>'
    return f'rc={e.returncode}: {detail}'


def snapshot(runs_root: str, source_file: str, message: str) -> None:
    """Copy `source_file` into `<runs_root>/_scripts/` and commit it.

    No-ops on missing source file or missing `git` on PATH (those are
    non-error conditions in environments without git).

    All other failures — `git init`, `git add`, `git commit`, the source
    copy itself — are logged at WARNING with the underlying stderr and
    re-raised. The caller decides whether to swallow them (the production
    caller `_try_snapshot` does, so a snapshot failure never blocks a
    run, but it's no longer silent).

    Initializes the repo on first call.
    """
    if not source_file or not os.path.isfile(source_file):
        return
    if shutil.which('git') is None:
        return
    repo = os.path.join(runs_root, '_scripts')
    os.makedirs(repo, exist_ok=True)
    env = dict(os.environ,
               GIT_AUTHOR_NAME='diffman', GIT_AUTHOR_EMAIL='diffman@local',
               GIT_COMMITTER_NAME='diffman', GIT_COMMITTER_EMAIL='diffman@local')
    if not os.path.isdir(os.path.join(repo, '.git')):
        try:
            _run_git(['init', '-q'], cwd=repo, env=env)
        except subprocess.CalledProcessError as e:
            _log.warning('git_backup: git init in %s failed: %s',
                         repo, _format_failure(e))
            raise
    dst = os.path.join(repo, os.path.basename(source_file))
    try:
        shutil.copy2(source_file, dst)
        _run_git(['add', '-A'], cwd=repo, env=env)
        _run_git(['commit', '-qm', message, '--allow-empty', '--no-gpg-sign'],
                 cwd=repo, env=env)
    except subprocess.CalledProcessError as e:
        _log.warning('git_backup: git %s in %s failed: %s',
                     ' '.join(e.cmd[1:]), repo, _format_failure(e))
        raise
    except OSError as e:
        _log.warning('git_backup: copy %s -> %s failed: %s',
                     source_file, dst, e)
        raise
