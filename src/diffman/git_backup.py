"""Best-effort git-backed snapshot of pipeline scripts.

Maintains <runs_root>/_scripts/.git/ with one commit per Pipeline.run().

Concurrency: many pipeline runs (often parallel, e.g. one srun task per
variation) call snapshot() against the SAME `_scripts/.git` repo. Plain
`git add`/`commit` take `.git/index.lock`, so concurrent callers collide,
and a caller killed mid-commit (preemption on an `urgent` allocation is
routine) leaves a STALE lock that blocks every later snapshot until it is
removed by hand. To avoid that, snapshots serialize on a cross-process file
lock, and the lock holder clears any leftover git lock files first — safe
because `_scripts/.git` is diffman-private, so a lock seen while we hold
exclusivity is necessarily stale.
"""

from __future__ import annotations

import fcntl
import glob
import logging
import os
import shutil
import subprocess
import time

_log = logging.getLogger(__name__)

# Max time to wait for the snapshot lock before giving up (best-effort: a
# wedged holder degrades to a skipped snapshot rather than a hung run).
_LOCK_TIMEOUT_S = 60.0


def _run_git(args, *, cwd: str, env: dict) -> subprocess.CompletedProcess:
    """Run git with stderr/stdout captured, raise on nonzero returncode."""
    return subprocess.run(['git', *args], cwd=cwd, env=env,
                          check=True, capture_output=True, text=True)


def _format_failure(e: subprocess.CalledProcessError) -> str:
    """One-line description with returncode + captured stderr/stdout."""
    detail = (e.stderr or '').strip() or (e.stdout or '').strip() or '<no output>'
    return f'rc={e.returncode}: {detail}'


def _clear_stale_git_locks(repo: str) -> None:
    """Remove leftover git lock files. Only call while holding the snapshot
    lock: `_scripts/.git` is written by nothing but diffman snapshots, so any
    lock present here is stale (a previous snapshot was killed mid-operation).
    """
    git_dir = os.path.join(repo, '.git')
    patterns = [
        os.path.join(git_dir, 'index.lock'),
        os.path.join(git_dir, '*.lock'),          # HEAD.lock, config.lock, ...
        os.path.join(git_dir, 'refs', '**', '*.lock'),
    ]
    for pat in patterns:
        for lock in glob.glob(pat, recursive=True):
            try:
                os.remove(lock)
                _log.warning('git_backup: removed stale git lock %s', lock)
            except FileNotFoundError:
                pass
            except OSError as e:
                _log.warning('git_backup: could not remove stale lock %s: %s',
                             lock, e)


def snapshot(runs_root: str, source_file: str, message: str) -> None:
    """Copy `source_file` into `<runs_root>/_scripts/` and commit it.

    No-ops on missing source file or missing `git` on PATH (those are
    non-error conditions in environments without git).

    Concurrent callers serialize on `<runs_root>/_scripts.snapshot.lock` (an
    flock held outside the git tree). Whoever holds it first clears any stale
    `.git/*.lock` left by a killed run, then commits. If the lock cannot be
    acquired within the timeout, the snapshot is skipped (logged, non-fatal).

    All other failures — `git init`, `git add`, `git commit`, the source
    copy itself — are logged at WARNING with the underlying stderr and
    re-raised. The caller decides whether to swallow them (the production
    caller `_try_snapshot` does, so a snapshot failure never blocks a run,
    but it's no longer silent).

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

    # Serialize snapshots across processes. The lock file lives OUTSIDE the
    # git tree (sibling of _scripts/) so `git add -A` never stages it.
    lock_path = os.path.join(runs_root, '_scripts.snapshot.lock')
    with open(lock_path, 'w') as lock_fh:
        deadline = time.monotonic() + _LOCK_TIMEOUT_S
        while True:
            try:
                fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    _log.warning('git_backup: snapshot lock busy >%.0fs at %s; '
                                 'skipping snapshot', _LOCK_TIMEOUT_S, lock_path)
                    return
                time.sleep(0.2)

        # We hold exclusivity: any git lock present is stale from a killed run.
        _clear_stale_git_locks(repo)

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
