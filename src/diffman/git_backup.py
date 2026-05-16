"""Best-effort git-backed snapshot of pipeline scripts.

Maintains <runs_root>/_scripts/.git/ with one commit per Pipeline.run().
"""

from __future__ import annotations

import os
import shutil
import subprocess


def snapshot(runs_root: str, source_file: str, message: str) -> None:
    """Copy `source_file` into `<runs_root>/_scripts/` and commit.

    Silently no-ops on missing git, missing file, or any failure. Initializes
    the repo on first call.
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
            subprocess.run(['git', 'init', '-q'], cwd=repo, check=True,
                           env=env, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            return
    dst = os.path.join(repo, os.path.basename(source_file))
    try:
        shutil.copy2(source_file, dst)
        subprocess.run(['git', 'add', '-A'], cwd=repo, check=False,
                       env=env, stderr=subprocess.DEVNULL)
        subprocess.run(['git', 'commit', '-qm', message,
                        '--allow-empty', '--no-gpg-sign'],
                       cwd=repo, check=False, env=env,
                       stderr=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL)
    except Exception:
        pass
