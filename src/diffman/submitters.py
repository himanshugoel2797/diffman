"""Job submission backends: local subprocess + SLURM sbatch wrapper."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import time
from typing import Optional


class Submitter:
    kind = 'base'

    def submit(self, cmd: list[str], *, cwd=None, env=None, log_dir=None,
               extra_flags: Optional[list[str]] = None) -> dict:
        raise NotImplementedError


class LocalSubmitter(Submitter):
    """Fire-and-forget subprocess launcher."""
    kind = 'local'

    def submit(self, cmd, *, cwd=None, env=None, log_dir=None,
               extra_flags=None) -> dict:
        os.makedirs(log_dir or '.', exist_ok=True)
        log_path = os.path.join(log_dir or '.', 'job.log')
        f = open(log_path, 'ab')
        ts = time.strftime('%Y-%m-%dT%H:%M:%S')
        f.write(f'--- {ts}: {shlex.join(cmd)} ---\n'.encode())
        f.flush()
        p = subprocess.Popen(cmd, cwd=cwd, env=env,
                             stdout=f, stderr=subprocess.STDOUT,
                             start_new_session=True)
        return {'id': f'pid-{p.pid}', 'kind': self.kind,
                'pid': p.pid, 'log': log_path}


class SlurmSubmitter(Submitter):
    """sbatch-based submitter; writes a wrapper script then invokes sbatch --parsable."""
    kind = 'slurm'

    def __init__(self, sbatch_flags: Optional[list[str]] = None):
        self.sbatch_flags = list(sbatch_flags or ())

    def submit(self, cmd, *, cwd=None, env=None, log_dir=None,
               extra_flags=None) -> dict:
        os.makedirs(log_dir or '.', exist_ok=True)
        script_path = os.path.join(log_dir or '.', 'sbatch.sh')
        log_path = os.path.join(log_dir or '.', 'slurm-%j.out')
        #Per-launch `extra_flags` are appended after server-level defaults,
        #so a duplicate `--partition=foo` on the launch overrides the
        #server default (sbatch honors the last directive).
        flags = list(self.sbatch_flags) + list(extra_flags or ())
        with open(script_path, 'w') as f:
            f.write('#!/bin/bash\n')
            f.write(f'#SBATCH --output={log_path}\n')
            for flag in flags:
                f.write(f'#SBATCH {flag}\n')
            f.write(f'cd {shlex.quote(cwd or os.getcwd())}\n')
            f.write(f'exec {shlex.join(cmd)}\n')
        os.chmod(script_path, 0o755)
        out = subprocess.check_output(
            ['sbatch', '--parsable', script_path],
            cwd=cwd, env=env).decode().strip()
        return {'id': f'slurm-{out}', 'kind': self.kind,
                'job_id': out, 'log': log_path, 'script': script_path}


def default_submitter(choice: str = 'auto',
                      sbatch_flags: Optional[list[str]] = None) -> Submitter:
    """Pick a submitter. choice ∈ {'auto', 'local', 'slurm'}."""
    if choice == 'local':
        return LocalSubmitter()
    if choice == 'slurm':
        return SlurmSubmitter(sbatch_flags=sbatch_flags)
    if shutil.which('sbatch') is not None:
        return SlurmSubmitter(sbatch_flags=sbatch_flags)
    return LocalSubmitter()
