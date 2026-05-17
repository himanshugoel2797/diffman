"""Snapshot gating under MPI: only rank 0 should touch the git repo.

The check is env-var based (no mpi4py import) so we drive it by
manipulating `os.environ` directly.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from diffman import core


@pytest.fixture
def clean_mpi_env(monkeypatch):
    """Strip every recognized MPI rank var so the test starts from rank 0."""
    for var in core._MPI_RANK_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_mpi_rank_defaults_to_zero(clean_mpi_env):
    assert core._mpi_rank() == 0


@pytest.mark.parametrize('var', list(core._MPI_RANK_VARS))
def test_mpi_rank_reads_each_supported_var(clean_mpi_env, var):
    clean_mpi_env.setenv(var, '3')
    assert core._mpi_rank() == 3


def test_mpi_rank_skips_unparseable(clean_mpi_env):
    #A garbage value is ignored — fall through to the next var / default 0.
    clean_mpi_env.setenv('PMI_RANK', 'not-a-number')
    clean_mpi_env.setenv('SLURM_PROCID', '7')
    assert core._mpi_rank() == 7


def test_try_snapshot_skipped_for_nonzero_rank(clean_mpi_env, tmp_path):
    clean_mpi_env.setenv('PMI_RANK', '1')
    src = tmp_path / 'pipe.py'
    src.write_text('x = 1\n')
    with mock.patch('diffman.git_backup.snapshot') as mocked:
        core._try_snapshot(str(tmp_path / 'runs'), str(src), 'm')
    mocked.assert_not_called()


def test_try_snapshot_runs_on_rank_zero(clean_mpi_env, tmp_path):
    clean_mpi_env.setenv('PMI_RANK', '0')
    src = tmp_path / 'pipe.py'
    src.write_text('x = 1\n')
    with mock.patch('diffman.git_backup.snapshot') as mocked:
        core._try_snapshot(str(tmp_path / 'runs'), str(src), 'm')
    mocked.assert_called_once()


def test_try_snapshot_runs_without_mpi_env(clean_mpi_env, tmp_path):
    src = tmp_path / 'pipe.py'
    src.write_text('x = 1\n')
    with mock.patch('diffman.git_backup.snapshot') as mocked:
        core._try_snapshot(str(tmp_path / 'runs'), str(src), 'm')
    mocked.assert_called_once()
