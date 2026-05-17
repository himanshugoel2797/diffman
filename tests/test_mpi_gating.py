"""Snapshot gating under MPI: only rank 0 should touch the git repo.

The check is env-var based (no mpi4py import) so we drive it by
manipulating `os.environ` directly.
"""

from __future__ import annotations

import os
from pathlib import Path
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


_uid = [0]


@pytest.fixture
def run_ctx(clean_mpi_env, tmp_path):
    """A minimal RunContext bound to a real on-disk run dir.

    Each test gets a fresh (module, variant) pair so the process-global
    `dm.registry` doesn't reject re-registration across tests.
    """
    import diffman as dm
    _uid[0] += 1
    rr = dm.RunRegistry(str(tmp_path / 'runs'))
    v = dm.registry.register(f'rank_test_v_{_uid[0]}',
                             module=f'rank_test_mod_{_uid[0]}', x=1)
    ctx = rr.open_run('rank_test_pipe', v)
    yield ctx, tmp_path


def test_artifact_skips_filesystem_on_nonzero_rank(clean_mpi_env, run_ctx):
    ctx, tmp_path = run_ctx
    src = tmp_path / 'data.bin'
    src.write_bytes(b'hello')
    clean_mpi_env.setenv('PMI_RANK', '2')
    dest = ctx.artifact('mystage', 'out.bin', str(src))
    #Path returned for use by the caller, but nothing actually written.
    assert dest.endswith('/stages/mystage/outputs/out.bin')
    assert not os.path.exists(dest)


def test_artifact_writes_on_rank_zero(clean_mpi_env, run_ctx):
    ctx, tmp_path = run_ctx
    src = tmp_path / 'data.bin'
    src.write_bytes(b'hello')
    clean_mpi_env.setenv('PMI_RANK', '0')
    dest = ctx.artifact('mystage', 'out.bin', str(src))
    assert os.path.exists(dest)
    #Symlink path resolves to the source file on a supporting FS.
    assert os.path.realpath(dest) == os.path.realpath(str(src))


def test_artifact_recovers_from_partial_symlink_failure(clean_mpi_env,
                                                         run_ctx,
                                                         monkeypatch):
    """Simulate Lustre-style symlink-fails-but-leaves-target: the partial
    dest is wiped and the copy fallback succeeds."""
    ctx, tmp_path = run_ctx
    srcdir = tmp_path / 'recons'
    srcdir.mkdir()
    (srcdir / 'r.ptyr').write_bytes(b'data')

    real_symlink = os.symlink

    def flaky_symlink(src, dst, *a, **kw):
        #Leave a partial dest (mirror the buggy-FS behavior the user hit),
        #then raise.
        real_symlink(src, dst)
        raise OSError(17, 'simulated EEXIST after partial create')

    monkeypatch.setattr('os.symlink', flaky_symlink)
    clean_mpi_env.setenv('PMI_RANK', '0')
    dest = ctx.artifact('mystage', 'recons', str(srcdir))
    #The copy fallback ran successfully on top of the partial state.
    assert os.path.isdir(dest)
    assert (Path(dest) / 'r.ptyr').read_bytes() == b'data'


def test_metric_skipped_on_nonzero_rank(clean_mpi_env, run_ctx):
    ctx, tmp_path = run_ctx
    clean_mpi_env.setenv('SLURM_PROCID', '3')
    ctx.metric('mystage', 'score', 0.42)
    metrics = Path(ctx.fdir) / 'stages' / 'mystage' / 'metrics.json'
    assert not metrics.exists()


def test_metric_writes_on_rank_zero(clean_mpi_env, run_ctx):
    ctx, tmp_path = run_ctx
    import json
    clean_mpi_env.setenv('SLURM_PROCID', '0')
    ctx.metric('mystage', 'score', 0.42)
    metrics = Path(ctx.fdir) / 'stages' / 'mystage' / 'metrics.json'
    assert json.loads(metrics.read_text()) == {'score': 0.42}


def test_mpi_barrier_is_noop_without_mpi4py(clean_mpi_env):
    """No mpi4py imported -> barrier must return cleanly and not pull
    mpi4py in. Pre-condition: nothing imported it earlier in the test
    session; if it did, this test is a no-op (and the import-side-
    effect guarantee can't be tested here)."""
    import sys
    if 'mpi4py' in sys.modules or 'mpi4py.MPI' in sys.modules:
        pytest.skip('mpi4py already imported earlier in session')
    core.mpi_barrier()  # must not raise, must not import anything.
    assert 'mpi4py' not in sys.modules
    assert 'mpi4py.MPI' not in sys.modules


def test_mpi_barrier_calls_comm_world_when_mpi4py_present(monkeypatch):
    """When mpi4py *is* already in sys.modules and initialized, the
    barrier delegates to MPI.COMM_WORLD.Barrier."""
    import sys
    barrier_calls = []

    class FakeCommWorld:
        def Barrier(self):
            barrier_calls.append(True)

    class FakeMPI:
        COMM_WORLD = FakeCommWorld()
        @staticmethod
        def Is_initialized(): return True
        @staticmethod
        def Is_finalized(): return False

    fake_mpi4py = type(sys)('mpi4py')
    fake_mpi4py.MPI = FakeMPI
    monkeypatch.setitem(sys.modules, 'mpi4py', fake_mpi4py)
    monkeypatch.setitem(sys.modules, 'mpi4py.MPI', FakeMPI)
    core.mpi_barrier()
    assert barrier_calls == [True]


def test_mpi_rank_public_alias_matches_private(clean_mpi_env):
    clean_mpi_env.setenv('PMI_RANK', '4')
    assert core.mpi_rank() == 4 == core._mpi_rank()
