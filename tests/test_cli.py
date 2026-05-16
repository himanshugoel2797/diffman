"""Tests for the new chain-aware CLI subcommands.

Each command exercises its parsing + output path end-to-end. Pipeline
modules + a chain are written under a tmp scan-root via the existing
make_pipeline fixture so the CLI's discovery sweep finds them.
"""

from __future__ import annotations

import json
import sys

import pytest

import diffman as dm
from diffman import discovery
from diffman.cli import main


# ---------------------------------------------------------------------------
# Shared fixture: two pipelines + one chain + one fork of the chain
# ---------------------------------------------------------------------------

PIPELINE_FWD = """
    import diffman as dm
    dm.register('base', n=1)
    dm.register('jitter', n=2)
    def _f(ctx):
        import os, tempfile
        p = os.path.join(tempfile.mkdtemp(), 'v.txt')
        open(p, 'w').write(str(ctx.variant.config['n']))
        ctx.artifact('s', 'v.txt', p)
        ctx.metric('s', 'flux', ctx.variant.config['n'] * 10)
    PIPELINE = dm.Pipeline('fwd', [dm.Stage('s', _f, config_keys=('n',))])
"""

PIPELINE_RECON = """
    import diffman as dm
    dm.register('ePIE', iters=100)
    def _f(ctx):
        import os, tempfile
        src = ctx.upstream_artifact('forward', 'stages/s/outputs/v.txt')
        n = int(open(src).read())
        p = os.path.join(tempfile.mkdtemp(), 'r.txt')
        open(p, 'w').write(str(n * ctx.variant.config['iters']))
        ctx.artifact('r', 'r.txt', p)
        ctx.metric('r', 'iters', ctx.variant.config['iters'])
    PIPELINE = dm.Pipeline('rec', [dm.Stage('r', _f, config_keys=('iters',))])
"""

CHAIN_MOD = """
    import diffman as dm
    import _diffman_cli_fwd as fwd
    import _diffman_cli_rec as rec
    CHAIN = dm.Chain('mychain', steps=[
        dm.ChainStep('forward', fwd.PIPELINE),
        dm.ChainStep('recon',   rec.PIPELINE, consumes=('forward',)),
    ])
    CHAIN.variation('baseline', forward='base',   recon='ePIE')
    CHAIN.variation('jittered', base='baseline', forward='jitter')
"""


@pytest.fixture
def cli_env(scan_root, make_pipeline, monkeypatch):
    make_pipeline('_diffman_cli_fwd',   PIPELINE_FWD)
    make_pipeline('_diffman_cli_rec',   PIPELINE_RECON)
    make_pipeline('_diffman_cli_chain', CHAIN_MOD)
    #Run baseline so progress / scoreboard have data.
    discovery.load_module('_diffman_cli_chain')
    mod = sys.modules['_diffman_cli_chain']
    rr = dm.RunRegistry(root=str(scan_root / 'runs'))
    mod.CHAIN.variations['baseline'].run(rr)
    monkeypatch.chdir(scan_root)   #commands default --scan-root='.' and --root='runs'
    return scan_root


# ---------------------------------------------------------------------------
# dm chains
# ---------------------------------------------------------------------------

class TestCmdChains:
    def test_lists_discovered_chains(self, cli_env, capsys):
        rc = main(['chains'])
        assert rc == 0
        out = capsys.readouterr().out
        assert 'mychain' in out
        assert 'baseline' in out and 'jittered' in out
        assert 'forward' in out and 'recon' in out

    def test_json_emits_structured_output(self, cli_env, capsys):
        rc = main(['chains', '--json'])
        assert rc == 0
        data = json.loads(capsys.readouterr().out)
        names = {c['name'] for c in data}
        assert 'mychain' in names
        my = next(c for c in data if c['name'] == 'mychain')
        assert my['steps'] == ['forward', 'recon']
        assert set(my['variations']) == {'baseline', 'jittered'}

    def test_friendly_message_when_nothing_discovered(self, scan_root,
                                                       monkeypatch, capsys):
        monkeypatch.chdir(scan_root)
        rc = main(['chains'])
        assert rc == 0
        assert 'no chains discovered' in capsys.readouterr().out


# ---------------------------------------------------------------------------
# dm chain <name>
# ---------------------------------------------------------------------------

class TestCmdChainShow:
    def test_prints_chain_detail_as_json(self, cli_env, capsys):
        rc = main(['chain', 'mychain'])
        assert rc == 0
        d = json.loads(capsys.readouterr().out)
        assert d['name'] == 'mychain'
        assert d['parent'] is None
        steps = {s['name']: s for s in d['steps']}
        assert steps['recon']['consumes'] == ['forward']
        baseline = next(v for v in d['variations']
                        if v['name'] == 'baseline')
        assert baseline['mapping'] == {'forward': 'base', 'recon': 'ePIE'}

    def test_unknown_chain_exits_nonzero(self, cli_env, capsys):
        with pytest.raises(SystemExit) as ei:
            main(['chain', 'ghost'])
        assert ei.value.code == 1
        assert 'not found' in capsys.readouterr().err


# ---------------------------------------------------------------------------
# dm progress
# ---------------------------------------------------------------------------

class TestCmdProgress:
    def test_progress_reports_done_steps_after_a_run(self, cli_env, capsys):
        rc = main(['progress', 'mychain', 'baseline'])
        assert rc == 0
        out = capsys.readouterr().out
        assert 'forward' in out and 'done' in out
        assert 'recon' in out

    def test_pending_for_a_variation_without_runs(self, cli_env, capsys):
        rc = main(['progress', 'mychain', 'jittered'])
        assert rc == 0
        out = capsys.readouterr().out
        #jittered hasn't been run — both steps should be pending.
        assert out.count('pending') == 2

    def test_unknown_variation_exits_nonzero(self, cli_env, capsys):
        rc = main(['progress', 'mychain', 'ghost'])
        assert rc == 1


# ---------------------------------------------------------------------------
# dm scoreboard
# ---------------------------------------------------------------------------

class TestCmdScoreboard:
    def test_scoreboard_lists_metrics_across_variations(self, cli_env, capsys):
        rc = main(['scoreboard', 'mychain'])
        assert rc == 0
        out = capsys.readouterr().out
        assert 'forward.s.flux' in out
        assert 'recon.r.iters' in out
        assert 'baseline' in out

    def test_baseline_renders_numeric_deltas(self, cli_env, capsys):
        #Run jittered too so deltas have something to compute against.
        discovery.load_module('_diffman_cli_chain')
        mod = sys.modules['_diffman_cli_chain']
        rr = dm.RunRegistry(root=str(cli_env / 'runs'))
        mod.CHAIN.variations['jittered'].run(rr)
        rc = main(['scoreboard', 'mychain', '--baseline', 'baseline'])
        assert rc == 0
        out = capsys.readouterr().out
        #baseline.flux=10, jittered.flux=20 → delta +10.
        jittered_line = next(l for l in out.splitlines()
                             if l.startswith('jittered'))
        assert '+10' in jittered_line

    def test_unknown_baseline_exits_nonzero(self, cli_env, capsys):
        rc = main(['scoreboard', 'mychain', '--baseline', 'ghost'])
        assert rc == 1

    def test_empty_message_when_no_metrics_recorded(
            self, scan_root, make_pipeline, monkeypatch, capsys):
        #A chain with no run yet has no metrics.
        make_pipeline('_diffman_cli_emptyfwd', """
            import diffman as dm
            dm.register('base', x=1)
            def _f(ctx): pass
            PIPELINE = dm.Pipeline('emp', [dm.Stage('s', _f)])
        """)
        make_pipeline('_diffman_cli_emptych', """
            import diffman as dm
            import _diffman_cli_emptyfwd as fwd
            CHAIN = dm.Chain('emptychain', steps=[
                dm.ChainStep('s', fwd.PIPELINE)])
            CHAIN.variation('v', s='base')
        """)
        monkeypatch.chdir(scan_root)
        rc = main(['scoreboard', 'emptychain'])
        assert rc == 0
        assert 'no metrics' in capsys.readouterr().out
