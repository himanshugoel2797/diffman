# diffman

Variant/cache/run manager for simulation pipelines, with a live-preview browser UI.

Started as a sub-component of the SRW ptychography simulator
(`srwl_uti_diffman` in [SRW](../xpp_nnl_dataset_gpu/smp_to_det/SRW)); spun out
here to grow features that don't fit the stdlib-only single-file design,
notably:

- **FastAPI + WebSocket** server for push-based updates
- **Interactive plots** of stage inputs/outputs (Plotly, h5py, numpy)
- **Live previews** via watchdog filesystem monitoring
- **Stage I/O preview pane** — click a stage to see its inputs (upstream
  outputs) and its own outputs rendered inline

The original SRW-resident `srwl_uti_diffman.py` is left in place; this
package is a richer, standalone evolution.

## Install

### pixi (recommended)

Pixi provisions a project-local conda+pypi env including `srwpy`, so the
SRW-aware previews work out of the box:

```bash
pixi install                 # build the env
pixi run scan                # discover pipeline modules in cwd
pixi run serve               # launch UI at http://127.0.0.1:8765
pixi shell                   # or drop into the env and run `diffman …`
```

### pip

```bash
pip install -e ".[all]"      # core + h5py + plotly
pip install srwpy            # optional: SRW-aware previews (.h5 / .dat)
```

`[all]` pulls in h5py + plotly. The generic preview path works without them
but degrades HDF5/plot views to a download link.

## CLI

```
diffman scan [root]                                 # discover pipeline modules
diffman list <module>                               # list variants
diffman describe <module> <variant> [--var k=v]*
diffman run <module> <variant> [--only S,...] [--force S,...] [--var k=v]*
diffman serve [--root DIR] [--port N] [--scan-root DIR]
              [--submitter auto|local|slurm] [--sbatch "flags"]
```

`--var` accepts dotted-path overrides (e.g. `scan.width=1e-6`,
`detector.apply_noise=true`). Inline overrides synthesize a *fork* variant
whose name is `<base>+<short-fp>`; each unique override set gets its own
run directory.

## Pipeline module

A pipeline module is any importable `.py` file that registers variants and
exposes a `PIPELINE` attribute:

```python
import diffman as dm

dm.register('mysim_base', scan=dict(width=5e-6, step=1e-7),
            detector=dict(apply_noise=False))
dm.register('mysim_noisy', base='mysim_base',
            detector=dict(apply_noise=True))

def _stage_simulate(ctx):
    cfg = ctx.variant.config
    # ... do work, write to ctx.artifact('simulate', 'frame_%d.png')
    return {'frames': ctx.stage_dir('simulate')}

PIPELINE = dm.Pipeline('mysim', [
    dm.Stage('simulate', _stage_simulate, config_keys=('scan', 'detector')),
])
```

The server auto-discovers any `.py` file under `--scan-root` containing
both `diffman` (import) and `PIPELINE` (assignment).

## On-disk layout

```
<runs-root>/
    <pipeline>/<variant>/<short-fp>/
        run.json                   # SRWLDfmRunRecord (all-text)
        config.json                # effective variant config
        run.log
        stages/<stage>/
            outputs/...            # artifacts (any format)
            _meta.json
            _key
    _scripts/                      # git-tracked backup of pipeline scripts
        .git/
        <module>.py
    _jobs/<fork>_<fp>/             # per-launch logs (job.log or slurm-*.out)
```

All app-managed state is JSON or git — text-friendly for version control.
