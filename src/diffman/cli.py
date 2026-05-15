"""diffman CLI: list / describe / run / scan / serve."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys

from . import discovery
from .core import RunRegistry, registry
from .submitters import default_submitter


def _parse_overrides(entries):
    from .server import parse_overrides
    return parse_overrides(entries)


def _make_override_variant(base, overrides):
    from .server import make_override_variant
    return make_override_variant(base, overrides)


def _cmd_scan(args):
    found = discovery.discover(args.root)
    if args.json:
        print(json.dumps(found, indent=2))
    else:
        cur = None
        for entry in found:
            if entry['dir'] != cur:
                cur = entry['dir']
                print(f"{cur}/")
            print(f"  {entry['module']}   ({entry['path']})")
        print(f"# {len(found)} module(s) under {os.path.abspath(args.root)}")
    return 0


def _cmd_list(args):
    discovery.load_module(args.module)
    names = registry.for_module(args.module) or discovery.module_variants(args.module)
    for n in names:
        print(n)
    return 0


def _cmd_describe(args):
    discovery.load_module(args.module)
    base = registry.get(args.variant)
    fork = _make_override_variant(base, _parse_overrides(args.var))
    print(json.dumps({
        'name': fork.name,
        'fingerprint': fork.fingerprint,
        'config': fork.config.merged(),
    }, indent=2, default=str))
    return 0


def _cmd_run(args):
    mod = discovery.load_module(args.module)
    if not hasattr(mod, 'PIPELINE'):
        raise SystemExit(f"module {args.module!r} has no PIPELINE attribute")
    base = registry.get(args.variant)
    variant = _make_override_variant(base, _parse_overrides(args.var))
    only = set(args.only.split(',')) if args.only else None
    force = set(args.force.split(',')) if args.force else None
    rr = RunRegistry(root=args.runs_root)
    rec = mod.PIPELINE.run(variant, rr, force=force, only=only)
    print('run dir:', rec.fdir)
    for k, st in rec.stage_status.items():
        print(f'  {k:10s} {st}')
    return 0


def _cmd_serve(args):
    from .server import run as run_server
    sbatch_flags = shlex.split(args.sbatch) if args.sbatch else None
    submitter = default_submitter(args.submitter, sbatch_flags=sbatch_flags)
    run_server(root=args.root, port=args.port, bind=args.bind,
               scan_root=args.scan_root, submitter=submitter,
               no_scan=args.no_scan)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog='diffman',
        description=(
            'Manage simulation pipelines: discover pipeline modules, list '
            'and inspect their variants (named config sets), execute runs '
            'with on-disk caching of per-stage outputs, and serve a browser '
            'UI to launch runs and preview artifacts.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = ap.add_subparsers(dest='cmd', metavar='COMMAND')

    # --- scan ----------------------------------------------------------------
    p_scan = sub.add_parser(
        'scan',
        help='walk a directory and report every diffman pipeline module found',
        description=(
            'Walk ROOT recursively looking for .py files whose source mentions '
            'both "diffman" and "PIPELINE". No code is executed. Discovered '
            'paths are cached so subsequent `diffman list/run` can import them '
            'by module name without a -m path.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_scan.add_argument(
        'root', nargs='?', default='.',
        help='directory to scan (recursively)')
    p_scan.add_argument(
        '--json', action='store_true',
        help='emit the result as JSON instead of a grouped text listing')

    # --- list ----------------------------------------------------------------
    p_list = sub.add_parser(
        'list',
        help='print the variant names registered by one pipeline module',
        description=(
            'Import MODULE (via the scan cache or PYTHONPATH) and print the '
            'names of every variant it registered via dm.register(). Variants '
            'are attributed to the module that registered them, so unrelated '
            'pipelines do not bleed into this list.'),
    )
    p_list.add_argument(
        'module', help='pipeline module name (e.g. APS_ptycho_v2, no .py)')

    # --- describe ------------------------------------------------------------
    p_desc = sub.add_parser(
        'describe',
        help='print the fully-resolved config for a variant (with overrides applied)',
        description=(
            'Resolve VARIANT in MODULE (deep-merging its inheritance chain), '
            'apply any --var overrides, and print the merged config plus its '
            'fingerprint. Useful for inspecting exactly what a run would see '
            'before launching it.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_desc.add_argument('module', help='pipeline module name')
    p_desc.add_argument('variant', help='variant name registered in MODULE')
    p_desc.add_argument(
        '--var', action='append', default=[], metavar='KEY=VALUE',
        help=('config override; dotted KEY descends into nested dicts. '
              'VALUE is parsed as a Python literal (numbers, bool, lists, '
              'strings). Repeatable. e.g. --var scan.width=1e-6 '
              '--var detector.apply_noise=true'))

    # --- run -----------------------------------------------------------------
    p_run = sub.add_parser(
        'run',
        help='execute a pipeline run (cached stages are reused)',
        description=(
            'Execute MODULE.PIPELINE under VARIANT. Each stage is keyed by '
            '(stage name, function source, restricted config, upstream keys); '
            'matching cache entries under <runs-root>/<pipeline>/<variant>/'
            '<short-fp>/stages/ are reused. Override variants from --var land '
            'in their own <variant>+<short-fp> run directory.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_run.add_argument('module', help='pipeline module name')
    p_run.add_argument('variant', help='variant name registered in MODULE')
    p_run.add_argument(
        '--only', metavar='STAGES',
        help=('run only these stages (comma-separated); others are marked '
              'skipped. Upstream cache entries must already exist.'))
    p_run.add_argument(
        '--force', metavar='STAGES',
        help=('re-run these stages (comma-separated) even if their cache key '
              'matches; downstream stages re-key off the new outputs.'))
    p_run.add_argument(
        '--var', action='append', default=[], metavar='KEY=VALUE',
        help='config override (see `diffman describe --var`). Repeatable.')
    p_run.add_argument(
        '--runs-root', default='runs', metavar='DIR',
        help='root directory where run artifacts and cache entries are stored')

    # --- serve ---------------------------------------------------------------
    p_serve = sub.add_parser(
        'serve',
        help='start the browser UI (FastAPI + WebSocket) for browsing and launching runs',
        description=(
            'Start the diffman browser UI. Discovers pipeline modules under '
            '--scan-root at startup, watches --root for filesystem changes, '
            'and exposes REST + WebSocket endpoints used by the bundled SPA. '
            'Launches submitted from the UI run via the chosen --submitter.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_serve.add_argument(
        '--root', default='runs', metavar='DIR',
        help='root directory containing existing runs and cache entries')
    p_serve.add_argument(
        '--port', type=int, default=8765,
        help='TCP port for the HTTP/WebSocket server')
    p_serve.add_argument(
        '--bind', default='127.0.0.1', metavar='ADDR',
        help=('interface to bind to. The UI has no auth — binding to a '
              'non-loopback address exposes launch/edit endpoints to anyone '
              'who can reach the port.'))
    p_serve.add_argument(
        '--submitter', default='auto',
        choices=('auto', 'local', 'slurm'),
        help=('how UI-triggered launches are executed. auto picks slurm if '
              'sbatch is on PATH, else local; local runs a subprocess on the '
              'server host; slurm submits via sbatch --parsable.'))
    p_serve.add_argument(
        '--sbatch', default='', metavar='"FLAGS"',
        help=('shell-quoted sbatch directives applied to every slurm '
              'submission as a baseline (the UI can append per-launch flags '
              'on top). e.g. --sbatch "--account=m2173 --time=01:00:00"'))
    p_serve.add_argument(
        '--scan-root', default='.', metavar='DIR',
        help='directory walked at startup to find pipeline modules')
    p_serve.add_argument(
        '--no-scan', action='store_true',
        help=('skip the startup scan; only modules importable from '
              'PYTHONPATH will be available'))

    args = ap.parse_args(argv)
    if args.cmd == 'scan':     return _cmd_scan(args)
    if args.cmd == 'list':     return _cmd_list(args)
    if args.cmd == 'describe': return _cmd_describe(args)
    if args.cmd == 'run':      return _cmd_run(args)
    if args.cmd == 'serve':    return _cmd_serve(args)
    ap.print_help()
    return 1


if __name__ == '__main__':
    sys.exit(main())
