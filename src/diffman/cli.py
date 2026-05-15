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


def _make_fork(base, overrides):
    from .server import make_fork
    return make_fork(base, overrides)


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
    names = discovery.module_variants(args.module) or registry.names()
    for n in names:
        print(n)
    return 0


def _cmd_describe(args):
    discovery.load_module(args.module)
    base = registry.get(args.variant)
    fork = _make_fork(base, _parse_overrides(args.var))
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
    variant = _make_fork(base, _parse_overrides(args.var))
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
    ap = argparse.ArgumentParser(prog='diffman',
                                 description='Pipeline variant/cache/run manager + UI')
    sub = ap.add_subparsers(dest='cmd')

    p_scan = sub.add_parser('scan', help='discover pipeline modules')
    p_scan.add_argument('root', nargs='?', default='.')
    p_scan.add_argument('--json', action='store_true')

    p_list = sub.add_parser('list', help='list variants for a module')
    p_list.add_argument('module')

    p_desc = sub.add_parser('describe', help='print resolved variant config')
    p_desc.add_argument('module')
    p_desc.add_argument('variant')
    p_desc.add_argument('--var', action='append', default=[])

    p_run = sub.add_parser('run', help='execute a pipeline run')
    p_run.add_argument('module')
    p_run.add_argument('variant')
    p_run.add_argument('--only')
    p_run.add_argument('--force')
    p_run.add_argument('--var', action='append', default=[])
    p_run.add_argument('--runs-root', default='runs')

    p_serve = sub.add_parser('serve', help='start the browser UI')
    p_serve.add_argument('--root', default='runs')
    p_serve.add_argument('--port', type=int, default=8765)
    p_serve.add_argument('--bind', default='127.0.0.1')
    p_serve.add_argument('--submitter', default='auto',
                         choices=('auto', 'local', 'slurm'))
    p_serve.add_argument('--sbatch', default='')
    p_serve.add_argument('--scan-root', default='.')
    p_serve.add_argument('--no-scan', action='store_true')

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
