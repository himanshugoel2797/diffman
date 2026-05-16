"""diffman CLI: discover pipeline modules, inspect variants, serve the UI.

No `run` / launch subcommand — diffman is a viewer. Users invoke their
own pipeline modules to produce run directories; diffman shows them.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from . import discovery
from .core import registry


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
    for n in registry.for_module(args.module):
        print(n)
    return 0


def _cmd_describe(args):
    discovery.load_module(args.module)
    v = registry.get(args.module, args.variant)
    print(json.dumps({
        'name': v.name,
        'module': v.module,
        'fingerprint': v.fingerprint,
        'config': dict(v.config),
    }, indent=2, default=str))
    return 0


def _cmd_serve(args):
    from .server import run as run_server
    run_server(root=args.root, port=args.port, bind=args.bind,
               scan_root=args.scan_root, no_scan=args.no_scan)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog='diffman',
        description=(
            'Track the graph of related simulation pipelines (forks) and '
            'the parameter differences at each fork. Browse runs produced '
            'by those pipelines via a web UI. Does not launch runs.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = ap.add_subparsers(dest='cmd', metavar='COMMAND')

    p_scan = sub.add_parser(
        'scan',
        help='walk a directory and report every diffman pipeline module found',
        description=(
            'Walk ROOT recursively for .py files mentioning both "diffman" '
            'and "PIPELINE". No code is executed. The discovered paths are '
            'cached so subsequent commands can import them by module name.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_scan.add_argument('root', nargs='?', default='.',
                        help='directory to scan (recursively)')
    p_scan.add_argument('--json', action='store_true',
                        help='emit JSON instead of grouped text')

    p_list = sub.add_parser(
        'list',
        help='print the variant names registered by one pipeline module',
        description=(
            'Import MODULE and print the names of the variants it registered. '
            'Variants are attributed to the module that called dm.register(), '
            'so unrelated pipelines do not bleed into this listing.'),
    )
    p_list.add_argument('module',
                        help='pipeline module name (e.g. APS_ptycho_v2)')

    p_desc = sub.add_parser(
        'describe',
        help='print a variant\'s fully-resolved config + fingerprint',
        description=(
            'Resolve VARIANT in MODULE (deep-merging its inheritance chain) '
            'and print the merged config plus a stable sha256 fingerprint.'),
    )
    p_desc.add_argument('module', help='pipeline module name')
    p_desc.add_argument('variant', help='variant name registered in MODULE')

    p_serve = sub.add_parser(
        'serve',
        help='start the browser UI for browsing pipelines, forks, and runs',
        description=(
            'Start the diffman browser UI. Discovers pipeline modules under '
            '--scan-root at startup, watches --root for run-directory '
            'changes, and exposes REST + WebSocket endpoints used by the '
            'bundled SPA. The UI is read-only — it does not submit runs.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_serve.add_argument('--root', default='runs', metavar='DIR',
                         help='root directory containing existing run dirs')
    p_serve.add_argument('--port', type=int, default=8765,
                         help='TCP port for the HTTP/WebSocket server')
    p_serve.add_argument(
        '--bind', default='127.0.0.1', metavar='ADDR',
        help=('interface to bind to. The UI has no auth; binding to a '
              'non-loopback address exposes it to anyone who can reach the port.'))
    p_serve.add_argument('--scan-root', default='.', metavar='DIR',
                         help='directory walked at startup for pipeline modules')
    p_serve.add_argument('--no-scan', action='store_true',
                         help='skip the startup scan')

    args = ap.parse_args(argv)
    if args.cmd == 'scan':     return _cmd_scan(args)
    if args.cmd == 'list':     return _cmd_list(args)
    if args.cmd == 'describe': return _cmd_describe(args)
    if args.cmd == 'serve':    return _cmd_serve(args)
    ap.print_help()
    return 1


if __name__ == '__main__':
    sys.exit(main())
