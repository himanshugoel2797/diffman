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
from .core import registry, RunRegistry


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


def _all_chains(scan_root: str) -> list:
    """Import every discovered module and return every chain it exposes,
    deduplicated by chain name. Used by `dm chains` / `dm chain show`."""
    discovery.discover(scan_root)
    seen = {}
    for entry in discovery.DISCOVERED_LIST:
        try:
            mod = discovery.load_module(entry['module'])
        except Exception:
            continue
        for ch in discovery.chains_in_module(mod):
            seen.setdefault(ch.name, ch)
    return list(seen.values())


def _find_chain(name: str, scan_root: str):
    for ch in _all_chains(scan_root):
        if ch.name == name:
            return ch
    print(f'chain {name!r} not found under {scan_root!r}', file=sys.stderr)
    sys.exit(1)


def _cmd_chains(args):
    chains = _all_chains(args.scan_root)
    if not chains:
        print(f'(no chains discovered under {os.path.abspath(args.scan_root)})')
        return 0
    if args.json:
        print(json.dumps(
            [{'name': c.name, 'parent': c.parent,
              'steps': [s.name for s in c.steps],
              'variations': list(c.variations)}
             for c in chains], indent=2))
        return 0
    for c in sorted(chains, key=lambda c: c.name):
        parent = f' (forked from {c.parent})' if c.parent else ''
        print(f'{c.name}{parent}')
        print(f'  steps:      {", ".join(s.name for s in c.steps)}')
        print(f'  variations: {", ".join(c.variations) or "(none)"}')
    return 0


def _cmd_chain_show(args):
    ch = _find_chain(args.chain, args.scan_root)
    out = {'name': ch.name, 'parent': ch.parent,
           'steps': [{'name': s.name, 'pipeline': s.pipeline.name,
                      'consumes': list(s.consumes)} for s in ch.steps],
           'variations': []}
    for v in ch.variations.values():
        try:
            mapping, err = v.resolve(), None
        except Exception as e:
            mapping, err = dict(v.overrides), str(e)
        out['variations'].append({'name': v.name, 'base': v.base,
                                  'forks_of': v.forks_of,
                                  'mapping': mapping, 'error': err})
    print(json.dumps(out, indent=2, default=str))
    return 0


def _cmd_progress(args):
    """Per-step status of one variation, computed from on-disk run.json
    records. Mirrors /api/chain_progress; useful for "is jitter_low
    finished yet?" from a shell."""
    from .server import _resolve_variation_runs, _summarize_stage_status
    ch = _find_chain(args.chain, args.scan_root)
    if args.variation not in ch.variations:
        print(f'variation {args.variation!r} not in chain {args.chain!r}',
              file=sys.stderr)
        return 1
    var = ch.variations[args.variation]
    matched = _resolve_variation_runs(ch, var, RunRegistry(args.root).list_runs())
    try:
        mapping = var.resolve()
    except Exception as e:
        print(f'variation does not resolve: {e}', file=sys.stderr); return 1
    width = max(len(s.name) for s in ch.steps)
    for step in ch.steps:
        r = matched[step.name]
        vname = mapping.get(step.name, '?')
        status = ('unspecified' if mapping.get(step.name) is None
                  else _summarize_stage_status(r.stage_status)
                  if r is not None else 'pending')
        fp = f'  [{r.fingerprint[:12]}]' if r else ''
        print(f'  {step.name:<{width}}  {status:<8}  ({step.pipeline.name}/{vname}){fp}')
    return 0


def _cmd_scoreboard(args):
    """Print the variation × metric table for a chain. Mirrors
    /api/scoreboard; -b/--baseline picks a row whose metrics other rows
    are shown as deltas against (numeric only)."""
    from .server import _resolve_variation_runs, _load_stage_metrics
    ch = _find_chain(args.chain, args.scan_root)
    all_runs = RunRegistry(args.root).list_runs()
    rows = []
    keys: set = set()
    for var_name, var in ch.variations.items():
        try:
            matched = _resolve_variation_runs(ch, var, all_runs)
        except KeyError:
            continue
        flat: dict = {}
        for step in ch.steps:
            r = matched[step.name]
            if r is None:
                continue
            for st_name, st_metrics in _load_stage_metrics(r.fdir):
                for k, v in st_metrics.items():
                    flat[f'{step.name}.{st_name}.{k}'] = v
                    keys.add(f'{step.name}.{st_name}.{k}')
        rows.append({'variation': var_name, 'metrics': flat})
    if args.baseline:
        base = next((r for r in rows if r['variation'] == args.baseline), None)
        if base is None:
            print(f'baseline variation {args.baseline!r} has no row',
                  file=sys.stderr); return 1
    if not keys:
        print('(no metrics recorded — stages must call ctx.metric(...))')
        return 0
    keys_sorted = sorted(keys)
    var_w = max(len(r['variation']) for r in rows)
    print(f'{"variation":<{var_w}}  ' + '  '.join(keys_sorted))
    base_metrics = (next(r['metrics'] for r in rows
                         if r['variation'] == args.baseline)
                    if args.baseline else None)
    for r in rows:
        cells = []
        for k in keys_sorted:
            v = r['metrics'].get(k, '—')
            if base_metrics is not None and r['variation'] != args.baseline \
                    and isinstance(v, (int, float)) \
                    and isinstance(base_metrics.get(k), (int, float)):
                delta = v - base_metrics[k]
                cells.append(f'{v} ({delta:+.4g})')
            else:
                cells.append(str(v))
        print(f'{r["variation"]:<{var_w}}  ' + '  '.join(cells))
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

    p_chains = sub.add_parser(
        'chains',
        help='list every chain discovered under SCAN_ROOT',
        description='Walk SCAN_ROOT, import every discovered module, and '
                    'list each `dm.Chain` it exposes along with the chain\'s '
                    'parent, steps, and variations.',
    )
    p_chains.add_argument('--scan-root', default='.', metavar='DIR')
    p_chains.add_argument('--json', action='store_true',
                          help='emit JSON instead of grouped text')

    p_show = sub.add_parser(
        'chain',
        help='print one chain\'s steps + variations as JSON',
    )
    p_show.add_argument('chain', help='chain name')
    p_show.add_argument('--scan-root', default='.', metavar='DIR')

    p_prog = sub.add_parser(
        'progress',
        help='per-step status of one chain variation, from disk',
        description='Compute chain progress by walking the upstream-fp '
                    'provenance recorded in each run.json. Same logic as '
                    '/api/chain_progress.',
    )
    p_prog.add_argument('chain', help='chain name')
    p_prog.add_argument('variation', help='variation name')
    p_prog.add_argument('--root', default='runs', metavar='DIR',
                        help='run directory root')
    p_prog.add_argument('--scan-root', default='.', metavar='DIR')

    p_sb = sub.add_parser(
        'scoreboard',
        help='variation × metric table for a chain',
        description='Aggregate every stages/<stage>/metrics.json across '
                    'a chain\'s variations into one table. `--baseline` '
                    'shows numeric metrics as deltas vs that row.',
    )
    p_sb.add_argument('chain', help='chain name')
    p_sb.add_argument('-b', '--baseline', metavar='VAR',
                      help='variation whose metrics other rows are shown '
                           'as deltas against')
    p_sb.add_argument('--root', default='runs', metavar='DIR')
    p_sb.add_argument('--scan-root', default='.', metavar='DIR')

    args = ap.parse_args(argv)
    if args.cmd == 'scan':       return _cmd_scan(args)
    if args.cmd == 'list':       return _cmd_list(args)
    if args.cmd == 'describe':   return _cmd_describe(args)
    if args.cmd == 'serve':      return _cmd_serve(args)
    if args.cmd == 'chains':     return _cmd_chains(args)
    if args.cmd == 'chain':      return _cmd_chain_show(args)
    if args.cmd == 'progress':   return _cmd_progress(args)
    if args.cmd == 'scoreboard': return _cmd_scoreboard(args)
    ap.print_help()
    return 1


if __name__ == '__main__':
    sys.exit(main())
