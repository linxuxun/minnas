#!/usr/bin/env python3
"""MiniNAS CLI - Command line interface."""
import sys
import argparse
from pathlib import Path
from datetime import datetime

from .repo import Repo
from .namespace import NamespaceStore, NamespaceExistsError, NamespaceNotFoundError
from .branch import BranchManager, BranchNotFoundError, BranchExistsError


def _color(text, code):
    return f"\033[{code}m{text}\033[0m"


GREEN = lambda t: _color(t, 32)
RED   = lambda t: _color(t, 31)
YEL   = lambda t: _color(t, 33)
BLUE  = lambda t: _color(t, 34)
BOLD  = lambda t: _color(t, 1)


def cmd_init(args):
    path = Path(args.path).expanduser().resolve()
    backend = args.backend or 'local'
    ns_name = args.namespace or 'default'
    try:
        repo = Repo.init(path, namespace=ns_name, backend=backend)
        print(f"{GREEN('✓')} Initialized MinNAS repository at {path}")
        print(f"  namespace: {BLUE(ns_name)}  backend: {BLUE(backend)}")
    except Exception as e:
        print(f"{RED('✗')} Init failed: {e}", file=sys.stderr)
        return 1


def cmd_status(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        st = repo.status()
        print(f"{BOLD('MiniNAS Status')}")
        print(f"  repo:     {BLUE(str(path))}")
        print(f"  branch:   {BLUE(repo.current_branch() or '(detached)')}")
        sn = repo.current_snapshot()
        print(f"  snapshot: {BLUE(sn[:8] if sn else 'none')}")
        m = st.get('modified', [])
        a = st.get('added', [])
        d = st.get('deleted', [])
        if m or a or d:
            for f in m:  print(f"  {YEL('M')} {f}")
            for f in a:  print(f"  {GREEN('A')} {f}")
            for f in d:  print(f"  {RED('D')} {f}")
        else:
            print(f"  {GREEN('(clean)')}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_commit(args):
    path = Path(args.repo or '.').expanduser().resolve()
    author = args.author or 'anonymous'
    try:
        repo = Repo(path)
        sha = repo.commit(args.message, author=author)
        print(f"{GREEN('✓')} Committed: {BLUE(sha[:8])}  {args.message}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} Commit failed: {e}", file=sys.stderr)
        return 1


def cmd_log(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        entries = repo.log(n=args.n or 10)
        if not entries:
            print("(no commits yet)")
            return 0
        for sha, msg, author, t in entries:
            ts = datetime.fromisoformat(t).strftime('%Y-%m-%d %H:%M')
            print(f"{BLUE(sha[:8])} {ts} {YEL(author)}")
            print(f"    {msg}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_snapshot_list(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        snaps = repo.list_snapshots()
        if not snaps:
            print("(no snapshots)")
            return 0
        for sha, msg, t in snaps:
            ts = datetime.fromisoformat(t).strftime('%Y-%m-%d %H:%M')
            print(f"{BLUE(sha[:8])} {ts}")
            print(f"    {msg}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_snapshot_show(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        s = repo.get_snapshot(args.sha)
        print(f"{BOLD('Snapshot')}: {args.sha[:8]}")
        print(f"  message:   {s.get('message','')}")
        print(f"  author:    {s.get('author','')}")
        print(f"  time:      {s.get('timestamp','')}")
        print(f"  parent:    {s.get('parent', 'none')}")
        print(f"  files:     {len(s.get('tree', {}))}")
        for p, bs in s.get('tree', {}).items():
            print(f"    {BLUE(bs[:8])}  {p}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_snapshot_checkout(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        repo.checkout_snapshot(args.sha)
        print(f"{GREEN('✓')} Checked out snapshot {args.sha[:8]}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_branch_list(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        bm = BranchManager(path)
        current = bm.get_current_branch()
        for name, sha in bm.list_branches():
            mark = '→' if name == current else ' '
            cur  = ' (current)' if name == current else ''
            print(f"  {mark} {BLUE(name):20s} {sha[:8]}{cur}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_branch_create(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        bm = BranchManager(path)
        sha = args.sha or bm.get_current_sha() or None
        bm.create_branch(args.name, sha)
        print(f"{GREEN('✓')} Created branch {BLUE(args.name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_branch_checkout(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        bm = BranchManager(path)
        bm.checkout(args.name)
        print(f"{GREEN('✓')} Switched to branch {BLUE(args.name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_branch_delete(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        bm = BranchManager(path)
        bm.delete_branch(args.name)
        print(f"{GREEN('✓')} Deleted branch {BLUE(args.name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_ns_list(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        ns = NamespaceStore(path)
        for name in ns.list_namespaces():
            print(f"  {BLUE(name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_ns_create(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        ns = NamespaceStore(path)
        ns.create_namespace(args.name)
        print(f"{GREEN('✓')} Created namespace {BLUE(args.name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_ns_switch(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        ns = NamespaceStore(path)
        ns.switch_namespace(args.name)
        print(f"{GREEN('✓')} Switched to namespace {BLUE(args.name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_ns_delete(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        ns = NamespaceStore(path)
        ns.delete_namespace(args.name)
        print(f"{GREEN('✓')} Deleted namespace {BLUE(args.name)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_fs_ls(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        entries = repo.fs.listdir(args.path or '/')
        for e in entries:
            print(f"  {e}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_fs_cat(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        fd = repo.fs.open(args.path, 'r')
        data = repo.fs.read(fd)
        repo.fs.close(fd)
        sys.stdout.write(data.decode(errors='replace'))
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_fs_write(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        fd = repo.fs.open(args.path, 'w')
        repo.fs.write(fd, args.content.encode())
        repo.fs.close(fd)
        print(f"{GREEN('✓')} Wrote {len(args.content)} bytes to {BLUE(args.path)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_fs_rm(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        repo.fs.rm(args.path)
        print(f"{GREEN('✓')} Removed {BLUE(args.path)}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_diff(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        changes = repo.diff(args.sha1, args.sha2)
        for c in changes:
            print(f"  {c['action']:8s}  {c['path']}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_gc(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        freed = repo.gc()
        print(f"{GREEN('✓')} GC freed {freed} unreferenced object(s)")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_stats(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        st = repo.stats()
        print(f"{BOLD('Repository Stats')}")
        for k, v in st.items():
            print(f"  {k:20s}: {v}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def cmd_debug(args):
    path = Path(args.repo or '.').expanduser().resolve()
    try:
        repo = Repo(path)
        issues = repo.verify()
        if not issues:
            print(f"{GREEN('✓')} Repository is consistent")
        else:
            for iss in issues:
                print(f"{RED('✗')} {iss}")
        return 0
    except Exception as e:
        print(f"{RED('✗')} {e}", file=sys.stderr)
        return 1


def main():
    parser = argparse.ArgumentParser(
        prog='minnas',
        description=f"{BOLD('MiniNAS')} - Git-style file storage with snapshots",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    # init
    p = sub.add_parser('init', help='Initialize a repository')
    p.add_argument('--path', '-p', default='.', help='Repository path')
    p.add_argument('--namespace', '-n', default='default', help='Namespace name')
    p.add_argument('--backend', '-b', choices=['local', 'memory'], help='Backend type')
    p.set_defaults(func=cmd_init)

    # status
    p = sub.add_parser('status', help='Show repository status')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_status)

    # commit
    p = sub.add_parser('commit', help='Commit current state as a snapshot')
    p.add_argument('message', help='Commit message')
    p.add_argument('--author', '-a', default='anonymous', help='Author name')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_commit)

    # log
    p = sub.add_parser('log', help='Show commit history')
    p.add_argument('-n', type=int, help='Number of entries')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_log)

    # snapshot
    sp = sub.add_parser('snapshot', help='Snapshot operations')
    ssp = sp.add_subparsers(dest='subcmd')

    sp_ls = ssp.add_parser('list', help='List snapshots')
    sp_ls.add_argument('--repo', '-r', default='.', help='Repository path')
    sp_ls.set_defaults(func=cmd_snapshot_list)

    sp_show = ssp.add_parser('show', help='Show snapshot details')
    sp_show.add_argument('sha', help='Snapshot SHA')
    sp_show.add_argument('--repo', '-r', default='.', help='Repository path')
    sp_show.set_defaults(func=cmd_snapshot_show)

    sp_co = ssp.add_parser('checkout', help='Checkout a snapshot')
    sp_co.add_argument('sha', help='Snapshot SHA')
    sp_co.add_argument('--repo', '-r', default='.', help='Repository path')
    sp_co.set_defaults(func=cmd_snapshot_checkout)

    sp_del = ssp.add_parser('delete', help='Delete a snapshot')
    sp_del.add_argument('sha', help='Snapshot SHA')
    sp_del.add_argument('--repo', '-r', default='.', help='Repository path')
    sp_del.set_defaults(func=cmd_snapshot_show)  # reuse show for now

    # branch
    bp = sub.add_parser('branch', help='Branch operations')
    bp.add_argument('name', nargs='?', help='Branch name')
    bp.add_argument('sha', nargs='?', help='Starting SHA for new branch')
    bp.add_argument('--repo', '-r', default='.', help='Repository path')
    bp.add_argument('--create', '-c', action='store_true', help='Create branch')
    bp.add_argument('--delete', '-d', action='store_true', help='Delete branch')
    bp.add_argument('--checkout', action='store_true', help='Checkout branch')
    bp.set_defaults(func=cmd_branch_list)

    def branch_dispatch(args):
        if args.create:
            cmd_branch_create(args)
        elif args.delete:
            cmd_branch_delete(args)
        elif args.checkout:
            cmd_branch_checkout(args)
        elif args.name:
            cmd_branch_checkout(args)
        else:
            cmd_branch_list(args)
    bp.set_defaults(func=branch_dispatch)

    # namespace
    np = sub.add_parser('namespace', help='Namespace operations')
    np.add_argument('action', choices=['list', 'create', 'switch', 'delete'], nargs='?', default='list')
    np.add_argument('name', nargs='?', help='Namespace name')
    np.add_argument('--repo', '-r', default='.', help='Repository path')
    np.set_defaults(func=cmd_ns_list)

    def ns_dispatch(args):
        if args.action == 'list':    cmd_ns_list(args)
        elif args.action == 'create': cmd_ns_create(args)
        elif args.action == 'switch': cmd_ns_switch(args)
        elif args.action == 'delete': cmd_ns_delete(args)
        else:                         cmd_ns_list(args)
    np.set_defaults(func=ns_dispatch)

    # fs
    fp = sub.add_parser('fs', help='Virtual filesystem operations')
    fp.add_argument('cmd', choices=['ls', 'cat', 'write', 'rm'], nargs='?', default='ls')
    fp.add_argument('path', nargs='?', help='File path')
    fp.add_argument('content', nargs='?', help='Content to write')
    fp.add_argument('--repo', '-r', default='.', help='Repository path')
    fp.set_defaults(func=cmd_fs_ls)

    def fs_dispatch(args):
        if args.cmd == 'ls':   cmd_fs_ls(args)
        elif args.cmd == 'cat': cmd_fs_cat(args)
        elif args.cmd == 'write': cmd_fs_write(args)
        elif args.cmd == 'rm':   cmd_fs_rm(args)
        else: cmd_fs_ls(args)
    fp.set_defaults(func=fs_dispatch)

    # diff
    p = sub.add_parser('diff', help='Diff two snapshots')
    p.add_argument('sha1', help='First snapshot SHA')
    p.add_argument('sha2', help='Second snapshot SHA')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_diff)

    # gc
    p = sub.add_parser('gc', help='Garbage collect unreferenced objects')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_gc)

    # stats
    p = sub.add_parser('stats', help='Show repository statistics')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_stats)

    # debug
    p = sub.add_parser('debug', help='Verify repository integrity')
    p.add_argument('--repo', '-r', default='.', help='Repository path')
    p.set_defaults(func=cmd_debug)

    args = parser.parse_args()
    try:
        rc = args.func(args)
        sys.exit(rc or 0)
    except SystemExit as e:
        sys.exit(e.code)


if __name__ == '__main__':
    main()
