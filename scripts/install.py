#!/usr/bin/env python3
"""Install only this project's two independent plugin directories. Dry-run by default."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile

PACKAGE = Path(__file__).resolve().parent.parent


def safe(path):
    path = Path(os.path.abspath(path))
    for part in [*path.parents, path]:
        if part.is_symlink():
            raise ValueError('Symbolic links are not accepted: ' + str(part))
    return path


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_atomic(path, data, owner, mode):
    safe(path)
    fd, temporary = tempfile.mkstemp(prefix='.bg-jobs-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
            os.fchmod(f.fileno(), mode)
            if os.geteuid() == 0:
                os.fchown(f.fileno(), owner[0], owner[1])
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def owner_of_parent(path):
    p = path.parent
    while not p.exists():
        p = p.parent
    s = p.stat()
    return s.st_uid, s.st_gid


def mkdir_owned(path, owner):
    missing = []
    p = safe(path)
    while not p.exists():
        missing.append(p)
        p = p.parent
    for p in reversed(missing):
        p.mkdir(mode=0o755)
        if os.geteuid() == 0:
            os.chown(p, *owner)


def run(args):
    root = safe(args.root)
    if not root.is_dir():
        raise ValueError('Application root must already exist')
    targets = {
        'server': safe(args.server_root or root / 'plugins/background-jobs'),
        'extension': safe(args.extension_root or root / 'public/scripts/extensions/third-party/background-jobs'),
    }
    if targets['server'] == targets['extension'] or any(a in b.parents for a, b in [(targets['server'], targets['extension']), (targets['extension'], targets['server'])]):
        raise ValueError('Server and extension directories must be separate')
    receipt_path = safe(args.receipt or root / '.background-jobs-install.json')
    if any(t == receipt_path or t in receipt_path.parents for t in targets.values()):
        raise ValueError('Installation receipt must be outside plugin directories')
    receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
    target_map = {k: str(v) for k, v in targets.items()}
    if receipt and (receipt.get('schema') != 1 or receipt.get('targets') != target_map):
        raise ValueError('Installation receipt belongs to another target')
    if args.action == 'uninstall':
        if not receipt:
            print('No installation receipt; no files removed.')
            return
        rows = receipt['files']
        for row in rows:
            if row['component'] not in targets:
                raise ValueError('Unknown receipt component')
            rel = Path(row['path'])
            if rel.is_absolute() or '..' in rel.parts:
                raise ValueError('Unsafe receipt path')
            p = safe(targets[row['component']] / rel)
            if p.exists() and (not p.is_file() or digest(p.read_bytes()) != row['sha256']):
                raise ValueError('Installed file has later edits; refusing removal: ' + str(rel))
        print(('Apply' if args.apply else 'Preview') + f' uninstall: {len(rows)} project files; saved jobs and user settings retained.')
        if not args.apply:
            return
        for row in rows:
            p = safe(targets[row['component']] / row['path'])
            if p.exists():
                if digest(p.read_bytes()) != row['sha256']:
                    raise ValueError('File changed after preflight; stopped')
                p.unlink()
        for target in targets.values():
            if target.exists():
                for d in sorted([x for x in target.rglob('*') if x.is_dir()], reverse=True):
                    try:
                        d.rmdir()
                    except OSError:
                        pass
                try:
                    target.rmdir()
                except OSError:
                    pass
        receipt_path.unlink()
        return

    rows = []
    for component, target in targets.items():
        source = PACKAGE / component
        for p in sorted(source.rglob('*')):
            safe(p)
            if p.is_dir():
                continue
            if p.suffix not in {'.mjs', '.js', '.json', '.css'}:
                raise ValueError('Unexpected package file: ' + p.name)
            rel = p.relative_to(source)
            data = p.read_bytes()
            destination = safe(target / rel)
            if destination.exists() and (not receipt or not destination.is_file() or destination.read_bytes() != data):
                raise ValueError('Target exists without matching installation; refusing overwrite: ' + str(destination))
            rows.append({'component': component, 'path': str(rel), 'sha256': digest(data), 'data': data})
    if not rows or not (PACKAGE / 'server/index.mjs').is_file() or not (PACKAGE / 'extension/manifest.json').is_file():
        raise ValueError('Incomplete distribution')
    if receipt and receipt['files'] != [{k: r[k] for k in ('component', 'path', 'sha256')} for r in rows]:
        raise ValueError('Different release installed; uninstall the original release first')
    print(('Apply' if args.apply else 'Preview') + f' install: {len(rows)} project files, no core or third-party files changed.')
    if not args.apply:
        return
    owners = {k: owner_of_parent(v) for k, v in targets.items()}
    created = []
    try:
        for row in rows:
            destination = safe(targets[row['component']] / row['path'])
            mkdir_owned(destination.parent, owners[row['component']])
            if destination.exists():
                if destination.read_bytes() != row['data']:
                    raise ValueError('Target changed after preflight; stopped')
                continue
            write_atomic(destination, row['data'], owners[row['component']], 0o644)
            created.append((destination, row['sha256']))
        new_receipt = {'schema': 1, 'targets': target_map,
                       'files': [{k: r[k] for k in ('component', 'path', 'sha256')} for r in rows]}
        mkdir_owned(receipt_path.parent, owner_of_parent(receipt_path))
        write_atomic(receipt_path, (json.dumps(new_receipt, indent=2) + '\n').encode(), owner_of_parent(receipt_path), 0o600)
    except Exception:
        for p, expected in reversed(created):
            if p.is_file() and digest(p.read_bytes()) == expected:
                p.unlink()
        raise
    print('Installed. No service restart, model call, or settings change was performed.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install', 'uninstall'])
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--server-root', type=Path)
    parser.add_argument('--extension-root', type=Path)
    parser.add_argument('--receipt', type=Path)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--dry-run', action='store_true')
    try:
        run(parser.parse_args())
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, 'Stopped: ' + str(error) + '\n')
