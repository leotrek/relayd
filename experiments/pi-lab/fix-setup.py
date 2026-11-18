#!/usr/bin/env python3
"""Repair lab access, build all configured architectures, and run setup.

Run on the lab workstation. Preserves installed software and experiment data.
Grants the selected SSH user passwordless root access on dedicated lab nodes.
"""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

import lab


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='cluster.json')
    parser.add_argument('--binaries', default='./pi-bin')
    parser.add_argument('--workspace', help='directory containing relayd, fluxd, orbitald')
    args = parser.parse_args()
    cfg = lab.validate(lab.read_json(args.config))
    # Validate the username before making any changes.
    lab.ssh_target(cfg['nodes'][0])
    username = os.environ['RELAYD_SSH_USER']
    failures = []
    for node in cfg['nodes']:
        target = lab.ssh_target(node)
        connection, env = lab.connection(node)
        print(f"\nChecking {node['name']} ({target})", flush=True)
        probe = subprocess.run(connection + ['-o', 'StrictHostKeyChecking=ask', target, 'true'],
                               env=env, capture_output=True, text=True)
        if probe.returncode and 'REMOTE HOST IDENTIFICATION HAS CHANGED' in probe.stderr:
            host = target.rsplit('@', 1)[1]
            if not re.fullmatch(r'[A-Za-z0-9_.:-]+', host):
                raise ValueError('unsupported SSH hostname')
            print(probe.stderr, file=sys.stderr)
            print('Verify the fingerprint above through the node console or another trusted channel.')
            if input(f'Type {host} to remove its old known_hosts entry, or Enter to skip: ') != host:
                failures.append(node['name'])
                continue
            run(['ssh-keygen', '-R', host])
        elif probe.returncode:
            print(probe.stderr, file=sys.stderr)
        try:
            # A terminal allows SSH host-key confirmation and interactive sudo.
            # Validate a temporary sudoers file before installing it atomically.
            script = '''set -eu
user=$1
tmp=$(mktemp /etc/sudoers.d/.relayd-lab.XXXXXX)
trap 'rm -f "$tmp"' EXIT
printf '%s ALL=(ALL) NOPASSWD: ALL\n' "$user" > "$tmp"
chmod 0440 "$tmp"
visudo -cf "$tmp"
mv -f "$tmp" "/etc/sudoers.d/relayd-lab-$user"
'''
            command = shlex.join(['sudo', '--', 'sh', '-c', script, 'sh', username])
            run(connection + ['-tt', '-o', 'StrictHostKeyChecking=ask', target, command], env=env)
            run(connection + [target, 'sudo -n true'], env=env)
        except subprocess.CalledProcessError:
            failures.append(node['name'])
    if failures:
        raise SystemExit('Access repair failed; setup not started: ' + ', '.join(failures))

    lab_path = str(Path(__file__).with_name('lab.py'))
    build = [sys.executable, lab_path, 'build', '--config', args.config, '--output', args.binaries]
    if args.workspace:
        build += ['--workspace', args.workspace]
    run(build)
    run([sys.executable, lab_path, 'setup', '--config', args.config,
         '--binaries', args.binaries, '--arch', 'auto', '--bootstrap'])


if __name__ == '__main__':
    main()
