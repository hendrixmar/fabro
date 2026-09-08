#!/usr/bin/env python3
"""Register sandbox debugger once in Fabro; no agent/container config files."""
import argparse
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import tomllib
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--settings', type=Path, default=Path.home() / '.fabro/settings.toml')
    parser.add_argument('--token-file', type=Path, default=Path.home() / '.fabro/storage/server.dev-token')
    parser.add_argument('--url', default='http://100.64.197.63:32276')
    parser.add_argument('--image', required=True, help='Built, clean shared agent image tag or digest')
    parser.add_argument('--replace', action='store_true',
                        help='Explicitly replace existing debugger catalog definitions using their current revisions')
    args = parser.parse_args()
    text = args.settings.read_text()
    settings = tomllib.loads(text)
    entry = settings.get('run', {}).get('agent', {}).get('mcps', {}).get('managed-debugger')
    expected = {'id': 'mcp-debugger', 'enabled': True}
    if entry is not None and entry != expected:
        raise SystemExit('Existing managed-debugger default differs; refusing to overwrite operator policy')
    token = args.token_file.read_text().strip()
    root = Path(__file__).resolve().parent
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode='w:gz') as archive:
        for name in ['package.json', 'package-lock.json', 'gateway.mjs']:
            archive.add(root / name, arcname=name)

    def api(method, path, body=None, revision=None):
        headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
        if revision:
            headers['If-Match'] = revision
        request = urllib.request.Request(args.url.rstrip('/') + '/api/v1' + path,
            headers=headers, data=None if body is None else json.dumps(body).encode(), method=method)
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    def upsert(path, body):
        identifier = body['id']
        try:
            current = api('GET', path + '/' + identifier)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            result = api('POST', path, body)
        else:
            if not args.replace:
                print(f"{path}/{identifier}: reusing existing operator configuration")
                return current
            result = api('PUT', path + '/' + identifier,
                         {key: value for key, value in body.items() if key != 'id'}, current['revision'])
        print(f"{path}/{identifier}: registered")
        return result

    upsert('/mcp-servers', {
        'id': 'mcp-debugger', 'display_name': 'Run-isolated debugger',
        'description': 'Fabro-managed debugmcp 0.24.2; authenticated per-agent runtime inside Docker, Python and JavaScript reproduction debugging; remote target attach disabled.',
        'transport': {'type': 'sandbox', 'protocol': 'streamable_http', 'port': 0,
            'command': ['bash', '-c', (root / 'bootstrap.sh').read_text()],
            'env': {'FABRO_DEBUGGER_BUNDLE': base64.b64encode(payload.getvalue()).decode(),
                    'FABRO_DEBUGGER_GATEWAY_SHA256': hashlib.sha256((root / 'gateway.mjs').read_bytes()).hexdigest()}},
        'startup_timeout_secs': 300, 'tool_timeout_secs': 120,
    })
    upsert('/environments', {
        'id': 'debugger-agents', 'provider': 'docker',
        'image': {'docker': args.image, 'dockerfile': None},
        'resources': {'cpu': 2, 'memory': '4GB', 'disk': None},
        'network': {'mode': 'allow_all', 'allow': []},
        'lifecycle': {'preserve': False, 'stop_on_terminal': True, 'auto_stop': None},
        'labels': {'sh.fabro.role': 'debugger-agents'}, 'env': {},
    })
    if entry is None:
        if args.settings.read_text() != text:
            raise SystemExit('Settings changed during setup; refusing to overwrite concurrent edits')
        text += '\n[run.agent.mcps.managed-debugger]\nid = "mcp-debugger"\nenabled = true\n'
        tomllib.loads(text)
        with tempfile.NamedTemporaryFile('w', dir=args.settings.parent, delete=False) as pending:
            try:
                pending.write(text)
                pending.flush()
                os.fsync(pending.fileno())
                os.chmod(pending.name, args.settings.stat().st_mode & 0o777)
                os.replace(pending.name, args.settings)
            finally:
                Path(pending.name).unlink(missing_ok=True)
    print('Server default MCP reference configured. Restart Fabro to load changed settings.')


if __name__ == '__main__':
    main()
