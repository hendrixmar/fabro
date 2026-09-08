#!/bin/bash
set -euo pipefail
# Only sandbox reproductions: never start a debugger on the Fabro host itself.
if [ ! -f /.dockerenv ]; then
    echo 'Managed debugger requires an isolated Docker sandbox' >&2
    exit 1
fi
if [ -f /opt/fabro-debugger/gateway.mjs ] &&
   [ "$(sha256sum /opt/fabro-debugger/gateway.mjs | cut -d ' ' -f 1)" = "$FABRO_DEBUGGER_GATEWAY_SHA256" ]; then
    exec node /opt/fabro-debugger/gateway.mjs "${FABRO_MCP_PORT:-3001}"
fi
ROOT=$(mktemp -d /tmp/fabro-debugger-runtime.XXXXXXXX)
printf '%s' "$FABRO_DEBUGGER_BUNDLE" | base64 -d | tar -xz -C "$ROOT"
unset FABRO_DEBUGGER_BUNDLE
if ! node -e 'process.exit(Number(process.versions.node.split(".")[0]) >= 22 ? 0 : 1)' 2>/dev/null; then
    case "$(uname -m)" in
        x86_64) ARCH=x64; SHA=c33c39ed9c80deddde77c960d00119918b9e352426fd604ba41638d6526a4744 ;;
        aarch64) ARCH=arm64; SHA=25ba95dfb96871fa2ef977f11f95ea90818c8fa15c0f2110771db08d4ba423be ;;
        *) echo 'Unsupported debugger runtime architecture' >&2; exit 1 ;;
    esac
    curl --fail --silent --show-error --max-time 120 \
        "https://nodejs.org/dist/v22.22.0/node-v22.22.0-linux-$ARCH.tar.gz" -o "$ROOT/node.tar.gz"
    printf '%s  %s\n' "$SHA" "$ROOT/node.tar.gz" | sha256sum --check --status
    mkdir "$ROOT/node"
    tar -xzf "$ROOT/node.tar.gz" --strip-components=1 -C "$ROOT/node"
    export PATH="$ROOT/node/bin:$PATH"
fi
npm ci --prefix "$ROOT" --ignore-scripts --no-audit --no-fund --silent
if command -v python3 >/dev/null && ! python3 -I -c 'import debugpy' 2>/dev/null; then
    python3 -I -m pip install --quiet --disable-pip-version-check --target "$ROOT/python" debugpy==1.8.16
    export PYTHONPATH="$ROOT/python${PYTHONPATH:+:$PYTHONPATH}"
fi
exec node "$ROOT/gateway.mjs" "${FABRO_MCP_PORT:-3001}"
