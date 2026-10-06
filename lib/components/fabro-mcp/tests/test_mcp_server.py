#!/usr/bin/env python3
"""Minimal MCP server for integration testing over stdio or HTTP.

Speaks JSON-RPC 2.0 over stdin/stdout or authenticated streamable HTTP.
Exposes a single tool: echo(message) -> message.
"""
import json
import os
import sys
import time

SERVER_INFO = {
    "name": "test-echo-server",
    "version": "0.1.0",
}

TOOL = {
    "name": "echo",
    "description": "Echo back the message",
    "inputSchema": {
        "type": "object",
        "properties": {
            "message": {"type": "string", "description": "Message to echo"}
        },
        "required": ["message"],
    },
}


def handle_request(req):
    method = req.get("method")
    req_id = req.get("id")
    params = req.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": SERVER_INFO,
            },
        }

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": [TOOL]},
        }

    if method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        if tool_name == "echo":
            msg = arguments.get("message", "")
            if msg == "__cwd__":
                msg = os.getcwd()
            elif msg.startswith("__env:") and msg.endswith("__"):
                key = msg[len("__env:") : -len("__")]
                msg = os.environ.get(key, "")
            elif msg.startswith("__sleep_ms:") and msg.endswith("__"):
                milliseconds = int(msg[len("__sleep_ms:") : -len("__")])
                time.sleep(milliseconds / 1000)
                msg = f"slept {milliseconds}ms"
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": msg}],
                },
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": f"unknown tool: {tool_name}"}],
                "isError": True,
            },
        }

    # Notifications (no id) — just ignore
    if req_id is None:
        return None

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def main():
    if "--http" in sys.argv:
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                expected = "Bearer " + os.environ["FABRO_MCP_BEARER_TOKEN"]
                if self.path != "/mcp" or self.headers.get("Authorization") != expected:
                    self.send_response(403)
                    self.end_headers()
                    return
                req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                resp = handle_request(req)
                if resp is None:
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = json.dumps(resp).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", int(os.environ["FABRO_MCP_PORT"])), Handler)
        if port_file := os.environ.get("FABRO_MCP_TEST_PORT_FILE"):
            with open(port_file, "w") as output:
                output.write(str(server.server_port))
        server.serve_forever()
        return
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue

        resp = handle_request(req)
        if resp is not None:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
