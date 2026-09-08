import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const url = process.env.MCP_URL ?? 'http://127.0.0.1:3001/mcp';
const token = process.env.FABRO_MCP_BEARER_TOKEN;
assert(token, 'Set FABRO_MCP_BEARER_TOKEN in environment, never argv');
assert.equal((await fetch(url, { method: 'POST' })).status, 401);
assert.equal((await fetch(url, { method: 'POST', headers: { authorization: 'Bearer wrong' } })).status, 401);
const clients = [];
const directory = await mkdtemp(join(tmpdir(), 'fabro-debugger-smoke-'));
async function connect() {
  const client = new Client({ name: 'fabro-debugger-proof', version: '1' });
  const transport = new StreamableHTTPClientTransport(new URL(url), {
    requestInit: { headers: { authorization: `Bearer ${token}` } },
  });
  await client.connect(transport);
  clients.push({ client, transport });
  return client;
}
async function call(client, name, args) {
  const result = await client.callTool({ name, arguments: args }, undefined, { timeout: 90_000 });
  assert(!result.isError, `MCP tool failed: ${name}`);
  const text = result.content.filter((item) => item.type === 'text').map((item) => item.text).join('\n');
  const parsed = JSON.parse(text);
  assert(parsed.success !== false, `${name}: ${parsed.message ?? parsed.error}`);
  return parsed;
}
try {
  const first = await connect();
  const second = await connect();
  const tools = await first.listTools();
  assert(tools.tools.some((tool) => tool.name === 'get_local_variables'));
  for (const [language, extension, source] of [
    ['python', 'py', 'def subtotal(unit, count):\n    total = unit * count\n    return total\n\nprint(subtotal(6, 7))\n'],
    ['javascript', 'cjs', 'function subtotal(unit, count) {\n  const total = unit * count;\n  return total;\n}\nconsole.log(subtotal(6, 7));\n'],
  ]) {
    const file = join(directory, `subtotal.${extension}`);
    await writeFile(file, source);
    const session = await call(first, 'create_debug_session', { language, name: 'managed proof' });
    const sessionId = session.sessionId;
    assert(sessionId);
    try {
      await call(first, 'set_breakpoint', { sessionId, file, line: 3 });
      await call(first, 'start_debugging', { sessionId, scriptPath: file });
      const stack = await call(first, 'get_stack_trace', { sessionId });
      const frame = stack.stackFrames.find((frame) => frame.name.includes('subtotal'));
      assert(frame, 'Expected pause in subtotal');
      const locals = await call(first, 'get_local_variables', { sessionId, frameId: frame.id });
      assert(locals.variables.some((variable) => variable.name === 'total' && variable.value === '42'));
      const foreign = await second.callTool({ name: 'get_stack_trace', arguments: { sessionId } });
      const foreignText = foreign.content.filter((item) => item.type === 'text').map((item) => item.text).join('\n');
      assert(foreign.isError || JSON.parse(foreignText).success === false, 'Another client accessed this debug session');
      console.log(`${language}: breakpoint hit, total=42, cross-client access rejected`);
    } finally {
      await call(first, 'close_debug_session', { sessionId });
    }
  }
} finally {
  await Promise.allSettled(clients.map(async ({ client, transport }) => {
    await transport.terminateSession();
    await client.close();
  }));
  await rm(directory, { recursive: true, force: true });
}
const health = await fetch(new URL('/health', url), { headers: { authorization: `Bearer ${token}` } });
assert.equal((await health.json()).sessions, 0, 'Debugger sessions survived explicit client cleanup');
console.log('Debugger authentication, real execution and session isolation passed');
