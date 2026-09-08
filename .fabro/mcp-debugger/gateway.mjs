import { randomUUID, timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { ResultSchema, isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

// One immutable Fabro-provisioned gateway per agent session. Debuggers remain
// inside that run's sandbox; no host mounts, published ports, or shared targets.
const token = process.env.FABRO_MCP_BEARER_TOKEN;
if (!token || token.length < 32) throw new Error('Fabro MCP bearer credential required');
const authorization = Buffer.from(`Bearer ${token}`);
const port = Number(process.argv[2] ?? 3001);
if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('Invalid MCP port');
const env = { ...process.env, DEBUG_MCP_NO_REDACT: '0' };
delete env.FABRO_MCP_BEARER_TOKEN;
delete env.NODE_OPTIONS;
delete env.FABRO_DEBUGGER_BUNDLE;
const sessions = new Map();
const connections = new Set();
let stopping = false;
const app = express();
app.use((req, res, next) => {
  if (stopping) return res.sendStatus(503);
  const supplied = Buffer.from(req.headers.authorization ?? '');
  if (req.headers.origin || supplied.length !== authorization.length ||
      !timingSafeEqual(supplied, authorization)) return res.sendStatus(401);
  next();
});
app.use(express.json({ limit: '1mb' }));
app.get('/health', (_req, res) => res.json({ status: 'ok', sessions: sessions.size }));

async function connectDebugger() {
  if (stopping) throw new Error('Debugger gateway is stopping');
  const client = new Client({ name: 'fabro-debugger-gateway', version: '1' });
  let server;
  let sessionId;
  let closing;
  const close = () => {
    if (closing) return closing;
    sessions.delete(sessionId);
    closing = Promise.resolve().then(() => Promise.allSettled([server?.close(), client.close()]))
      .finally(() => connections.delete(connection));
    return closing;
  };
  const connection = { close };
  connections.add(connection);
  const stdio = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL('./node_modules/@debugmcp/mcp-debugger/dist/cli.mjs', import.meta.url)), 'stdio'],
    env,
    stderr: 'ignore',
    cwd: process.cwd(),
  });
  try {
    await client.connect(stdio);
    if (stopping || closing) throw new Error('Debugger connection cancelled');
    server = new Server({ name: 'fabro-mcp-debugger', version: '0.24.2' }, {
      capabilities: client.getServerCapabilities(), instructions: client.getInstructions(),
    });
    server.fallbackRequestHandler = async (request) => {
      if (request.method === 'tools/call') {
        const { name, arguments: args = {} } = request.params;
        if (['attach_to_process', 'create_debug_session'].includes(name) && args.host &&
            !['localhost', '127.0.0.1', '::1'].includes(args.host)) {
          throw new Error('Remote target attachment is disabled; reproduce inside this run sandbox');
        }
      }
      return client.request(request, ResultSchema, { timeout: 120_000 });
    };
    server.fallbackNotificationHandler = (notification) => client.notification(notification);
    client.fallbackNotificationHandler = (notification) => server.notification(notification);
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: randomUUID,
      onsessioninitialized: (id) => {
        sessionId = id;
        sessions.set(id, { transport, close });
      },
    });
    await server.connect(transport);
    const onclose = transport.onclose;
    transport.onclose = () => { onclose?.(); void close(); };
    return { transport, close };
  } catch (error) {
    await close();
    throw error;
  }
}

app.all('/mcp', async (req, res) => {
  let connection;
  let created = false;
  try {
    const id = req.headers['mcp-session-id'];
    if (typeof id === 'string') {
      connection = sessions.get(id);
      if (!connection) return res.sendStatus(404);
    } else if (req.method === 'POST' && isInitializeRequest(req.body)) {
      connection = await connectDebugger();
      created = true;
    } else {
      return res.sendStatus(400);
    }
    await connection.transport.handleRequest(req, res, req.body);
    if (created && res.statusCode >= 400) await connection.close();
  } catch {
    if (created) await connection?.close();
    if (!res.headersSent) res.status(502).json({ error: 'Debugger MCP connection failed' });
  }
});
const listener = app.listen(port, '0.0.0.0');
listener.on('error', () => { process.exitCode = 1; void shutdown(); });
async function shutdown() {
  if (stopping) return;
  stopping = true;
  const deadline = setTimeout(() => process.exit(1), 5000);
  deadline.unref();
  const listenerClosed = new Promise((resolve) => listener.close(resolve));
  await Promise.allSettled([...connections].map((connection) => connection.close()));
  listener.closeAllConnections();
  await listenerClosed;
  clearTimeout(deadline);
}
process.once('SIGTERM', () => void shutdown());
process.once('SIGINT', () => void shutdown());
