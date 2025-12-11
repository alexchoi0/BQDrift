# bqdrift REPL Module

Two modes for interacting with bqdrift:

1. **Interactive Mode** - Terminal REPL with tab completion and history
2. **Server Mode** - JSON-RPC 2.0 over stdin/stdout for programmatic access

## Quick Start

```bash
# Interactive mode (auto-detected when stdin is a TTY)
bqdrift --repl --project my-project --queries ./queries

# Server mode (forced with --server)
bqdrift --repl --server --project my-project --queries ./queries

# Production server with limits
bqdrift --repl --server \
  --project my-project \
  --queries ./queries \
  --max-sessions 50 \
  --idle-timeout 300
```

## Server Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--max-sessions` | 100 | Maximum concurrent sessions |
| `--idle-timeout` | 300 | Default session idle timeout (seconds) |
| `--max-idle-timeout` | 3600 | Maximum allowed idle timeout (seconds) |

## Concurrency Model

- **Parallel across sessions**: Requests to different sessions execute concurrently
- **Sequential within session**: Requests to the same session execute in order
- **Session isolation**: Each session has its own BqClient and cached queries
- **Auto-cleanup**: Expired sessions are removed automatically

## TypeScript Client

```typescript
import { spawn, ChildProcess } from 'child_process';
import * as readline from 'readline';

interface JsonRpcResponse {
  jsonrpc: string;
  id: number;
  result?: unknown;
  error?: { code: number; message: string };
}

interface SessionInfo {
  id: string;
  created_at: string;
  last_activity: string;
  request_count: number;
  idle_timeout_secs: number;
  expires_at: string;
  project?: string;
  metadata?: Record<string, string>;
}

interface ServerConfig {
  max_sessions: number;
  current_sessions: number;
  default_idle_timeout_secs: number;
  max_idle_timeout_secs: number;
  default_project?: string;
  default_queries_path: string;
}

class BqDriftClient {
  private process: ChildProcess;
  private pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
  }>();
  private nextId = 1;

  constructor(options: {
    project: string;
    queriesPath: string;
    maxSessions?: number;
    idleTimeout?: number;
  }) {
    const args = [
      '--repl', '--server',
      '--project', options.project,
      '--queries', options.queriesPath,
    ];
    if (options.maxSessions) {
      args.push('--max-sessions', String(options.maxSessions));
    }
    if (options.idleTimeout) {
      args.push('--idle-timeout', String(options.idleTimeout));
    }

    this.process = spawn('bqdrift', args);

    const rl = readline.createInterface({ input: this.process.stdout! });
    rl.on('line', (line) => {
      const response: JsonRpcResponse = JSON.parse(line);
      const pending = this.pending.get(response.id);
      if (pending) {
        this.pending.delete(response.id);
        if (response.error) {
          const err = new Error(response.error.message) as Error & { code: number };
          err.code = response.error.code;
          pending.reject(err);
        } else {
          pending.resolve(response.result);
        }
      }
    });
  }

  request<T = unknown>(method: string, params: object = {}, session?: string): Promise<T> {
    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      this.pending.set(id, { resolve: resolve as (v: unknown) => void, reject });
      this.process.stdin!.write(JSON.stringify({
        jsonrpc: '2.0',
        method,
        params,
        id,
        ...(session && { session }),
      }) + '\n');
    });
  }

  // Session management
  sessions(): Promise<SessionInfo[]> {
    return this.request('sessions');
  }

  serverConfig(): Promise<ServerConfig> {
    return this.request('server_config');
  }

  createSession(options: {
    session?: string;
    project?: string;
    queriesPath?: string;
    idleTimeout?: number;
    metadata?: Record<string, string>;
  }): Promise<SessionInfo> {
    return this.request('session_create', {
      session: options.session,
      project: options.project,
      queries_path: options.queriesPath,
      idle_timeout: options.idleTimeout,
      metadata: options.metadata,
    });
  }

  destroySession(session: string): Promise<{ destroyed: boolean }> {
    return this.request('session_destroy', { session });
  }

  keepalive(session: string): Promise<{ success: boolean }> {
    return this.request('session_keepalive', { session });
  }

  // Health check
  ping(): Promise<{ pong: boolean }> {
    return this.request('ping');
  }

  // Query operations
  list(detailed = false): Promise<{ queries: string[] }> {
    return this.request('list', { detailed });
  }

  run(query: string, partition: string, session?: string): Promise<unknown> {
    return this.request('run', { query, partition }, session);
  }

  backfill(query: string, from: string, to: string, session?: string): Promise<unknown> {
    return this.request('backfill', { query, from, to }, session);
  }

  check(query: string, partition: string, session?: string): Promise<unknown> {
    return this.request('check', { query, partition }, session);
  }

  async close(): Promise<void> {
    await this.request('exit');
    this.process.kill();
  }
}

// Usage
async function main() {
  const client = new BqDriftClient({
    project: 'my-project',
    queriesPath: './queries',
    maxSessions: 10,
    idleTimeout: 600,
  });

  // Check server config
  const config = await client.serverConfig();
  console.log(`Sessions: ${config.current_sessions}/${config.max_sessions}`);

  // Run queries in parallel across sessions
  const partitions = ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18'];
  const results = await Promise.all(
    partitions.map((p, i) => client.run('daily_stats', p, `worker-${i}`))
  );

  // Check active sessions
  const sessions = await client.sessions();
  console.log('Active sessions:', sessions.map(s => s.id));

  await client.close();
}
```

## JSON-RPC Protocol

### Request Format

```json
{
  "jsonrpc": "2.0",
  "method": "run",
  "params": {"query": "daily_stats", "partition": "2024-01-15"},
  "id": 1,
  "session": "worker-0"
}
```

### Error Codes

| Code | Meaning |
|------|---------|
| -32700 | Parse error |
| -32600 | Invalid request |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32603 | Internal error |
| -32001 | Session expired |
| -32002 | Session limit reached |

## Methods Reference

### Session Management

| Method | Description |
|--------|-------------|
| `sessions` | List all active sessions |
| `server_config` | Get server configuration and limits |
| `session_create` | Create session with custom config |
| `session_destroy` | Destroy a session |
| `session_keepalive` | Extend session expiration |

### Query Operations

| Method | Description |
|--------|-------------|
| `list` | List all queries |
| `show` | Show query details |
| `validate` | Validate query definitions |
| `run` | Execute query for a partition |
| `backfill` | Backfill date range |
| `check` | Run invariant checks |

### Other

| Method | Description |
|--------|-------------|
| `ping` | Health check (returns `{"pong": true}`) |
| `status` | Show session status |
| `reload` | Reload queries from disk |
| `init` | Initialize tracking table |
| `sync` | Sync drifted partitions |
| `audit` | Audit source files |
| `exit` | Exit the server |
