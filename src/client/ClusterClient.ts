import { EventEmitter } from 'events';
import { ClusterInfo, EvalOptions, MessageType, PendingRequest, RawIPCMessage } from '../types';
import { IPCMessage } from '../ipc/IPCMessage';
import { generateNonce } from '../util/Util';
import { Logger } from '../util/Logger';

// ─── IPC Transport ────────────────────────────────────────────────────────────

interface IPCTransport {
  send(data: RawIPCMessage): void;
  onMessage(handler: (msg: RawIPCMessage) => void): void;
}

function buildTransport(mode: string): IPCTransport {
  if (mode === 'worker') {
    // worker_threads mode — use parentPort
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { parentPort } = require('worker_threads') as typeof import('worker_threads');
    return {
      send: (data) => parentPort?.postMessage(data),
      onMessage: (handler) => parentPort?.on('message', handler),
    };
  }
  // Default: child_process mode — use process.send / process.on('message')
  return {
    send: (data) => process.send?.(data),
    onMessage: (handler) => process.on('message', handler),
  };
}

// ─── getInfo ─────────────────────────────────────────────────────────────────

/**
 * Read cluster info injected by the manager into the cluster's environment.
 * Call this in your bot file to get the shard list for the Discord client.
 *
 * @example
 * const { getInfo } = require('devcodes-sharding');
 * const client = new Client({
 *   shards: getInfo().SHARD_LIST,
 *   shardCount: getInfo().TOTAL_SHARDS,
 * });
 */
export function getInfo(): ClusterInfo {
  if (!process.env['CLUSTER_MANAGER']) {
    throw new Error(
      'getInfo() must be called from inside a cluster spawned by ShardingManager.',
    );
  }
  return {
    CLUSTER_ID: Number(process.env['CLUSTER_ID']),
    CLUSTER_COUNT: Number(process.env['CLUSTER_COUNT']),
    SHARD_LIST: JSON.parse(process.env['SHARD_LIST'] ?? '[]') as number[],
    TOTAL_SHARDS: Number(process.env['TOTAL_SHARDS']),
    FIRST_SHARD_ID: Number(process.env['FIRST_SHARD_ID']),
    LAST_SHARD_ID: Number(process.env['LAST_SHARD_ID']),
    MAINTENANCE: process.env['CLUSTER_MAINTENANCE'] ?? null,
  };
}

// ─── ClusterClient ────────────────────────────────────────────────────────────

export declare interface ClusterClient {
  on(event: 'ready', listener: () => void): this;
  on(event: 'maintenance', listener: (msg: string) => void): this;
  on(event: 'message', listener: (msg: IPCMessage) => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
}

/**
 * # ClusterClient
 *
 * Add this to your bot file to hook into the sharding system.
 *
 * @example
 * const { ClusterClient, getInfo } = require('devcodes-sharding');
 * const { Client } = require('discord.js');
 *
 * const client = new Client({
 *   shards: getInfo().SHARD_LIST,
 *   shardCount: getInfo().TOTAL_SHARDS,
 *   intents: [...],
 * });
 *
 * client.cluster = new ClusterClient(client);
 *
 * client.once('ready', () => client.cluster.triggerReady());
 * client.login(process.env.TOKEN);
 */
export class ClusterClient extends EventEmitter {
  /** Discord client passed in the constructor. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly client: any;
  /** Static info about this cluster (shard list, counts, etc.). */
  readonly info: ClusterInfo;

  /** This cluster's ID. */
  get id(): number { return this.info.CLUSTER_ID; }
  /** Total number of clusters. */
  get count(): number { return this.info.CLUSTER_COUNT; }

  private readonly _log: Logger;
  private readonly _ipc: IPCTransport;
  private readonly _pending = new Map<string, PendingRequest>();
  private _heartbeatTimer: NodeJS.Timeout | null = null;
  private _maintenance: string | null = null;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(client: any) {
    super();
    this.client = client;
    this.info = getInfo();
    this._log = new Logger(`ClusterClient#${this.id}`);
    this._ipc = buildTransport(process.env['CLUSTER_MANAGER_MODE'] ?? 'process');

    this._ipc.onMessage((raw) => this._handleMessage(raw));
    this._startHeartbeat();

    this._log.info(
      `Ready — shards [${this.info.SHARD_LIST.join(', ')}] of ${this.info.TOTAL_SHARDS}`,
    );
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Signal to the manager that this cluster is fully ready.
   * **Must be called** when your Discord client fires its ready event.
   */
  triggerReady(): void {
    this._send({ _type: MessageType.CLUSTER_READY });
    this.emit('ready');
  }

  /**
   * Put this cluster into maintenance mode.
   * Emits the `maintenance` event and notifies the manager.
   */
  triggerMaintenance(message: string): void {
    this._maintenance = message;
    process.env['CLUSTER_MAINTENANCE'] = message;
    this._send({ _type: MessageType.MAINTENANCE_ENABLE, message });
    this.emit('maintenance', message);
  }

  /** Disable maintenance mode on this cluster. */
  clearMaintenance(): void {
    this._maintenance = null;
    delete process.env['CLUSTER_MAINTENANCE'];
    this._send({ _type: MessageType.MAINTENANCE_DISABLE });
    this.emit('maintenance', null);
  }

  get maintenance(): string | null {
    return this._maintenance;
  }

  // ─── IPC ───────────────────────────────────────────────────────────────────

  /**
   * Send a fire-and-forget message to the manager.
   */
  send(data: Record<string, unknown>): void {
    this._send({ ...data, _type: MessageType.CUSTOM_MESSAGE } as RawIPCMessage);
  }

  /**
   * Send a request to the manager and await a reply.
   *
   * @example
   * const result = await client.cluster.request({ action: 'getStats' });
   */
  request<T = unknown>(data: Record<string, unknown>, timeout = 30_000): Promise<T> {
    const nonce = generateNonce();
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(nonce);
        reject(new Error(`ClusterClient request timed out after ${timeout}ms`));
      }, timeout);

      this._pending.set(nonce, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });

      this._send({
        ...data,
        _type: MessageType.CUSTOM_REQUEST,
        _nonce: nonce,
        _clusterId: this.id,
      } as RawIPCMessage);
    });
  }

  // ─── Eval ──────────────────────────────────────────────────────────────────

  /**
   * Evaluate a function on every cluster's Discord client.
   * Returns an array of results in cluster-ID order.
   *
   * @example
   * const sizes = await client.cluster.broadcastEval(c => c.guilds.cache.size);
   * const total = sizes.reduce((a, b) => a + b, 0);
   */
  broadcastEval<T = unknown>(
    script: string | ((client: unknown, ctx?: unknown) => T),
    options: EvalOptions = {},
  ): Promise<T[]> {
    const serialized =
      typeof script === 'function' ? script.toString() : script;

    const nonce = generateNonce();

    return new Promise<T[]>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(nonce);
        reject(new Error('broadcastEval timed out'));
      }, options.timeout ?? 10_000);

      this._pending.set(nonce, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });

      this._send({
        _type: MessageType.BROADCAST_EVAL,
        _nonce: nonce,
        _clusterId: this.id,
        script: serialized,
        context: options.context ?? {},
      } as RawIPCMessage);
    });
  }

  /**
   * Fetch a property value from every cluster's Discord client.
   */
  fetchClientValues(prop: string): Promise<unknown[]> {
    return this.broadcastEval(
      new Function(`return this.${prop}`) as (c: unknown) => unknown,
    );
  }

  /**
   * Evaluate a function on the manager process itself.
   */
  evalOnManager<T = unknown>(
    script: string | ((manager: unknown) => T),
    timeout = 10_000,
  ): Promise<T> {
    const serialized =
      typeof script === 'function' ? script.toString() : script;

    const nonce = generateNonce();

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(nonce);
        reject(new Error('evalOnManager timed out'));
      }, timeout);

      this._pending.set(nonce, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });

      this._send({
        _type: MessageType.MANAGER_EVAL,
        _nonce: nonce,
        _clusterId: this.id,
        script: serialized,
      } as RawIPCMessage);
    });
  }

  // ─── Queue Control ─────────────────────────────────────────────────────────

  /**
   * Trigger the manager to spawn the next cluster in the queue.
   * Use this when `options.queue.auto` is `false`.
   */
  spawnNextCluster(): void {
    this._send({ _type: MessageType.SPAWN_NEXT_CLUSTER });
  }

  // ─── Internal ──────────────────────────────────────────────────────────────

  private _send(data: RawIPCMessage): void {
    this._ipc.send({ ...data, _clusterId: this.id });
  }

  private _handleMessage(raw: RawIPCMessage): void {
    const msg = new IPCMessage(raw);

    switch (msg._type) {
      case MessageType.HEARTBEAT_ACK:
        // No-op — just confirms manager is alive
        break;

      case MessageType.GRACEFUL_SHUTDOWN:
        // Cluster is asked to shut down gracefully
        this._log.info(`Graceful shutdown requested: ${msg['reason'] ?? 'unknown'}`);
        // Acknowledge and exit after a tick so the send can complete
        this._send({
          _type: MessageType.CUSTOM_REPLY,
          _nonce: msg._nonce ?? '',
          _result: 'ok',
        } as RawIPCMessage);
        setImmediate(() => process.exit(0));
        break;

      case MessageType.BROADCAST_EVAL: {
        // Manager is asking this cluster to eval a script
        const script = msg['script'] as string;
        const context = (msg['context'] ?? {}) as Record<string, unknown>;
        const nonce = msg._nonce;
        if (!nonce) break;

        let result: unknown;
        let error: string | undefined;
        try {
          // eslint-disable-next-line no-new-func
          const fn = new Function('client', 'context', `return (${script})(client, context)`);
          result = fn(this.client, context);
        } catch (err) {
          error = (err as Error).message;
        }

        // If result is a Promise, await it
        Promise.resolve(result)
          .then((resolved) => {
            this._send({
              _type: MessageType.BROADCAST_EVAL_RESPONSE,
              _nonce: nonce,
              _result: resolved,
            } as RawIPCMessage);
          })
          .catch((err: Error) => {
            this._send({
              _type: MessageType.BROADCAST_EVAL_RESPONSE,
              _nonce: nonce,
              _error: error ?? err.message,
            } as RawIPCMessage);
          });
        break;
      }

      case MessageType.BROADCAST_EVAL_RESPONSE:
      case MessageType.MANAGER_EVAL_RESPONSE:
      case MessageType.CUSTOM_REPLY: {
        const nonce = msg._nonce;
        if (!nonce) break;
        const pending = this._pending.get(nonce);
        if (!pending) break;
        clearTimeout(pending.timer);
        this._pending.delete(nonce);
        if (msg['_error']) {
          pending.reject(new Error(String(msg['_error'])));
        } else {
          pending.resolve(msg['_result'] ?? msg.toJSON());
        }
        break;
      }

      case MessageType.MAINTENANCE_ENABLE: {
        const text = (msg['message'] ?? '') as string;
        this._maintenance = text;
        process.env['CLUSTER_MAINTENANCE'] = text;
        this.emit('maintenance', text);
        break;
      }

      case MessageType.MAINTENANCE_DISABLE:
        this._maintenance = null;
        delete process.env['CLUSTER_MAINTENANCE'];
        this.emit('maintenance', null);
        break;

      case MessageType.CLUSTER_INFO_UPDATE: {
        // Manager hot-updated cluster info (e.g. totalShards increased)
        const newTotal = msg['totalShards'] as number | undefined;
        if (newTotal) {
          (this.info as unknown as Record<string, unknown>)['TOTAL_SHARDS'] = newTotal;
          process.env['TOTAL_SHARDS'] = String(newTotal);
        }
        break;
      }

      default:
        this.emit('message', msg);
        break;
    }
  }

  private _startHeartbeat(): void {
    const interval = 30_000; // default; overridden if manager sends config
    this._heartbeatTimer = setInterval(() => {
      const nonce = generateNonce();
      this._send({ _type: MessageType.HEARTBEAT, _nonce: nonce });
    }, interval);

    // Don't keep the process alive just for heartbeats
    this._heartbeatTimer.unref?.();
  }

  /** Stop heartbeat timer (called on graceful shutdown). */
  destroy(): void {
    if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
  }
}
