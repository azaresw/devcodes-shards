import { EventEmitter } from 'events';
import { fork, ChildProcess } from 'child_process';
import { Worker } from 'worker_threads';
import {
  ClusterSpawnOptions,
  ClusterStatus,
  ClusterStats,
  EvalOptions,
  MessageType,
  PendingRequest,
  RawIPCMessage,
  RemoveClusterOptions,
} from '../types';
import { IPCMessage } from '../ipc/IPCMessage';
import { generateNonce } from '../util/Util';
import { Logger, LogLevel } from '../util/Logger';

// Forward-declare to avoid circular import at module level
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyManager = any;

export declare interface Cluster {
  on(event: 'ready', listener: () => void): this;
  on(event: 'death', listener: (exitCode: number | null, signal: string | null) => void): this;
  on(event: 'message', listener: (msg: IPCMessage) => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: 'spawn', listener: () => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
}

/**
 * Represents a single spawned cluster (child process or worker thread).
 *
 * This class is managed by ShardingManager. You should never instantiate it
 * directly — use `manager.clusters.get(id)` or the hot-add API.
 */
export class Cluster extends EventEmitter {
  /** Cluster ID assigned by the manager. */
  readonly id: number;
  /** Shard IDs this cluster is responsible for. */
  shardList: number[];
  /** Total shards across all clusters. */
  totalShards: number;
  /** Current lifecycle state. */
  status: ClusterStatus = ClusterStatus.IDLE;

  /** Number of times this cluster has been respawned. */
  restarts = 0;
  /** Timestamps of recent restarts (used for throttle). */
  readonly restartHistory: number[] = [];

  private readonly _manager: AnyManager;
  private readonly _options: ClusterSpawnOptions;
  private readonly _log: Logger;
  private _process: ChildProcess | Worker | null = null;
  private _spawnedAt: number | null = null;

  /** Pending nonce → resolve/reject for request() calls. */
  private readonly _pending = new Map<string, PendingRequest>();

  constructor(manager: AnyManager, options: ClusterSpawnOptions) {
    super();
    this._manager = manager;
    this._options = options;
    this.id = options.id;
    this.shardList = options.shardList;
    this.totalShards = options.totalShards;
    // Inherit the manager-level logging flag so toggling manager.options.logging
    // silences cluster output too.
    const loggingEnabled: boolean = manager.options?.logging ?? true;
    this._log = new Logger(`Cluster#${this.id}`, LogLevel.INFO, loggingEnabled);
  }

  // ─── Public API ────────────────────────────────────────────────────────────

  /**
   * Spawn the cluster process/worker and wait until it signals ready.
   * @param timeout Ms to wait for the CLUSTER_READY signal. Default 30 000.
   */
  async spawn(timeout = 30_000): Promise<void> {
    if (this._process) throw new Error(`Cluster ${this.id} is already spawned.`);

    this.status = ClusterStatus.SPAWNING;
    this._log.info(`Spawning (shards: [${this.shardList.join(', ')}])`);

    const env: NodeJS.ProcessEnv = {
      ...process.env,
      ...(this._options.env ?? {}),
      CLUSTER_ID: String(this.id),
      CLUSTER_COUNT: String(this._options.clusterCount),
      SHARD_LIST: JSON.stringify(this.shardList),
      TOTAL_SHARDS: String(this.totalShards),
      FIRST_SHARD_ID: String(this.shardList[0]),
      LAST_SHARD_ID: String(this.shardList[this.shardList.length - 1]),
      CLUSTER_MANAGER: 'true',
      CLUSTER_MANAGER_MODE: this._options.mode,
    };

    if (this._options.mode === 'process') {
      const child = fork(this._options.file, this._options.args ?? [], {
        env,
        execArgv: this._options.execArgv ?? [],
        silent: false,
      });
      this._process = child;
      child.on('message', (msg) => this._handleRawMessage(msg as RawIPCMessage));
      child.on('error', (err) => this.emit('error', err));
      child.on('exit', (code, signal) => this._handleExit(code, signal));
    } else {
      const worker = new Worker(this._options.file, {
        env,
        argv: this._options.args ?? [],
        execArgv: this._options.execArgv ?? [],
      });
      this._process = worker;
      worker.on('message', (msg) => this._handleRawMessage(msg as RawIPCMessage));
      worker.on('error', (err) => this.emit('error', err));
      worker.on('exit', (code) => this._handleExit(code, null));
    }

    this._spawnedAt = Date.now();
    this.emit('spawn');

    return new Promise<void>((resolve, reject) => {
      const timer =
        timeout > 0
          ? setTimeout(() => {
              reject(
                new Error(
                  `Cluster ${this.id} did not signal ready within ${timeout}ms`,
                ),
              );
            }, timeout)
          : null;

      const onReady = () => {
        if (timer) clearTimeout(timer);
        resolve();
      };
      const onError = (err: Error) => {
        if (timer) clearTimeout(timer);
        reject(err);
      };

      this.once('ready', onReady);
      this.once('error', onError);
    });
  }

  /**
   * Kill the cluster process immediately.
   * All other clusters are completely unaffected.
   */
  async kill(reason = 'unknown'): Promise<void> {
    this._log.info(`Killing (reason: ${reason})`);
    this.status = ClusterStatus.DEAD;
    this._rejectAllPending(new Error(`Cluster ${this.id} was killed: ${reason}`));

    if (this._process instanceof Worker) {
      await this._process.terminate();
    } else if (this._process) {
      this._process.kill('SIGTERM');
    }
    this._process = null;
  }

  /**
   * Respawn the cluster: kill then re-spawn.
   * No other clusters are touched.
   */
  async respawn(timeout = 30_000): Promise<void> {
    this._log.info('Respawning…');
    this.status = ClusterStatus.RESTARTING;
    this.restarts++;
    this.restartHistory.push(Date.now());

    if (this._process) await this.kill('respawn');
    this._process = null;

    await this.spawn(timeout);
  }

  /**
   * Send a fire-and-forget message to the cluster.
   */
  send(data: RawIPCMessage): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (!this._process) {
        reject(new Error(`Cluster ${this.id} is not running`));
        return;
      }
      if (this._process instanceof Worker) {
        this._process.postMessage(data);
        resolve();
      } else {
        (this._process as ChildProcess).send(data, (err) =>
          err ? reject(err) : resolve(),
        );
      }
    });
  }

  /**
   * Send a request and wait for a reply with the same nonce.
   */
  request<T = unknown>(data: Record<string, unknown>, timeout = 30_000): Promise<T> {
    const nonce = generateNonce();
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(nonce);
        reject(new Error(`Request to cluster ${this.id} timed out after ${timeout}ms`));
      }, timeout);

      this._pending.set(nonce, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });

      this.send({
        ...data,
        _type: (data['_type'] as MessageType) ?? MessageType.CUSTOM_REQUEST,
        _nonce: nonce,
        _clusterId: this.id,
      } as RawIPCMessage).catch(reject);
    });
  }

  /**
   * Evaluate a function (or string) inside this cluster's Discord client.
   */
  eval<T = unknown>(
    script: string | ((client: unknown, ctx?: unknown) => T),
    options: EvalOptions = {},
  ): Promise<T> {
    const serialized =
      typeof script === 'function' ? script.toString() : script;

    return this.request<T>(
      {
        _type: MessageType.BROADCAST_EVAL,
        script: serialized,
        context: options.context ?? {},
      },
      options.timeout ?? 10_000,
    );
  }

  /**
   * Send a graceful-shutdown signal and wait for ack, then kill.
   */
  async gracefulShutdown(options: RemoveClusterOptions = {}): Promise<void> {
    const { graceful = true, timeout = 10_000, reason = 'graceful shutdown' } = options;

    if (graceful && this._process) {
      try {
        await this.request(
          { _type: MessageType.GRACEFUL_SHUTDOWN, reason },
          timeout,
        );
      } catch {
        this._log.warn('Graceful shutdown timed out — force killing.');
      }
    }

    await this.kill(reason);
  }

  // ─── Stats ─────────────────────────────────────────────────────────────────

  getStats(): ClusterStats {
    return {
      id: this.id,
      status: this.status,
      uptime: this._spawnedAt ? Date.now() - this._spawnedAt : 0,
      restarts: this.restarts,
      memoryUsage: null,
      guilds: 0,
      shards: this.shardList.length,
    };
  }

  get uptime(): number {
    return this._spawnedAt ? Date.now() - this._spawnedAt : 0;
  }

  // ─── Internal ──────────────────────────────────────────────────────────────

  private _handleRawMessage(raw: RawIPCMessage): void {
    const msg = new IPCMessage(raw);

    switch (msg._type) {
      case MessageType.CLUSTER_READY:
        this.status = ClusterStatus.READY;
        this._log.info('Ready');
        this.emit('ready');
        break;

      case MessageType.HEARTBEAT:
        // Ack the heartbeat back to the cluster
        this.send({ _type: MessageType.HEARTBEAT_ACK, _nonce: msg._nonce }).catch(() => null);
        this._manager.emit('heartbeat', this);
        break;

      case MessageType.CUSTOM_REPLY:
      case MessageType.BROADCAST_EVAL_RESPONSE:
      case MessageType.MANAGER_EVAL_RESPONSE:
      case MessageType.CLIENT_VALUES_RESPONSE:
      case MessageType.METRICS_RESPONSE:
        if (msg._nonce) this._resolveRequest(msg._nonce, raw);
        break;

      case MessageType.BROADCAST_EVAL:
        // Cluster wants to broadcastEval — forward to manager
        this._manager._handleClusterBroadcastEvalRequest(this, msg);
        break;

      case MessageType.MANAGER_EVAL:
        // Cluster wants to eval on manager process
        this._manager._handleManagerEvalRequest(this, msg);
        break;

      case MessageType.SPAWN_NEXT_CLUSTER:
        this._manager.queue?.next();
        break;

      default:
        this.emit('message', msg);
        this._manager.emit('clusterMessage', this, msg);
        break;
    }
  }

  private _resolveRequest(nonce: string, data: RawIPCMessage): void {
    const pending = this._pending.get(nonce);
    if (!pending) return;
    clearTimeout(pending.timer);
    this._pending.delete(nonce);

    if (data['_error']) {
      pending.reject(new Error(String(data['_error'])));
    } else {
      pending.resolve(data['_result'] ?? data);
    }
  }

  private _rejectAllPending(reason: Error): void {
    for (const [nonce, req] of this._pending) {
      clearTimeout(req.timer);
      req.reject(reason);
      this._pending.delete(nonce);
    }
  }

  private _handleExit(code: number | null, signal: string | null): void {
    this._log.warn(`Exited (code=${code ?? 'null'} signal=${signal ?? 'none'})`);
    this.status = ClusterStatus.DEAD;
    this._process = null;
    this._rejectAllPending(new Error(`Cluster ${this.id} exited unexpectedly`));
    this.emit('death', code, signal);
    this._manager.emit('clusterDeath', this, code, signal);
  }
}
