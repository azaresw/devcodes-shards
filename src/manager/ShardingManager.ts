import { EventEmitter } from 'events';
import * as os from 'os';
import { Cluster } from './Cluster';
import { ClusterQueue } from './ClusterQueue';
import {
  AddClusterOptions,
  ClusterSpawnOptions,
  ClusterStats,
  ClusterStatus,
  EvalOptions,
  IPlugin,
  MessageType,
  PendingBroadcast,
  RawIPCMessage,
  RemoveClusterOptions,
  RespawnAllOptions,
  ShardingManagerOptions,
  SpawnOptions,
} from '../types';
import { IPCMessage } from '../ipc/IPCMessage';
import {
  calcShardId,
  chunkArray,
  fetchGatewayShards,
  generateNonce,
  sleep,
} from '../util/Util';
import { Logger, LogLevel } from '../util/Logger';

export declare interface ShardingManager {
  // Cluster lifecycle
  on(event: 'clusterCreate', listener: (cluster: Cluster) => void): this;
  on(event: 'clusterReady', listener: (cluster: Cluster) => void): this;
  on(event: 'clusterDeath', listener: (cluster: Cluster, code: number | null, signal: string | null) => void): this;
  on(event: 'clusterRespawn', listener: (cluster: Cluster) => void): this;
  on(event: 'clusterAdd', listener: (cluster: Cluster) => void): this;
  on(event: 'clusterRemove', listener: (clusterId: number, reason: string) => void): this;
  // IPC
  on(event: 'clusterMessage', listener: (cluster: Cluster, msg: IPCMessage) => void): this;
  on(event: 'heartbeat', listener: (cluster: Cluster) => void): this;
  // Debug
  on(event: 'debug', listener: (msg: string) => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
}

/**
 * # ShardingManager
 *
 * The main entry point. Run this in your `cluster.js` file.
 *
 * ## Quick start
 * ```js
 * const { ShardingManager } = require('devcodes-sharding');
 * const manager = new ShardingManager('./bot.js', {
 *   totalShards: 'auto',
 *   shardsPerCluster: 4,
 *   token: process.env.TOKEN,
 * });
 * manager.spawn();
 * ```
 *
 * ## Hot-add a cluster (zero downtime, no other clusters touched)
 * ```js
 * const cluster = await manager.addCluster();
 * ```
 *
 * ## Hot-remove a cluster (only that cluster dies)
 * ```js
 * await manager.removeCluster(2);
 * ```
 */
export class ShardingManager extends EventEmitter {
  /** All active clusters, keyed by cluster ID. */
  readonly clusters = new Map<number, Cluster>();
  /** The spawn queue (accessible for manual control). */
  readonly queue: ClusterQueue;

  readonly file: string;
  readonly options: Required<ShardingManagerOptions>;

  private _totalShards = 0;
  private _totalClusters = 0;
  private readonly _log: Logger;

  /** Pending broadcastEval requests from clusters or the manager itself. */
  private readonly _pendingBroadcasts = new Map<string, PendingBroadcast>();

  /** Registered plugins. */
  private readonly _plugins = new Map<string, IPlugin>();

  constructor(file: string, options: ShardingManagerOptions = {}) {
    super();
    this.file = file;

    const loggingEnabled = options.logging ?? true;

    this.options = {
      totalShards: options.totalShards ?? 'auto',
      totalClusters: options.totalClusters ?? 'auto',
      shardsPerCluster: options.shardsPerCluster ?? 0,
      shardList: options.shardList ?? [],
      mode: options.mode ?? 'process',
      respawn: options.respawn ?? true,
      shardArgs: options.shardArgs ?? [],
      execArgv: options.execArgv ?? [],
      env: options.env ?? {},
      token: options.token ?? '',
      heartbeat: { interval: 30_000, maxMissedBeats: 3, ...(options.heartbeat ?? {}) },
      restarts: { max: 5, interval: 3_600_000, ...(options.restarts ?? {}) },
      queue: { auto: true, timeout: 30_000, delay: 7_000, ...(options.queue ?? {}) },
      guildsPerShard: options.guildsPerShard ?? 0,
      logging: loggingEnabled,
    } as Required<ShardingManagerOptions>;

    this._log = new Logger('ShardingManager', LogLevel.INFO, loggingEnabled);

    this.queue = new ClusterQueue(
      this.options.queue.delay,
      this.options.queue.timeout,
      loggingEnabled,
    );

    this._log.info(`Initialized (mode=${this.options.mode})`);
  }

  // ─── Initial Spawn ─────────────────────────────────────────────────────────

  /**
   * Resolve shard/cluster counts and spawn all initial clusters.
   */
  async spawn(options: SpawnOptions = {}): Promise<Map<number, Cluster>> {
    const delay = options.delay ?? this.options.queue.delay;
    const timeout = options.timeout ?? this.options.queue.timeout;

    // 1. Resolve totalShards
    if (this.options.totalShards === 'auto') {
      if (!this.options.token) throw new Error("token is required when totalShards is 'auto'");
      this._log.info('Fetching recommended shard count from Discord…');
      this._totalShards = await fetchGatewayShards(this.options.token);
      this._log.info(`Discord recommends ${this._totalShards} shards.`);
    } else {
      this._totalShards = this.options.totalShards as number;
    }

    // 2. Resolve shardList
    const shardList =
      this.options.shardList.length > 0
        ? this.options.shardList
        : Array.from({ length: this._totalShards }, (_, i) => i);

    // 3. Resolve shardsPerCluster / totalClusters
    let shardsPerCluster: number;
    if (this.options.shardsPerCluster > 0) {
      shardsPerCluster = this.options.shardsPerCluster;
    } else if (this.options.totalClusters !== 'auto') {
      const tc = this.options.totalClusters as number;
      shardsPerCluster = Math.ceil(shardList.length / tc);
    } else {
      shardsPerCluster = Math.ceil(shardList.length / os.cpus().length);
    }

    const shardChunks = chunkArray(shardList, shardsPerCluster);
    this._totalClusters = shardChunks.length;

    this._log.info(
      `Spawning ${this._totalClusters} clusters — ${this._totalShards} total shards, ${shardsPerCluster} shards/cluster`,
    );

    // 4. Enqueue each cluster
    shardChunks.forEach((chunk, i) => {
      const cluster = this._createCluster(i, chunk);
      this.queue.push({
        id: i,
        run: () => cluster.spawn(timeout),
      });
    });

    if (this.options.queue.auto) {
      if (delay !== undefined) this.queue.delay = delay;
      await this.queue.start();
    }

    return this.clusters;
  }

  // ─── Hot-Add / Hot-Remove ──────────────────────────────────────────────────

  /**
   * Hot-add a new cluster **without touching any existing clusters**.
   *
   * The new cluster will handle whichever shard IDs you assign to it.
   * Existing clusters keep running without interruption.
   *
   * If `options.shards` is omitted, the manager automatically picks the next
   * unassigned shard IDs within `totalShards`.
   *
   * @example
   * // Auto-pick next unassigned shards (2 shards)
   * const cluster = await manager.addCluster({ shardsPerCluster: 2 });
   *
   * // Explicit shard IDs
   * const cluster = await manager.addCluster({ shards: [16, 17] });
   */
  async addCluster(options: AddClusterOptions = {}): Promise<Cluster> {
    const { timeout = 30_000 } = options;

    let shardList: number[];

    if (options.shards && options.shards.length > 0) {
      shardList = options.shards;
    } else {
      // Find unassigned shard IDs within totalShards
      const assigned = new Set(
        [...this.clusters.values()].flatMap((c) => c.shardList),
      );
      const all = Array.from({ length: this._totalShards }, (_, i) => i);
      const unassigned = all.filter((s) => !assigned.has(s));

      if (unassigned.length === 0) {
        throw new Error(
          `No unassigned shards available within totalShards (${this._totalShards}). ` +
          `Increase totalShards first, or pass explicit shard IDs via options.shards.`,
        );
      }

      const count =
        options.shardsPerCluster ??
        (this.options.shardsPerCluster > 0 ? this.options.shardsPerCluster : 1);

      shardList = unassigned.slice(0, count);
    }

    const id = this._nextClusterId();
    this._totalClusters++;

    this._log.info(
      `Hot-adding cluster #${id} with shards [${shardList.join(', ')}]…`,
    );

    const cluster = this._createCluster(id, shardList);
    await cluster.spawn(timeout);

    this.emit('clusterAdd', cluster);
    this._log.info(`Cluster #${id} is ready (hot-add complete).`);

    return cluster;
  }

  /**
   * Hot-remove a single cluster **without touching any other cluster**.
   *
   * Optionally sends a graceful-shutdown signal and waits for the cluster
   * to acknowledge before killing it.
   *
   * @example
   * await manager.removeCluster(3);                       // graceful
   * await manager.removeCluster(3, { graceful: false });  // immediate kill
   */
  async removeCluster(id: number, options: RemoveClusterOptions = {}): Promise<void> {
    const cluster = this.clusters.get(id);
    if (!cluster) throw new Error(`Cluster ${id} not found.`);

    this._log.info(`Hot-removing cluster #${id}…`);
    await cluster.gracefulShutdown(options);
    this.clusters.delete(id);

    this.emit('clusterRemove', id, options.reason ?? 'manual remove');
    this._log.info(`Cluster #${id} removed. All other clusters unaffected.`);
  }

  /**
   * Respawn a **single** cluster without touching any other.
   */
  async respawnCluster(id: number, timeout = 30_000): Promise<Cluster> {
    const cluster = this.clusters.get(id);
    if (!cluster) throw new Error(`Cluster ${id} not found.`);

    this._log.info(`Respawning cluster #${id}…`);
    await cluster.respawn(timeout);
    this.emit('clusterRespawn', cluster);
    return cluster;
  }

  /**
   * Rolling respawn of ALL clusters, one at a time.
   * Existing clusters keep running until their turn.
   */
  async respawnAll(options: RespawnAllOptions = {}): Promise<Map<number, Cluster>> {
    const {
      clusterDelay = 7_000,
      respawnDelay = 500,
      timeout = 30_000,
    } = options;

    this._log.info('Rolling respawn of all clusters…');

    let first = true;
    for (const cluster of this.clusters.values()) {
      if (!first) await sleep(clusterDelay);
      first = false;
      await sleep(respawnDelay);
      await cluster.respawn(timeout);
      this.emit('clusterRespawn', cluster);
    }

    return this.clusters;
  }

  // ─── Eval / IPC ────────────────────────────────────────────────────────────

  /**
   * Evaluate a function (or string) on every cluster's Discord client.
   * Returns an array of results in cluster-ID order.
   *
   * @example
   * const counts = await manager.broadcastEval(client => client.guilds.cache.size);
   * const total = counts.reduce((a, b) => a + b, 0);
   */
  async broadcastEval<T = unknown>(
    script: string | ((client: unknown, ctx?: unknown) => T),
    options: EvalOptions = {},
  ): Promise<T[]> {
    const serialized =
      typeof script === 'function' ? script.toString() : script;

    // Target a single cluster?
    if (options.cluster !== undefined) {
      const cluster = this.clusters.get(options.cluster);
      if (!cluster) throw new Error(`Cluster ${options.cluster} not found.`);
      const result = await cluster.eval<T>(script, options);
      return [result];
    }

    // Broadcast to all
    const nonce = generateNonce();
    const total = this.clusters.size;
    if (total === 0) return [];

    const results = new Array<T>(total).fill(undefined as unknown as T);

    return new Promise<T[]>((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingBroadcasts.delete(nonce);
        reject(new Error('broadcastEval timed out'));
      }, options.timeout ?? 10_000);

      this._pendingBroadcasts.set(nonce, {
        results: results as unknown[],
        received: 0,
        total,
        resolve: (r) => { clearTimeout(timer); resolve(r as T[]); },
        reject: (e) => { clearTimeout(timer); reject(e); },
        timer,
      });

      // Send eval request to every cluster
      for (const cluster of this.clusters.values()) {
        cluster
          .send({
            _type: MessageType.BROADCAST_EVAL,
            _nonce: nonce,
            script: serialized,
            context: options.context ?? {},
          })
          .catch((err: Error) => {
            const pending = this._pendingBroadcasts.get(nonce);
            if (pending) {
              clearTimeout(pending.timer);
              this._pendingBroadcasts.delete(nonce);
              pending.reject(err);
            }
          });
      }
    });
  }

  /**
   * Fetch a property from every cluster's Discord client.
   * Equivalent to `broadcastEval(c => c.PROP)`.
   *
   * @example
   * const pings = await manager.fetchClientValues('ws.ping');
   */
  async fetchClientValues(prop: string): Promise<unknown[]> {
    return this.broadcastEval(
      new Function(`return this.${prop}`) as (c: unknown) => unknown,
    );
  }

  /**
   * Evaluate a function synchronously on the manager process itself.
   *
   * @example
   * const clusterCount = manager.evalOnManager(m => m.clusters.size);
   */
  evalOnManager<T>(fn: (manager: this) => T): T {
    return fn(this);
  }

  // ─── Utility ───────────────────────────────────────────────────────────────

  /**
   * Get the Discord shard ID for a guild ID.
   */
  getShardIdForGuildId(guildId: string): number {
    return calcShardId(guildId, this._totalShards);
  }

  /**
   * Get the cluster ID responsible for a given guild ID.
   * Returns `null` if no cluster covers that shard.
   */
  getClusterIdForGuildId(guildId: string): number | null {
    const shardId = this.getShardIdForGuildId(guildId);
    for (const cluster of this.clusters.values()) {
      if (cluster.shardList.includes(shardId)) return cluster.id;
    }
    return null;
  }

  /** Fetch stats from every cluster. */
  async getStats(): Promise<ClusterStats[]> {
    return [...this.clusters.values()].map((c) => c.getStats());
  }

  /** Total number of resolved shards. */
  get totalShards(): number {
    return this._totalShards;
  }

  /** Total number of spawned clusters. */
  get totalClusters(): number {
    return this._totalClusters;
  }

  // ─── Plugin System ─────────────────────────────────────────────────────────

  /**
   * Register a plugin.
   *
   * @example
   * const { HeartbeatManager } = require('devcodes-sharding');
   * manager.extend(new HeartbeatManager({ interval: 20_000 }));
   */
  extend(plugin: IPlugin): this {
    if (this._plugins.has(plugin.name)) {
      throw new Error(`Plugin "${plugin.name}" is already registered.`);
    }
    this._plugins.set(plugin.name, plugin);
    plugin.build(this);
    this._log.info(`Plugin "${plugin.name}" registered.`);
    return this;
  }

  getPlugin<T extends IPlugin = IPlugin>(name: string): T | undefined {
    return this._plugins.get(name) as T | undefined;
  }

  // ─── Internal ──────────────────────────────────────────────────────────────

  private _createCluster(id: number, shardList: number[]): Cluster {
    const opts: ClusterSpawnOptions = {
      id,
      shardList,
      totalShards: this._totalShards,
      clusterCount: this._totalClusters,
      mode: this.options.mode,
      file: this.file,
      env: this.options.env,
      args: this.options.shardArgs,
      execArgv: this.options.execArgv,
    };

    const cluster = new Cluster(this, opts);

    // Auto-respawn on death
    cluster.on('death', (code, signal) => {
      if (!this.options.respawn) return;
      if (cluster.status === ClusterStatus.DEAD) {
        this._throttledRespawn(cluster);
      }
    });

    cluster.on('ready', () => {
      this.emit('clusterReady', cluster);
    });

    this.clusters.set(id, cluster);
    this.emit('clusterCreate', cluster);
    return cluster;
  }

  private _nextClusterId(): number {
    let id = 0;
    while (this.clusters.has(id)) id++;
    return id;
  }

  private async _throttledRespawn(cluster: Cluster): Promise<void> {
    const max = this.options.restarts.max ?? 5;
    const interval = this.options.restarts.interval ?? 3_600_000;
    const now = Date.now();

    // Prune old restart history
    const recent = cluster.restartHistory.filter((t) => now - t < interval);
    cluster.restartHistory.length = 0;
    cluster.restartHistory.push(...recent);

    if (cluster.restartHistory.length >= max) {
      this._log.error(
        `Cluster ${cluster.id} exceeded ${max} restarts in ${interval}ms — giving up.`,
      );
      this.emit('debug', `Cluster ${cluster.id} restart limit reached`);
      return;
    }

    this._log.warn(`Auto-respawning cluster ${cluster.id}…`);
    try {
      await cluster.respawn(this.options.queue.timeout);
      this.emit('clusterRespawn', cluster);
    } catch (err) {
      this._log.error(`Cluster ${cluster.id} failed to respawn`, err);
    }
  }

  /** Called by Cluster when a cluster wants to broadcastEval (forwarded from bot process). */
  _handleClusterBroadcastEvalRequest(sourceCluster: Cluster, msg: IPCMessage): void {
    const nonce = msg._nonce;
    const script = msg['script'] as string;
    const context = (msg['context'] ?? {}) as Record<string, unknown>;

    if (!nonce) return;

    const total = this.clusters.size;
    const aggregated = new Array<unknown>(total).fill(undefined);
    let received = 0;

    const broadcastNonce = generateNonce();

    this._pendingBroadcasts.set(broadcastNonce, {
      results: aggregated,
      received: 0,
      total,
      timer: setTimeout(() => {
        this._pendingBroadcasts.delete(broadcastNonce);
        sourceCluster
          .send({
            _type: MessageType.BROADCAST_EVAL_RESPONSE,
            _nonce: nonce,
            _error: 'broadcastEval timed out',
          } as RawIPCMessage)
          .catch(() => null);
      }, 10_000),
      resolve: (results) => {
        sourceCluster
          .send({
            _type: MessageType.BROADCAST_EVAL_RESPONSE,
            _nonce: nonce,
            _result: results,
          } as RawIPCMessage)
          .catch(() => null);
      },
      reject: (err) => {
        sourceCluster
          .send({
            _type: MessageType.BROADCAST_EVAL_RESPONSE,
            _nonce: nonce,
            _error: err.message,
          } as RawIPCMessage)
          .catch(() => null);
      },
    });

    // Send to all clusters
    for (const cluster of this.clusters.values()) {
      cluster
        .send({
          _type: MessageType.BROADCAST_EVAL,
          _nonce: broadcastNonce,
          script,
          context,
        } as RawIPCMessage)
        .catch(() => null);
    }
  }

  /** Called by Cluster when a cluster wants to eval on the manager. */
  _handleManagerEvalRequest(sourceCluster: Cluster, msg: IPCMessage): void {
    const nonce = msg._nonce;
    const script = msg['script'] as string;

    if (!nonce) return;

    try {
      // eslint-disable-next-line no-new-func
      const fn = new Function('manager', `return (${script})(manager)`);
      const result = fn(this);
      sourceCluster
        .send({
          _type: MessageType.MANAGER_EVAL_RESPONSE,
          _nonce: nonce,
          _result: result,
        } as RawIPCMessage)
        .catch(() => null);
    } catch (err) {
      sourceCluster
        .send({
          _type: MessageType.MANAGER_EVAL_RESPONSE,
          _nonce: nonce,
          _error: (err as Error).message,
        } as RawIPCMessage)
        .catch(() => null);
    }
  }

  /** Called internally by Cluster when a BROADCAST_EVAL_RESPONSE arrives at the manager. */
  _handleBroadcastEvalResponse(
    clusterId: number,
    nonce: string,
    result: unknown,
    error?: string,
  ): void {
    const pending = this._pendingBroadcasts.get(nonce);
    if (!pending) return;

    if (error) {
      pending.results[clusterId] = new Error(error);
    } else {
      pending.results[clusterId] = result;
    }

    pending.received++;
    if (pending.received >= pending.total) {
      clearTimeout(pending.timer);
      this._pendingBroadcasts.delete(nonce);
      pending.resolve(pending.results);
    }
  }
}
