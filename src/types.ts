// ─── Manager Options ─────────────────────────────────────────────────────────

export interface ShardingManagerOptions {
  /** Number of total internal Discord gateway shards. Default: 'auto' (fetches from Discord) */
  totalShards?: number | 'auto';
  /** Number of cluster processes/workers to spawn. Default: 'auto' (CPU count) */
  totalClusters?: number | 'auto';
  /** Shards per cluster. Overrides totalClusters when set. */
  shardsPerCluster?: number;
  /** Specific shard IDs to handle (useful for cross-host setups). Default: all shards */
  shardList?: number[];
  /** Execution mode: child processes or worker threads. Default: 'process' */
  mode?: 'process' | 'worker';
  /** Auto-respawn dead clusters. Default: true */
  respawn?: boolean;
  /** CLI arguments forwarded to the bot script */
  shardArgs?: string[];
  /** Node.js execArgv forwarded to child processes */
  execArgv?: string[];
  /** Extra environment variables injected into every cluster */
  env?: Record<string, string>;
  /** Discord bot token — required when totalShards is 'auto' */
  token?: string;
  /** Heartbeat monitoring options */
  heartbeat?: Partial<HeartbeatOptions>;
  /** Per-cluster restart throttle */
  restarts?: Partial<RestartOptions>;
  /** Cluster spawn queue options */
  queue?: Partial<QueueOptions>;
  /**
   * Target guild count per shard.
   * - number  → treat that value as the max guilds/shard ceiling for the AutoScaler.
   *             Scale-up fires when any shard exceeds this; scale-down fires when the
   *             average drops below `guildsPerShard / 4`.
   * - 'auto'  → let the system decide: uses Discord's recommended ceiling of 2 000
   *             guilds/shard for scale-up, and 300 guilds/shard as the scale-down floor.
   * - 0 / undefined → disabled (AutoScaler uses its own plugin-level options instead).
   */
  guildsPerShard?: number | 'auto';
  /**
   * Enable or disable all internal console logging. Default: `true`.
   * Set to `false` to silence everything from the manager, clusters, queue and plugins.
   */
  logging?: boolean;
}

// ─── Plugin Interface ─────────────────────────────────────────────────────────

export interface IPlugin {
  name: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  build(manager: any): void;
}

// ─── Sub-options ─────────────────────────────────────────────────────────────

export interface HeartbeatOptions {
  /** Interval between expected heartbeats in ms. Default: 30_000 */
  interval: number;
  /** Max consecutive missed heartbeats before respawning. Default: 3 */
  maxMissedBeats: number;
}

export interface RestartOptions {
  /** Max restarts allowed within interval. Default: 5 */
  max: number;
  /** Rolling window in ms to count restarts. Default: 3_600_000 (1 h) */
  interval: number;
}

export interface QueueOptions {
  /** Automatically start processing the spawn queue. Default: true */
  auto: boolean;
  /** Time in ms to wait for each cluster to become ready. Default: 30_000 */
  timeout: number;
  /** Delay in ms between spawning consecutive clusters. Default: 7_000 */
  delay: number;
}

// ─── Internal Cluster Options ─────────────────────────────────────────────────

export interface ClusterSpawnOptions {
  id: number;
  shardList: number[];
  totalShards: number;
  clusterCount: number;
  mode: 'process' | 'worker';
  file: string;
  env?: Record<string, string>;
  args?: string[];
  execArgv?: string[];
}

// ─── Cluster Info (available in bot process) ──────────────────────────────────

export interface ClusterInfo {
  CLUSTER_ID: number;
  CLUSTER_COUNT: number;
  SHARD_LIST: number[];
  TOTAL_SHARDS: number;
  FIRST_SHARD_ID: number;
  LAST_SHARD_ID: number;
  MAINTENANCE: string | null;
}

// ─── Cluster Stats ────────────────────────────────────────────────────────────

export interface ClusterStats {
  id: number;
  status: ClusterStatus;
  uptime: number;
  restarts: number;
  memoryUsage: NodeJS.MemoryUsage | null;
  guilds: number;
  shards: number;
}

// ─── Enums ────────────────────────────────────────────────────────────────────

export enum ClusterStatus {
  IDLE = 'IDLE',
  SPAWNING = 'SPAWNING',
  READY = 'READY',
  RESTARTING = 'RESTARTING',
  DEAD = 'DEAD',
}

export enum MessageType {
  // Lifecycle
  CLUSTER_READY = 0,
  HEARTBEAT = 1,
  HEARTBEAT_ACK = 2,
  MAINTENANCE_ENABLE = 3,
  MAINTENANCE_DISABLE = 4,
  GRACEFUL_SHUTDOWN = 5,
  CLUSTER_INFO_UPDATE = 6,

  // Custom IPC
  CUSTOM_MESSAGE = 10,
  CUSTOM_REQUEST = 11,
  CUSTOM_REPLY = 12,

  // BroadcastEval
  BROADCAST_EVAL = 20,
  BROADCAST_EVAL_RESPONSE = 21,

  // Manager eval
  MANAGER_EVAL = 22,
  MANAGER_EVAL_RESPONSE = 23,

  // Client value fetch
  CLIENT_VALUES = 24,
  CLIENT_VALUES_RESPONSE = 25,

  // Queue
  SPAWN_NEXT_CLUSTER = 30,

  // Metrics
  METRICS_REQUEST = 40,
  METRICS_RESPONSE = 41,
}

// ─── IPC Message ─────────────────────────────────────────────────────────────

export interface RawIPCMessage {
  _type: MessageType;
  _nonce?: string;
  _clusterId?: number;
  [key: string]: unknown;
}

// ─── Action Options ───────────────────────────────────────────────────────────

export interface SpawnOptions {
  delay?: number;
  timeout?: number;
}

export interface RespawnAllOptions {
  /** Delay between spawning each new cluster (ms). Default: 7_000 */
  clusterDelay?: number;
  /** Delay before respawning a cluster after kill (ms). Default: 500 */
  respawnDelay?: number;
  /** Timeout per cluster to become ready (ms). Default: 30_000 */
  timeout?: number;
}

/**
 * Options for hot-adding a cluster without touching any existing clusters.
 *
 * If `shards` is not provided, the manager automatically picks the next
 * unassigned shard IDs within `totalShards`.  Pass `shards` explicitly when
 * you want precise control (e.g. after scaling up `totalShards` externally).
 */
export interface AddClusterOptions {
  /** Explicit shard IDs to assign to the new cluster. */
  shards?: number[];
  /** How many shards to pull from the unassigned pool. Defaults to `shardsPerCluster`. */
  shardsPerCluster?: number;
  /** Ms to wait for the new cluster to signal ready. Default: 30_000 */
  timeout?: number;
}

/**
 * Options for hot-removing a single cluster.
 * All other clusters remain completely untouched.
 */
export interface RemoveClusterOptions {
  /** Send a graceful-shutdown notice and wait for the cluster to ack. Default: true */
  graceful?: boolean;
  /** Timeout (ms) for graceful ack before force-killing. Default: 10_000 */
  timeout?: number;
  /** Human-readable reason forwarded to the cluster process. */
  reason?: string;
}

export interface EvalOptions {
  /** Eval on a specific cluster ID only. */
  cluster?: number;
  /** Timeout in ms. Default: 10_000 */
  timeout?: number;
  /** Serialisable context object passed as second arg to the eval fn. */
  context?: Record<string, unknown>;
}

// ─── Internal pending-request bookkeeping ─────────────────────────────────────

export interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: NodeJS.Timeout;
}

export interface PendingBroadcast {
  results: unknown[];
  received: number;
  total: number;
  resolve: (value: unknown[]) => void;
  reject: (reason: Error) => void;
  timer: NodeJS.Timeout;
}
