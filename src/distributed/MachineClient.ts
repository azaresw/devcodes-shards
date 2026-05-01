/**
 * MachineClient
 *
 * Runs on EACH machine in the fleet.  Pass it your local ShardingManager and
 * the address of the MachineCoordinator; it will:
 *
 *  - Register this machine's existing clusters with the coordinator on connect.
 *  - Forward every `clusterAdd` / `clusterRemove` event from the local manager
 *    to the coordinator so all other machines are notified.
 *  - Receive notifications from the coordinator when any OTHER machine adds or
 *    removes a cluster, keeping a shared global registry up to date.
 *  - Expose `getGlobalRegistry()` so your code can see every cluster on every
 *    machine, and `findClusterForGuild(guildId)` for cross-machine routing.
 *  - Auto-reconnect on disconnect (configurable).
 *
 * Protocol: newline-delimited JSON over plain TCP (same as MachineCoordinator).
 */
import * as net from 'net';
import { EventEmitter } from 'events';
import type { ShardingManager } from '../manager/ShardingManager';
import type { Cluster } from '../manager/Cluster';

// ─── Public types ─────────────────────────────────────────────────────────────

export interface MachineClientOptions {
  /**
   * A unique identifier for this machine.
   * Used in all cross-machine events so you can tell which machine triggered
   * an add/remove.  Must be unique across the fleet.
   */
  machineId: string;
  /** Host/IP of the MachineCoordinator. */
  coordinatorHost: string;
  /** TCP port of the MachineCoordinator. */
  coordinatorPort: number;
  /**
   * Must match `MachineCoordinatorOptions.password` if one was set.
   * Leave undefined when the coordinator has no password.
   */
  password?: string;
  /**
   * Automatically reconnect when the connection drops.  Default: `true`.
   */
  reconnect?: boolean;
  /**
   * Milliseconds to wait before each reconnect attempt.  Default: `5000`.
   */
  reconnectDelay?: number;
}

export interface RemoteClusterEntry {
  id: number;
  shardList: number[];
}

export interface GlobalMachineEntry {
  machineId: string;
  /** All clusters currently active on this machine. */
  clusters: RemoteClusterEntry[];
}

// ─── Class ────────────────────────────────────────────────────────────────────

export declare interface MachineClient {
  /** Emitted after a successful registration + SYNC round-trip. */
  on(event: 'sync', listener: (registry: GlobalMachineEntry[]) => void): this;
  /** Another machine connected to the coordinator. */
  on(event: 'machineJoin', listener: (machineId: string) => void): this;
  /** Another machine disconnected from the coordinator. */
  on(event: 'machineLeave', listener: (machineId: string) => void): this;
  /** A cluster was added on a REMOTE machine. */
  on(event: 'remoteClusterAdd', listener: (machineId: string, clusterId: number, shardList: number[]) => void): this;
  /** A cluster was removed on a REMOTE machine. */
  on(event: 'remoteClusterRemove', listener: (machineId: string, clusterId: number) => void): this;
  /** Connection to coordinator established. */
  on(event: 'connect', listener: () => void): this;
  /** Connection to coordinator lost (will reconnect if enabled). */
  on(event: 'disconnect', listener: () => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
}

export class MachineClient extends EventEmitter {
  readonly machineId: string;

  private readonly _manager: ShardingManager;
  private readonly _options: MachineClientOptions;

  /** Cached view of every OTHER machine's clusters. */
  private readonly _remoteMachines = new Map<string, GlobalMachineEntry>();

  private _socket: net.Socket | null = null;
  private _buf = '';
  private _destroyed = false;
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private _retrying = false;

  constructor(manager: ShardingManager, options: MachineClientOptions) {
    super();
    this._manager = manager;
    this._options = options;
    this.machineId = options.machineId;
    this._hookManager();
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Connect to the coordinator and register.
   * Resolves once the TCP connection is established (before SYNC arrives).
   */
  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this._destroyed) {
        return reject(new Error('MachineClient has been destroyed'));
      }

      const socket = new net.Socket();
      this._socket = socket;
      socket.setKeepAlive(true, 15_000);

      socket.connect(this._options.coordinatorPort, this._options.coordinatorHost, () => {
        this._register();
        this.emit('connect');
        resolve();
      });

      socket.on('data', chunk => {
        this._buf += chunk.toString('utf8');
        const lines = this._buf.split('\n');
        this._buf = lines.pop() ?? '';
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try {
            this._handleMessage(JSON.parse(trimmed) as Record<string, unknown>);
          } catch { /* malformed — ignore */ }
        }
      });

      socket.on('error', err => {
        this.emit('error', err);
        // Only reject for the initial connect() call — retries are handled by _startRetryLoop
        if (!this._retrying) reject(err);
      });

      socket.on('close', () => {
        this.emit('disconnect');
        if (!this._destroyed && (this._options.reconnect ?? true)) {
          this._startRetryLoop();
        }
      });
    });
  }

  /**
   * Returns a combined view of ALL machines' clusters (local + remote).
   * Useful for dashboards or routing decisions.
   */
  getGlobalRegistry(): GlobalMachineEntry[] {
    const local: GlobalMachineEntry = {
      machineId: this.machineId,
      clusters: this._getLocalClusters(),
    };
    return [local, ...Array.from(this._remoteMachines.values())];
  }

  /**
   * Given a guild ID, returns which machine + cluster handles it.
   * Uses the standard Discord shard formula: `(guildId >> 22) % totalShards`.
   *
   * Returns `null` if no registered cluster covers that shard.
   */
  findClusterForGuild(guildId: string): { machineId: string; clusterId: number } | null {
    // Access the internal _totalShards value via cast
    const totalShards: number = (this._manager as unknown as { _totalShards: number })._totalShards || 1;
    const shardId = Number(BigInt(guildId) >> 22n) % totalShards;

    // Check local clusters first
    for (const cluster of this._manager.clusters.values()) {
      if (cluster.shardList.includes(shardId)) {
        return { machineId: this.machineId, clusterId: cluster.id };
      }
    }

    // Check remote machines
    for (const [machineId, machine] of this._remoteMachines) {
      for (const cluster of machine.clusters) {
        if (cluster.shardList.includes(shardId)) {
          return { machineId, clusterId: cluster.id };
        }
      }
    }

    return null;
  }

  /**
   * Gracefully disconnect and stop all reconnect attempts.
   */
  disconnect(): void {
    this._destroyed = true;
    this._retrying = false;
    if (this._reconnectTimer !== null) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    this._socket?.destroy();
    this._socket = null;
  }

  // ─── Manager event hooks ─────────────────────────────────────────────────────

  private _hookManager(): void {
    this._manager.on('clusterAdd', (cluster: Cluster) => {
      this._send({
        type: 'CLUSTER_ADDED',
        machineId: this.machineId,
        clusterId: cluster.id,
        shardList: cluster.shardList,
      });
    });

    this._manager.on('clusterRemove', (clusterId: number) => {
      this._send({
        type: 'CLUSTER_REMOVED',
        machineId: this.machineId,
        clusterId,
      });
    });
  }

  // ─── Coordinator message handling ────────────────────────────────────────────

  private _handleMessage(msg: Record<string, unknown>): void {
    switch (msg.type) {
      case 'AUTH_FAILED':
        this.emit('error', new Error('[devcodes-sharding / MachineClient] Coordinator rejected: wrong password'));
        this._destroyed = true;
        this._socket?.destroy();
        break;

      case 'SYNC': {
        // Full snapshot of all OTHER machines already registered
        const machines = (msg.machines as GlobalMachineEntry[]) ?? [];
        for (const m of machines) {
          this._remoteMachines.set(m.machineId, {
            machineId: m.machineId,
            clusters: m.clusters ?? [],
          });
        }
        this.emit('sync', this.getGlobalRegistry());
        break;
      }

      case 'MACHINE_REGISTERED': {
        const machineId = String(msg.machineId);
        this._remoteMachines.set(machineId, {
          machineId,
          clusters: (msg.clusters as RemoteClusterEntry[]) ?? [],
        });
        this.emit('machineJoin', machineId);
        break;
      }

      case 'MACHINE_DISCONNECTED': {
        const machineId = String(msg.machineId);
        this._remoteMachines.delete(machineId);
        this.emit('machineLeave', machineId);
        break;
      }

      case 'CLUSTER_ADDED': {
        const machineId = String(msg.machineId);
        const clusterId = Number(msg.clusterId);
        const shardList = (msg.shardList as number[]) ?? [];

        let machine = this._remoteMachines.get(machineId);
        if (!machine) {
          machine = { machineId, clusters: [] };
          this._remoteMachines.set(machineId, machine);
        }
        // Replace or append
        machine.clusters = machine.clusters.filter(c => c.id !== clusterId);
        machine.clusters.push({ id: clusterId, shardList });

        this.emit('remoteClusterAdd', machineId, clusterId, shardList);
        break;
      }

      case 'CLUSTER_REMOVED': {
        const machineId = String(msg.machineId);
        const clusterId = Number(msg.clusterId);

        const machine = this._remoteMachines.get(machineId);
        if (machine) {
          machine.clusters = machine.clusters.filter(c => c.id !== clusterId);
        }

        this.emit('remoteClusterRemove', machineId, clusterId);
        break;
      }

      case 'PONG':
        // keepalive ack — nothing to do
        break;

      default:
        break;
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  private _register(): void {
    this._send({
      type: 'REGISTER',
      machineId: this.machineId,
      password: this._options.password,
      clusters: this._getLocalClusters(),
    });
  }

  private _getLocalClusters(): RemoteClusterEntry[] {
    return Array.from(this._manager.clusters.values()).map(c => ({
      id: c.id,
      shardList: [...c.shardList],
    }));
  }

  private _send(msg: object): void {
    if (this._socket?.writable) {
      try { this._socket.write(JSON.stringify(msg) + '\n'); } catch { /* ignore */ }
    }
  }

  private _scheduleReconnect(): void {
    if (this._reconnectTimer !== null) return;
    const delay = this._options.reconnectDelay ?? 5_000;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this.connect().catch(() => { /* will retry again on next close */ });
    }, delay);
  }

  private _startRetryLoop(): void {
    if (this._retrying || this._destroyed) return;
    this._retrying = true;
    const delay = this._options.reconnectDelay ?? 5_000;

    const attempt = (): void => {
      if (this._destroyed) { this._retrying = false; return; }
      this._reconnectTimer = setTimeout(() => {
        this._reconnectTimer = null;
        if (this._destroyed) { this._retrying = false; return; }

        const socket = new net.Socket();
        this._socket = socket;
        socket.setKeepAlive(true, 15_000);

        socket.connect(this._options.coordinatorPort, this._options.coordinatorHost, () => {
          this._retrying = false;
          this._register();
          this.emit('connect');
        });

        socket.on('data', chunk => {
          this._buf += chunk.toString('utf8');
          const lines = this._buf.split('\n');
          this._buf = lines.pop() ?? '';
          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            try {
              this._handleMessage(JSON.parse(trimmed) as Record<string, unknown>);
            } catch { /* malformed */ }
          }
        });

        socket.on('error', err => { this.emit('error', err); });

        socket.on('close', () => {
          this.emit('disconnect');
          if (!this._destroyed && (this._options.reconnect ?? true)) {
            attempt(); // schedule next attempt
          } else {
            this._retrying = false;
          }
        });
      }, delay);
    };

    attempt();
  }
}
