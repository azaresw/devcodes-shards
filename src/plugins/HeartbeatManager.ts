import { IPlugin } from '../types';
import { Logger } from '../util/Logger';
import type { ShardingManager } from '../manager/ShardingManager';
import type { Cluster } from '../manager/Cluster';

export interface HeartbeatManagerOptions {
  /** Interval in ms the cluster should send heartbeats. Default: 30 000 */
  interval?: number;
  /** Max consecutive missed heartbeats before the cluster is respawned. Default: 3 */
  maxMissedBeats?: number;
}

interface BeatRecord {
  missed: number;
  lastBeat: number;
  timer: NodeJS.Timeout | null;
}

/**
 * HeartbeatManager plugin.
 *
 * Monitors every cluster for liveness. If a cluster stops sending heartbeats
 * for `maxMissedBeats * interval` ms it is automatically respawned.
 *
 * @example
 * const { HeartbeatManager } = require('devcodes-sharding');
 * manager.extend(new HeartbeatManager({ interval: 20_000, maxMissedBeats: 3 }));
 */
export class HeartbeatManager implements IPlugin {
  readonly name = 'HeartbeatManager';

  private readonly _opts: Required<HeartbeatManagerOptions>;
  private readonly _records = new Map<number, BeatRecord>();
  private readonly _log = new Logger('HeartbeatManager');
  private _manager!: ShardingManager;

  constructor(options: HeartbeatManagerOptions = {}) {
    this._opts = {
      interval: options.interval ?? 30_000,
      maxMissedBeats: options.maxMissedBeats ?? 3,
    };
  }

  build(manager: ShardingManager): void {
    this._manager = manager;

    // Track newly created clusters
    manager.on('clusterCreate', (cluster) => this._watch(cluster));

    // Each time a heartbeat arrives, reset the miss counter
    manager.on('heartbeat', (cluster: Cluster) => this._onBeat(cluster));

    // Clean up records on removal
    manager.on('clusterRemove', (id: number) => this._unwatch(id));

    // Also watch clusters that were already spawned before the plugin was registered
    for (const cluster of manager.clusters.values()) {
      this._watch(cluster);
    }

    this._log.info(
      `Active — interval=${this._opts.interval}ms maxMissedBeats=${this._opts.maxMissedBeats}`,
    );
  }

  private _watch(cluster: Cluster): void {
    const record: BeatRecord = {
      missed: 0,
      lastBeat: Date.now(),
      timer: null,
    };

    // Start a watchdog timer for this cluster
    record.timer = setInterval(() => this._check(cluster, record), this._opts.interval * 1.5);

    this._records.set(cluster.id, record);
  }

  private _unwatch(clusterId: number): void {
    const record = this._records.get(clusterId);
    if (record?.timer) clearInterval(record.timer);
    this._records.delete(clusterId);
  }

  private _onBeat(cluster: Cluster): void {
    const record = this._records.get(cluster.id);
    if (!record) return;
    record.missed = 0;
    record.lastBeat = Date.now();
  }

  private async _check(cluster: Cluster, record: BeatRecord): Promise<void> {
    const elapsed = Date.now() - record.lastBeat;
    if (elapsed < this._opts.interval * 1.5) return; // still within tolerance

    record.missed++;
    this._log.warn(
      `Cluster #${cluster.id} missed heartbeat (${record.missed}/${this._opts.maxMissedBeats})`,
    );

    if (record.missed >= this._opts.maxMissedBeats) {
      this._log.error(
        `Cluster #${cluster.id} is unresponsive — respawning…`,
      );
      record.missed = 0;
      record.lastBeat = Date.now();

      try {
        await this._manager.respawnCluster(cluster.id);
      } catch (err) {
        this._log.error(`Failed to respawn cluster #${cluster.id}`, err);
      }
    }
  }
}
