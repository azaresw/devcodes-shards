import { IPlugin } from '../types';
import { Logger, LogLevel } from '../util/Logger';
import type { ShardingManager } from '../manager/ShardingManager';

/* eslint-disable no-console */

export interface AutoScalerOptions {
  /**
   * How often to check shard load (ms). Default: 60_000.
   */
  checkInterval?: number;

  /**
   * Maximum guilds per shard ceiling (scale-UP threshold).
   *
   * Priority order:
   *  1. This option, if set explicitly.
   *  2. `manager.options.guildsPerShard` if it is a number.
   *  3. `2_000` when `manager.options.guildsPerShard === 'auto'`.
   *  4. `2_400` fallback default.
   */
  maxGuildsPerShard?: number;

  /**
   * Scale-DOWN floor — average guilds/shard must drop below this
   * before the emptiest cluster is removed.
   *
   * Defaults to `maxGuildsPerShard / 4` when not specified.
   */
  minGuildsPerShard?: number;

  /**
   * Whether to automatically remove clusters when load is too low.
   *
   * Defaults to `true` when `manager.options.guildsPerShard` is set,
   * `false` otherwise (safe default).
   */
  scaleDown?: boolean;

  /**
   * Shards per cluster for any newly spawned clusters.
   * Defaults to the manager's `shardsPerCluster` option.
   */
  shardsPerCluster?: number;

  /**
   * Custom function to collect per-shard guild counts.
   * Return `Array<[shardId, guildCount]>`.
   * Defaults to a discord.js-compatible broadcastEval.
   */
  collectData?: (manager: ShardingManager) => Promise<Array<[number, number]>>;
}

/**
 * AutoScaler plugin.
 *
 * Periodically collects per-shard guild counts and:
 * - **Scales UP**  — hot-adds a cluster when any shard exceeds the ceiling.
 * - **Scales DOWN** — hot-removes the emptiest cluster when the average load
 *   drops below the floor (opt-in via `scaleDown` or `manager.options.guildsPerShard`).
 *
 * All add/remove events are printed to the console with clear labels.
 * Set `manager.options.logging = false` to suppress all output.
 *
 * @example
 * // Plugin-level thresholds
 * manager.extend(new AutoScaler({ maxGuildsPerShard: 2000, scaleDown: true }));
 *
 * // Manager-level threshold (plugin reads it automatically)
 * const manager = new ShardingManager('./bot.js', {
 *   guildsPerShard: 1500,   // ceiling = 1500, floor = 375 (ceiling / 4)
 * });
 * manager.extend(new AutoScaler());
 *
 * // 'auto' mode — uses Discord's recommended 2 000 ceiling
 * const manager = new ShardingManager('./bot.js', { guildsPerShard: 'auto' });
 * manager.extend(new AutoScaler());
 */
export class AutoScaler implements IPlugin {
  readonly name = 'AutoScaler';

  private readonly _rawOpts: AutoScalerOptions;
  private _log!: Logger;
  private _manager!: ShardingManager;
  private _timer: NodeJS.Timeout | null = null;
  private _scaling = false;

  // Resolved thresholds (set in build() after manager is available)
  private _ceiling = 2_400;
  private _floor = 600;
  private _scaleDown = false;

  constructor(options: AutoScalerOptions = {}) {
    this._rawOpts = options;
  }

  build(manager: ShardingManager): void {
    this._manager = manager;
    const loggingEnabled = manager.options.logging ?? true;
    this._log = new Logger('AutoScaler', LogLevel.INFO, loggingEnabled);

    // ─── Resolve ceiling ────────────────────────────────────────────────────
    if (this._rawOpts.maxGuildsPerShard !== undefined) {
      // Explicit plugin option takes priority
      this._ceiling = this._rawOpts.maxGuildsPerShard;
    } else {
      const mgs = (manager.options as { guildsPerShard?: number | 'auto' }).guildsPerShard;
      if (typeof mgs === 'number' && mgs > 0) {
        this._ceiling = mgs;
      } else if (mgs === 'auto') {
        this._ceiling = 2_000; // Discord's recommended ceiling
      } else {
        this._ceiling = 2_400; // default
      }
    }

    // ─── Resolve floor ──────────────────────────────────────────────────────
    this._floor =
      this._rawOpts.minGuildsPerShard !== undefined
        ? this._rawOpts.minGuildsPerShard
        : Math.floor(this._ceiling / 4);

    // ─── Resolve scaleDown ──────────────────────────────────────────────────
    if (this._rawOpts.scaleDown !== undefined) {
      this._scaleDown = this._rawOpts.scaleDown;
    } else {
      // Auto-enable scale-down only when the user explicitly configured guildsPerShard
      const mgs = (manager.options as { guildsPerShard?: number | 'auto' }).guildsPerShard;
      this._scaleDown = mgs !== undefined && mgs !== 0;
    }

    const interval = this._rawOpts.checkInterval ?? 60_000;
    this._timer = setInterval(() => this._check(), interval);

    this._log.info(
      [
        `Active`,
        `check=${interval}ms`,
        `ceiling=${this._ceiling} guilds/shard`,
        `floor=${this._floor} guilds/shard`,
        `scaleDown=${this._scaleDown}`,
      ].join(' | '),
    );
  }

  /** Stop the periodic check. */
  stop(): void {
    if (this._timer) clearInterval(this._timer);
  }

  // ─── Core check loop ───────────────────────────────────────────────────────

  private async _check(): Promise<void> {
    if (this._scaling) return;

    try {
      const data = await this._collectGuildData();
      if (!data || data.length === 0) return;

      if (this._manager.options.logging) {
        const summary = data.map(([s, g]) => `shard-${s}: ${g}`).join(', ');
        this._manager.emit('debug', `[AutoScaler] load check — ${summary}`);
      }

      await this._checkScaleUp(data);
      if (this._scaleDown) await this._checkScaleDown(data);
    } catch (err) {
      this._log.error('Error during auto-scale check', err);
    }
  }

  // ─── Scale-UP ──────────────────────────────────────────────────────────────

  private async _checkScaleUp(data: Array<[number, number]>): Promise<void> {
    const overloaded = data.filter(([, guilds]) => guilds >= this._ceiling);
    if (overloaded.length === 0) return;

    this._scaling = true;
    try {
      const worst = overloaded.reduce((a, b) => (b[1] > a[1] ? b : a));

      if (this._manager.options.logging) {
        console.log(
          `\x1b[33m[devcodes-sharding / AutoScaler]\x1b[0m ` +
          `\x1b[1mSCALE UP\x1b[0m — ` +
          `${overloaded.length} shard(s) over ceiling (${this._ceiling} guilds/shard). ` +
          `Worst: shard-${worst[0]} with ${worst[1]} guilds. ` +
          `Adding a new cluster…`,
        );
      }

      this._log.warn(
        `${overloaded.length} shard(s) overloaded — hot-adding cluster…`,
      );

      const spc =
        (this._rawOpts.shardsPerCluster ?? 0) > 0
          ? this._rawOpts.shardsPerCluster
          : undefined;

      const cluster = await this._manager.addCluster({ shardsPerCluster: spc });

      if (this._manager.options.logging) {
        console.log(
          `\x1b[32m[devcodes-sharding / AutoScaler]\x1b[0m ` +
          `\x1b[1mSCALE UP COMPLETE\x1b[0m — ` +
          `Added cluster #${cluster.id} covering shards [${cluster.shardList.join(', ')}]. ` +
          `Total clusters: ${this._manager.clusters.size}.`,
        );
      }

      this._manager.emit('scaleUp', cluster);
    } catch (err) {
      this._log.error('Scale-up failed', err);
      if (this._manager.options.logging) {
        console.error(
          `\x1b[31m[devcodes-sharding / AutoScaler]\x1b[0m Scale-up failed:`,
          (err as Error).message,
        );
      }
    } finally {
      this._scaling = false;
    }
  }

  // ─── Scale-DOWN ────────────────────────────────────────────────────────────

  private async _checkScaleDown(data: Array<[number, number]>): Promise<void> {
    // Only scale down when there is more than 1 cluster to avoid total shutdown
    if (this._manager.clusters.size <= 1) return;

    // Calculate per-cluster total guild count
    const clusterLoad = new Map<number, number>();
    for (const cluster of this._manager.clusters.values()) {
      const total = cluster.shardList.reduce((sum, sid) => {
        const entry = data.find(([s]) => s === sid);
        return sum + (entry ? entry[1] : 0);
      }, 0);
      const avgPerShard = cluster.shardList.length > 0 ? total / cluster.shardList.length : 0;
      clusterLoad.set(cluster.id, avgPerShard);
    }

    // Overall average guilds/shard across all clusters
    const overallAvg =
      data.reduce((sum, [, g]) => sum + g, 0) / Math.max(data.length, 1);

    if (overallAvg >= this._floor) return; // load still healthy, no removal needed

    // Pick the cluster with the lowest average guilds/shard
    let emptiest: { id: number; avg: number } | null = null;
    for (const [id, avg] of clusterLoad) {
      if (!emptiest || avg < emptiest.avg) emptiest = { id, avg };
    }

    if (!emptiest) return;

    this._scaling = true;
    try {
      const removedCluster = this._manager.clusters.get(emptiest.id);
      const shardLabel = removedCluster
        ? `[${removedCluster.shardList.join(', ')}]`
        : '?';

      if (this._manager.options.logging) {
        console.log(
          `\x1b[33m[devcodes-sharding / AutoScaler]\x1b[0m ` +
          `\x1b[1mSCALE DOWN\x1b[0m — ` +
          `Overall avg ${overallAvg.toFixed(0)} guilds/shard is below floor (${this._floor}). ` +
          `Removing cluster #${emptiest.id} (shards ${shardLabel}, avg ${emptiest.avg.toFixed(0)} guilds/shard)…`,
        );
      }

      this._log.warn(
        `Avg load ${overallAvg.toFixed(0)} < floor ${this._floor} — removing cluster #${emptiest.id}`,
      );

      await this._manager.removeCluster(emptiest.id, {
        graceful: true,
        reason: 'AutoScaler scale-down',
      });

      if (this._manager.options.logging) {
        console.log(
          `\x1b[32m[devcodes-sharding / AutoScaler]\x1b[0m ` +
          `\x1b[1mSCALE DOWN COMPLETE\x1b[0m — ` +
          `Cluster #${emptiest.id} removed. ` +
          `Total clusters: ${this._manager.clusters.size}.`,
        );
      }

      this._manager.emit('scaleDown', emptiest.id);
    } catch (err) {
      this._log.error('Scale-down failed', err);
      if (this._manager.options.logging) {
        console.error(
          `\x1b[31m[devcodes-sharding / AutoScaler]\x1b[0m Scale-down failed:`,
          (err as Error).message,
        );
      }
    } finally {
      this._scaling = false;
    }
  }

  // ─── Data collection ───────────────────────────────────────────────────────

  private async _collectGuildData(): Promise<Array<[number, number]>> {
    if (this._rawOpts.collectData) {
      return this._rawOpts.collectData(this._manager);
    }

    try {
      const perCluster = await this._manager.broadcastEval<Array<[number, number]>>(
        (client: unknown) => {
          const c = client as {
            ws?: { shards?: Map<number, unknown> };
            guilds?: { cache?: Map<string, { shardId: number }> };
          };
          if (!c.guilds?.cache) return [];
          const map = new Map<number, number>();
          for (const guild of c.guilds.cache.values()) {
            const sid = (guild as unknown as { shardId: number }).shardId;
            map.set(sid, (map.get(sid) ?? 0) + 1);
          }
          return [...map.entries()];
        },
        { timeout: 10_000 },
      );

      return perCluster.flat();
    } catch {
      return [];
    }
  }
}
