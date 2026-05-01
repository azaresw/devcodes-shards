import { EventEmitter } from 'events';
import { sleep } from '../util/Util';
import { Logger, LogLevel } from '../util/Logger';

export interface QueueEntry {
  id: number;
  run: () => Promise<void>;
}

/**
 * Sequential spawn queue for clusters.
 *
 * Ensures clusters are brought online one at a time (or in controlled batches)
 * to avoid hammering the Discord gateway with IDENTIFY payloads simultaneously.
 *
 * Usage:
 *   queue.push({ id: 0, run: () => cluster.spawn() });
 *   await queue.start(); // resolves when all items finish
 *
 * Manual control (when options.queue.auto = false):
 *   queue.push(...);
 *   queue.next(); // spawn one at a time from your bot code
 */
export class ClusterQueue extends EventEmitter {
  private _queue: QueueEntry[] = [];
  private _running = false;
  private _paused = false;
  private readonly _log: Logger;

  /** Delay between consecutive spawns (ms). */
  delay: number;
  /** Per-cluster ready timeout (ms). */
  timeout: number;

  constructor(delay = 7_000, timeout = 30_000, loggingEnabled = true) {
    super();
    this.delay = delay;
    this.timeout = timeout;
    this._log = new Logger('Queue', LogLevel.INFO, loggingEnabled);
  }

  /** Add an entry to the back of the queue. */
  push(entry: QueueEntry): void {
    this._queue.push(entry);
  }

  /** Return queue length. */
  get size(): number {
    return this._queue.length;
  }

  get isRunning(): boolean {
    return this._running;
  }

  /**
   * Process the entire queue sequentially.
   * Resolves when all entries have completed.
   */
  async start(): Promise<void> {
    this._running = true;
    while (this._queue.length > 0) {
      // Pause support: wait until resumed
      while (this._paused) {
        await sleep(250);
      }

      const entry = this._queue.shift();
      if (!entry) break;

      this._log.debug(`Processing entry id=${entry.id}`);
      this.emit('next', entry);

      await entry.run();

      if (this._queue.length > 0) {
        await sleep(this.delay);
      }
    }
    this._running = false;
    this.emit('done');
  }

  /**
   * Manually process the next entry in the queue.
   * Used when `auto: false`.
   */
  async next(): Promise<void> {
    if (this._paused) {
      this._log.warn('Queue is paused — call queue.resume() first.');
      return;
    }
    const entry = this._queue.shift();
    if (!entry) return;
    this.emit('next', entry);
    await entry.run();
  }

  /** Pause automatic queue processing after the current entry finishes. */
  stop(): void {
    this._paused = true;
    this._log.info('Queue paused.');
    this.emit('stop');
  }

  /** Resume a paused queue. */
  resume(): void {
    this._paused = false;
    this._log.info('Queue resumed.');
    this.emit('resume');
  }

  /** Clear all pending (not yet started) entries. */
  clear(): void {
    this._queue = [];
  }
}
