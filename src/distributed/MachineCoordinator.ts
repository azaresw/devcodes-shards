/**
 * MachineCoordinator
 *
 * Runs on ONE machine (or a dedicated node) and acts as the central hub for a
 * multi-machine deployment.  Every other machine connects a MachineClient to
 * this coordinator so the whole fleet shares a consistent view of which cluster
 * lives where — and is notified instantly whenever any machine adds or removes
 * a cluster.
 *
 * Protocol: newline-delimited JSON over plain TCP.
 */
import * as net from 'net';
import { EventEmitter } from 'events';

// ─── Public types ─────────────────────────────────────────────────────────────

export interface MachineCoordinatorOptions {
  /** TCP port to listen on. */
  port: number;
  /** Bind address. Default: '0.0.0.0' (all interfaces). */
  host?: string;
  /**
   * Optional shared secret.  Every MachineClient must supply the same value in
   * its `password` option or the connection is rejected.
   */
  password?: string;
}

export interface MachineEntry {
  machineId: string;
  clusters: Array<{ id: number; shardList: number[] }>;
}

// ─── Internal ─────────────────────────────────────────────────────────────────

interface ConnectedMachine extends MachineEntry {
  socket: net.Socket;
}

// ─── Class ────────────────────────────────────────────────────────────────────

export declare interface MachineCoordinator {
  on(event: 'machineRegistered', listener: (machineId: string) => void): this;
  on(event: 'machineDisconnected', listener: (machineId: string) => void): this;
  on(event: 'clusterAdded', listener: (machineId: string, clusterId: number, shardList: number[]) => void): this;
  on(event: 'clusterRemoved', listener: (machineId: string, clusterId: number) => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
}

export class MachineCoordinator extends EventEmitter {
  private readonly _server: net.Server;
  private readonly _options: MachineCoordinatorOptions;
  private readonly _machines = new Map<string, ConnectedMachine>();

  constructor(options: MachineCoordinatorOptions) {
    super();
    this._options = options;
    this._server = net.createServer(socket => this._handleConnection(socket));
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /** Start listening. Resolves when the port is bound. */
  listen(): Promise<void> {
    return new Promise((resolve, reject) => {
      this._server.once('error', reject);
      this._server.listen(this._options.port, this._options.host ?? '0.0.0.0', () => {
        this._server.removeListener('error', reject);
        resolve();
      });
    });
  }

  /** Returns a snapshot of every registered machine and their clusters. */
  getMachines(): MachineEntry[] {
    return Array.from(this._machines.values()).map(m => ({
      machineId: m.machineId,
      clusters: m.clusters.map(c => ({ id: c.id, shardList: [...c.shardList] })),
    }));
  }

  /** Stop the coordinator and close all connections. */
  close(): Promise<void> {
    for (const m of this._machines.values()) {
      try { m.socket.destroy(); } catch { /* ignore */ }
    }
    this._machines.clear();
    return new Promise(resolve => this._server.close(() => resolve()));
  }

  // ─── Connection handling ─────────────────────────────────────────────────────

  private _handleConnection(socket: net.Socket): void {
    let buf = '';
    let registeredId: string | null = null;

    socket.setKeepAlive(true, 15_000);
    socket.on('error', () => socket.destroy());

    socket.on('data', chunk => {
      buf += chunk.toString('utf8');
      const lines = buf.split('\n');
      buf = lines.pop() ?? '';
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        try {
          const msg = JSON.parse(trimmed) as Record<string, unknown>;
          this._handleMessage(socket, msg, id => { registeredId = id; });
        } catch { /* malformed — ignore */ }
      }
    });

    socket.on('close', () => {
      if (!registeredId) return;
      if (this._machines.has(registeredId)) {
        this._machines.delete(registeredId);
        this._broadcast({ type: 'MACHINE_DISCONNECTED', machineId: registeredId }, registeredId);
        this.emit('machineDisconnected', registeredId);
      }
    });
  }

  private _handleMessage(
    socket: net.Socket,
    msg: Record<string, unknown>,
    setId: (id: string) => void,
  ): void {
    switch (msg.type) {
      case 'REGISTER': {
        // Auth check
        if (this._options.password && msg.password !== this._options.password) {
          this._write(socket, { type: 'AUTH_FAILED' });
          socket.destroy();
          return;
        }
        const machineId = String(msg.machineId ?? 'unknown');
        const clusters = (msg.clusters as Array<{ id: number; shardList: number[] }> | undefined) ?? [];

        const machine: ConnectedMachine = { machineId, socket, clusters };
        this._machines.set(machineId, machine);
        setId(machineId);

        // Send the new machine a full picture of everyone already connected
        const snapshot: MachineEntry[] = Array.from(this._machines.values())
          .filter(m => m.machineId !== machineId)
          .map(m => ({ machineId: m.machineId, clusters: m.clusters }));
        this._write(socket, { type: 'SYNC', machines: snapshot });

        // Notify everyone else this machine joined
        this._broadcast({ type: 'MACHINE_REGISTERED', machineId, clusters }, machineId);
        this.emit('machineRegistered', machineId);
        break;
      }

      case 'CLUSTER_ADDED': {
        const machineId = String(msg.machineId);
        const clusterId = Number(msg.clusterId);
        const shardList = (msg.shardList as number[]) ?? [];

        const machine = this._machines.get(machineId);
        if (machine) {
          // Update internal state
          machine.clusters = machine.clusters.filter(c => c.id !== clusterId);
          machine.clusters.push({ id: clusterId, shardList });
        }

        this._broadcast({ type: 'CLUSTER_ADDED', machineId, clusterId, shardList }, machineId);
        this.emit('clusterAdded', machineId, clusterId, shardList);
        break;
      }

      case 'CLUSTER_REMOVED': {
        const machineId = String(msg.machineId);
        const clusterId = Number(msg.clusterId);

        const machine = this._machines.get(machineId);
        if (machine) {
          machine.clusters = machine.clusters.filter(c => c.id !== clusterId);
        }

        this._broadcast({ type: 'CLUSTER_REMOVED', machineId, clusterId }, machineId);
        this.emit('clusterRemoved', machineId, clusterId);
        break;
      }

      case 'PING': {
        this._write(socket, { type: 'PONG' });
        break;
      }

      default:
        break;
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────────

  private _broadcast(msg: object, excludeId?: string): void {
    const line = JSON.stringify(msg) + '\n';
    for (const [id, machine] of this._machines) {
      if (id === excludeId) continue;
      try { machine.socket.write(line); } catch { /* ignore */ }
    }
  }

  private _write(socket: net.Socket, msg: object): void {
    try { socket.write(JSON.stringify(msg) + '\n'); } catch { /* ignore */ }
  }
}
