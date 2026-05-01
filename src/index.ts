// ─── Manager ─────────────────────────────────────────────────────────────────
export { ShardingManager } from './manager/ShardingManager';
export { Cluster } from './manager/Cluster';
export { ClusterQueue } from './manager/ClusterQueue';

// ─── Client (bot process) ─────────────────────────────────────────────────────
export { ClusterClient, getInfo } from './client/ClusterClient';

// ─── Plugins ─────────────────────────────────────────────────────────────────
export { HeartbeatManager } from './plugins/HeartbeatManager';
export type { HeartbeatManagerOptions } from './plugins/HeartbeatManager';

export { AutoScaler } from './plugins/AutoScaler';
export type { AutoScalerOptions } from './plugins/AutoScaler';

// ─── IPC ─────────────────────────────────────────────────────────────────────
export { IPCMessage } from './ipc/IPCMessage';

// ─── Types ───────────────────────────────────────────────────────────────────
export {
  ClusterStatus,
  MessageType,
} from './types';

export type {
  ShardingManagerOptions,
  ClusterSpawnOptions,
  ClusterInfo,
  ClusterStats,
  EvalOptions,
  AddClusterOptions,
  RemoveClusterOptions,
  RespawnAllOptions,
  SpawnOptions,
  HeartbeatOptions,
  RestartOptions,
  QueueOptions,
  IPlugin,
  RawIPCMessage,
  PendingRequest,
  PendingBroadcast,
} from './types';

// ─── Distributed (multi-machine) ─────────────────────────────────────────────
export { MachineCoordinator } from './distributed/MachineCoordinator';
export type { MachineCoordinatorOptions, MachineEntry } from './distributed/MachineCoordinator';

export { MachineClient } from './distributed/MachineClient';
export type { MachineClientOptions, RemoteClusterEntry, GlobalMachineEntry } from './distributed/MachineClient';

// ─── Utils (re-exported for advanced users) ───────────────────────────────────
export { calcShardId, fetchGatewayShards, generateNonce } from './util/Util';
