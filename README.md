# devcodes-sharding

[![npm version](https://img.shields.io/npm/v/devcodes-sharding)](https://www.npmjs.com/package/devcodes-sharding)
[![npm downloads](https://img.shields.io/npm/dm/devcodes-sharding)](https://www.npmjs.com/package/devcodes-sharding)
[![GitHub](https://img.shields.io/github/license/azaresw/devcodes-shards)](https://github.com/azaresw/devcodes-shards/blob/main/LICENSE)

Advanced Discord.js sharding — **hot-add and hot-remove shards/clusters at runtime** with zero downtime. Your bot grows without ever rebooting.

Built on top of the ideas from `discord-hybrid-sharding` but with first-class support for live scaling, auto scale-down, per-shard guild targets, and a master on/off logging switch.

---

## Why devcodes-sharding?

| Feature | discord-hybrid-sharding | **devcodes-sharding** |
|---|---|---|
| Hot-add cluster (zero downtime) | ✗ needs full recluster | ✅ `manager.addCluster()` |
| Hot-remove single cluster | ✗ | ✅ `manager.removeCluster(id)` |
| Auto scale-DOWN (remove idle clusters) | ✗ | ✅ built-in |
| Multi-machine distributed sharding | ✗ | ✅ `MachineCoordinator` + `MachineClient` |
| Cross-machine cluster sync (add/remove) | ✗ | ✅ instant TCP broadcast to all machines |
| Guilds-per-shard threshold config | plugin only | ✅ top-level `guildsPerShard` option |
| `'auto'` guilds/shard mode | ✗ | ✅ uses Discord's recommended 2 000 ceiling |
| Master logging toggle | ✗ | ✅ `logging: false` silences everything |
| Respawn one cluster without touching others | ✅ | ✅ |
| Heartbeat monitoring | ✅ | ✅ |
| Zero external runtime dependencies | ✗ | ✅ |
| Full TypeScript + declaration maps | ✅ | ✅ |

---

## Installation

```bash
npm install devcodes-sharding
```

---

## Quick Start

### cluster.js (manager process)

```js
const { ShardingManager, HeartbeatManager, AutoScaler } = require('devcodes-sharding');

const manager = new ShardingManager('./bot.js', {
  totalShards: 'auto',      // fetch recommended count from Discord
  shardsPerCluster: 4,      // 4 shards per process
  token: process.env.TOKEN,

  // Optional: set a target — AutoScaler reads this automatically
  guildsPerShard: 1500,     // scale up at 1500 guilds/shard, scale down at 375 (ceiling / 4)
  // guildsPerShard: 'auto' // use Discord's recommended 2000 ceiling

  logging: true,            // set false to silence all internal logs
});

manager.extend(new HeartbeatManager({ interval: 20_000, maxMissedBeats: 3 }));
manager.extend(new AutoScaler());   // reads guildsPerShard from manager automatically

manager.on('clusterCreate', cluster => console.log(`Cluster ${cluster.id} spawned`));
manager.on('clusterReady',  cluster => console.log(`Cluster ${cluster.id} ready`));

manager.spawn();
```

### bot.js (inside every cluster)

```js
const { ClusterClient, getInfo } = require('devcodes-sharding');
const { Client } = require('discord.js');

const client = new Client({
  shards: getInfo().SHARD_LIST,
  shardCount: getInfo().TOTAL_SHARDS,
  intents: [/* your intents */],
});

client.cluster = new ClusterClient(client);

client.once('ready', () => {
  client.cluster.triggerReady(); // REQUIRED — tells the manager this cluster is up
});

client.login(process.env.TOKEN);
```

---

## Hot-Add a Cluster (Zero Downtime)

Add a new cluster **at any time** without touching or rebooting any existing cluster:

```js
// Auto-pick next unassigned shard IDs
const cluster = await manager.addCluster();
console.log(`Added cluster #${cluster.id} covering shards [${cluster.shardList.join(', ')}]`);

// Specify exact shards
const cluster = await manager.addCluster({ shards: [16, 17, 18, 19] });

// Control how many shards the new cluster takes
const cluster = await manager.addCluster({ shardsPerCluster: 2 });
```

---

## Hot-Remove a Cluster (Only That Cluster Dies)

Remove a single cluster gracefully — every other cluster keeps running:

```js
await manager.removeCluster(3);                       // graceful shutdown
await manager.removeCluster(3, { graceful: false });  // immediate kill
await manager.removeCluster(3, { reason: 'scaling down for night hours' });
```

---

## Auto-Scaling

The `AutoScaler` plugin automatically hot-adds and hot-removes clusters based on live guild counts.

### How the thresholds work

| Config | Ceiling (scale UP when any shard exceeds) | Floor (scale DOWN when avg drops below) |
|---|---|---|
| `guildsPerShard: 1500` | 1 500 guilds/shard | 375 guilds/shard (ceiling ÷ 4) |
| `guildsPerShard: 'auto'` | 2 000 guilds/shard | 500 guilds/shard |
| `guildsPerShard` not set + explicit plugin opts | plugin `maxGuildsPerShard` | plugin `minGuildsPerShard` |

### Console output (when logging is on)

```
[devcodes-sharding / AutoScaler] SCALE UP — 2 shard(s) over ceiling (1500 guilds/shard). Worst: shard-3 with 1823 guilds. Adding a new cluster…
[devcodes-sharding / AutoScaler] SCALE UP COMPLETE — Added cluster #4 covering shards [16, 17]. Total clusters: 5.

[devcodes-sharding / AutoScaler] SCALE DOWN — Overall avg 201 guilds/shard is below floor (375). Removing cluster #4 (shards [16, 17], avg 88 guilds/shard)…
[devcodes-sharding / AutoScaler] SCALE DOWN COMPLETE — Cluster #4 removed. Total clusters: 4.
```

### Plugin-level options (all optional)

```js
manager.extend(new AutoScaler({
  checkInterval: 60_000,       // how often to check load (ms)
  maxGuildsPerShard: 2000,     // override ceiling (plugin option wins over manager option)
  minGuildsPerShard: 400,      // override scale-down floor
  scaleDown: true,             // explicitly enable/disable scale-down
  shardsPerCluster: 2,         // shards per newly added cluster
  collectData: async (manager) => {
    // custom data collection — return Array<[shardId, guildCount]>
    return [[0, 1200], [1, 1800]];
  },
}));
```

---

## Multi-Machine Distributed Sharding

Split your shards across **multiple servers/VMs** while keeping every machine in sync. When any machine hot-adds or hot-removes a cluster, every other machine is notified instantly — no restarts, no manual coordination.

### Architecture

```
  Machine A (shards 0-7)          Machine B (shards 8-15)
  ┌─────────────────────┐         ┌─────────────────────┐
  │  ShardingManager    │         │  ShardingManager    │
  │  MachineClient ─────┼────┐ ┌──┼─ MachineClient      │
  └─────────────────────┘    │ │  └─────────────────────┘
                              ▼ ▼
                    ┌──────────────────┐
                    │ MachineCoordinator│  (any machine or dedicated node)
                    └──────────────────┘
```

### 1. Start the coordinator (once, on any machine or dedicated node)

```js
const { MachineCoordinator } = require('devcodes-sharding');

const coordinator = new MachineCoordinator({
  port: 4000,
  password: 'my-secret',  // optional but recommended
});

await coordinator.listen();
console.log('Coordinator ready on :4000');

coordinator.on('machineRegistered',  id => console.log(`Machine joined: ${id}`));
coordinator.on('machineDisconnected', id => console.log(`Machine left:   ${id}`));
coordinator.on('clusterAdded',   (machine, clusterId, shards) => console.log(`${machine} added cluster ${clusterId}`));
coordinator.on('clusterRemoved', (machine, clusterId)         => console.log(`${machine} removed cluster ${clusterId}`));
```

### 2. Connect each machine (run on every bot server)

```js
const { ShardingManager, MachineCoordinator, MachineClient } = require('devcodes-sharding');

const manager = new ShardingManager('./bot.js', {
  totalShards: 16,
  shardList: [0, 1, 2, 3, 4, 5, 6, 7],  // this machine's slice
  token: process.env.TOKEN,
});

await manager.spawn();

const machine = new MachineClient(manager, {
  machineId: 'machine-a',           // must be unique across the fleet
  coordinatorHost: '10.0.0.1',      // coordinator IP
  coordinatorPort: 4000,
  password: 'my-secret',
  reconnect: true,                  // auto-reconnect on disconnect (default: true)
  reconnectDelay: 5000,             // ms before each reconnect attempt (default: 5000)
});

await machine.connect();
```

### 3. Global registry — see every cluster on every machine

```js
// Returns an array of { machineId, clusters: [{ id, shardList }] } for ALL machines
const registry = machine.getGlobalRegistry();
console.log(registry);
// [
//   { machineId: 'machine-a', clusters: [{ id: 0, shardList: [0,1,2,3] }, ...] },
//   { machineId: 'machine-b', clusters: [{ id: 0, shardList: [8,9,10,11] }, ...] },
// ]
```

### 4. Cross-machine guild routing

```js
// Find which machine + cluster handles a given guild
const target = machine.findClusterForGuild('123456789012345678');
// { machineId: 'machine-b', clusterId: 0 }
```

### 5. Automatic sync on hot-add / hot-remove

Hot-add or hot-remove a cluster on **any** machine — every other machine is updated automatically:

```js
// On machine-a: add a cluster
const cluster = await manager.addCluster({ shards: [6, 7] });
// machine-b instantly receives: remoteClusterAdd event

// On machine-b: remove a cluster
await manager.removeCluster(1);
// machine-a instantly receives: remoteClusterRemove event
```

### MachineClient Events

```js
machine.on('connect',             () => { })                                    // connected to coordinator
machine.on('disconnect',          () => { })                                    // lost connection (will retry)
machine.on('sync',                registry => { })                              // initial fleet snapshot received
machine.on('machineJoin',         machineId => { })                             // another machine connected
machine.on('machineLeave',        machineId => { })                             // another machine disconnected
machine.on('remoteClusterAdd',    (machineId, clusterId, shardList) => { })     // remote hot-add
machine.on('remoteClusterRemove', (machineId, clusterId) => { })                // remote hot-remove
machine.on('error',               err => { })
```

### MachineCoordinator Options

```js
new MachineCoordinator({
  port: 4000,           // TCP port to listen on
  host: '0.0.0.0',      // bind address (default: all interfaces)
  password: 'secret',   // optional shared secret — clients must supply the same value
});
```

### MachineClient Options

```js
new MachineClient(manager, {
  machineId: 'machine-a',        // unique ID for this machine
  coordinatorHost: '10.0.0.1',   // coordinator address
  coordinatorPort: 4000,         // coordinator port
  password: 'secret',            // must match coordinator password (if set)
  reconnect: true,               // auto-reconnect on disconnect
  reconnectDelay: 5000,          // ms to wait before each reconnect attempt
});
```

---

## ShardingManager Options

```js
const manager = new ShardingManager('./bot.js', {
  totalShards: 'auto',         // number | 'auto'
  totalClusters: 'auto',       // number | 'auto' (uses CPU count)
  shardsPerCluster: 4,         // overrides totalClusters when set
  shardList: [],               // specific shard IDs (cross-host setups)
  mode: 'process',             // 'process' | 'worker'
  respawn: true,               // auto-respawn dead clusters
  token: process.env.TOKEN,    // required when totalShards: 'auto'
  guildsPerShard: 1500,        // number | 'auto' — target for AutoScaler
  logging: true,               // master on/off for all internal logging

  heartbeat: {
    interval: 30_000,          // ms between expected heartbeats
    maxMissedBeats: 3,         // missed beats before respawn
  },
  restarts: {
    max: 5,                    // max restarts per cluster
    interval: 3_600_000,       // rolling window for restart count (1 hour)
  },
  queue: {
    auto: true,                // auto-process spawn queue
    delay: 7_000,              // delay between cluster spawns (ms)
    timeout: 30_000,           // per-cluster ready timeout (ms)
  },
  env: { MY_VAR: 'value' },    // extra env vars injected into every cluster
  shardArgs: [],               // CLI args forwarded to bot.js
  execArgv: [],                // node execArgv (e.g. ['--max-old-space-size=4096'])
});
```

---

## BroadcastEval

Evaluate a function on every cluster's Discord client:

```js
// Manager-side
const counts = await manager.broadcastEval(client => client.guilds.cache.size);
const total = counts.reduce((a, b) => a + b, 0);
console.log(`${total} total guilds`);

// Bot-side
const pings = await client.cluster.broadcastEval(c => c.ws.ping);

// With context
const guild = await client.cluster.broadcastEval(
  (c, ctx) => c.guilds.cache.get(ctx.guildId),
  { context: { guildId: '1234567890' } }
);

// Target a single cluster
const result = await manager.broadcastEval(c => c.guilds.cache.size, { cluster: 2 });
```

---

## IPC — Manager ↔ Cluster Communication

```js
// Manager → Cluster: fire and forget
manager.clusters.get(0).send({ type: 'reload', module: 'commands' });

// Manager → Cluster: request/reply
const reply = await manager.clusters.get(0).request({ type: 'getStats' }, 5000);

// Cluster → Manager: request/reply
const result = await client.cluster.request({ action: 'getStats' });

// Cluster: listen for messages
client.cluster.on('message', msg => {
  if (msg.isCustomRequest) {
    // send a reply
    client.cluster.send(msg.buildReply({ data: 'here' }));
  }
});

// Eval on manager process from a cluster
const clusterCount = await client.cluster.evalOnManager(m => m.clusters.size);
```

---

## HeartbeatManager Plugin

Monitors every cluster for liveness. Respawns automatically if a cluster stops responding.

```js
const { HeartbeatManager } = require('devcodes-sharding');

manager.extend(new HeartbeatManager({
  interval: 30_000,     // expected heartbeat interval (ms)
  maxMissedBeats: 3,    // respawn after this many missed beats
}));
```

---

## ClusterClient API (in bot.js)

```js
client.cluster.id            // this cluster's ID
client.cluster.count         // total cluster count
client.cluster.info          // { CLUSTER_ID, CLUSTER_COUNT, SHARD_LIST, TOTAL_SHARDS, ... }
client.cluster.maintenance   // current maintenance message, or null

client.cluster.triggerReady()                 // signal manager that bot is ready (REQUIRED)
client.cluster.triggerMaintenance('updating') // enter maintenance mode
client.cluster.clearMaintenance()             // leave maintenance mode

client.cluster.send({ ... })                  // fire-and-forget to manager
client.cluster.request({ ... }, timeout)      // request → reply from manager
client.cluster.broadcastEval(fn, options)     // eval on all clusters
client.cluster.evalOnManager(fn)              // eval on manager process
client.cluster.fetchClientValues('ws.ping')   // shortcut for broadcastEval property fetch
client.cluster.spawnNextCluster()             // trigger next cluster in queue (manual mode)
```

---

## getInfo() — Shard Info in Bot Process

```js
const { getInfo } = require('devcodes-sharding');

const info = getInfo();
// {
//   CLUSTER_ID: 0,
//   CLUSTER_COUNT: 4,
//   SHARD_LIST: [0, 1, 2, 3],
//   TOTAL_SHARDS: 16,
//   FIRST_SHARD_ID: 0,
//   LAST_SHARD_ID: 3,
//   MAINTENANCE: null,
// }
```

---

## Manager Events

```js
manager.on('clusterCreate',  cluster => { })
manager.on('clusterReady',   cluster => { })
manager.on('clusterDeath',   (cluster, code, signal) => { })
manager.on('clusterRespawn', cluster => { })
manager.on('clusterAdd',     cluster => { })           // hot-add completed
manager.on('clusterRemove',  (id, reason) => { })      // hot-remove completed
manager.on('clusterMessage', (cluster, msg) => { })
manager.on('heartbeat',      cluster => { })
manager.on('debug',          msg => { })
manager.on('scaleUp',        cluster => { })           // AutoScaler added a cluster
manager.on('scaleDown',      clusterId => { })         // AutoScaler removed a cluster
manager.on('scaleUpRequired', ({ currentTotal, newTotalShards }) => { }) // manual action needed
```

---

## License

MIT — built by [Devcodes](https://github.com/azaresw)
