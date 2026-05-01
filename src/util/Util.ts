import { randomUUID } from 'crypto';
import * as https from 'https';
import * as os from 'os';

/** Generate a unique nonce string for request/reply correlation. */
export function generateNonce(): string {
  return randomUUID();
}

/** Split an array into chunks of at most `size` elements. */
export function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

/**
 * Discord's shard-ID formula.
 * shard_id = (guild_id >> 22) % total_shards
 */
export function calcShardId(guildId: string, totalShards: number): number {
  return Number(BigInt(guildId) >> 22n) % totalShards;
}

/** Returns the number of logical CPU cores (used for 'auto' cluster count). */
export function getCpuCount(): number {
  return os.cpus().length;
}

/**
 * Fetch Discord's recommended shard count for the given bot token.
 * Hits GET /api/v10/gateway/bot and returns `shards`.
 */
export async function fetchGatewayShards(token: string): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const req = https.request(
      {
        hostname: 'discord.com',
        path: '/api/v10/gateway/bot',
        method: 'GET',
        headers: {
          Authorization: `Bot ${token}`,
          'User-Agent': 'devcodes-sharding (https://github.com/devcodes/devcodes-sharding, 1.0.0)',
        },
      },
      (res) => {
        let raw = '';
        res.on('data', (chunk: Buffer) => (raw += chunk.toString()));
        res.on('end', () => {
          try {
            const body = JSON.parse(raw) as { shards?: number; message?: string };
            if (typeof body.shards !== 'number') {
              reject(new Error(`Discord gateway error: ${body.message ?? raw}`));
            } else {
              resolve(body.shards);
            }
          } catch (e) {
            reject(e);
          }
        });
      },
    );
    req.on('error', reject);
    req.end();
  });
}

/** Async sleep helper. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Clamp a number between min and max. */
export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}
