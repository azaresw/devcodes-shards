/* eslint-disable no-console */

export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

const COLORS = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
  gray: '\x1b[90m',
} as const;

export class Logger {
  /**
   * @param tag         Label shown in every log line, e.g. `ShardingManager`.
   * @param minLevel    Minimum level to print. Default: INFO.
   * @param enabled     Master on/off switch. When `false` nothing is printed. Default: `true`.
   */
  constructor(
    private readonly tag: string,
    private readonly minLevel: LogLevel = LogLevel.INFO,
    private readonly enabled: boolean = true,
  ) {}

  private _format(level: string, color: string, msg: string): string {
    const time = new Date().toISOString().slice(11, 23);
    return `${COLORS.gray}[${time}]${COLORS.reset} ${color}${COLORS.bold}[${this.tag}/${level}]${COLORS.reset} ${msg}`;
  }

  debug(msg: string): void {
    if (this.enabled && this.minLevel <= LogLevel.DEBUG) {
      console.debug(this._format('DEBUG', COLORS.cyan, msg));
    }
  }

  info(msg: string): void {
    if (this.enabled && this.minLevel <= LogLevel.INFO) {
      console.info(this._format('INFO', COLORS.green, msg));
    }
  }

  warn(msg: string): void {
    if (this.enabled && this.minLevel <= LogLevel.WARN) {
      console.warn(this._format('WARN', COLORS.yellow, msg));
    }
  }

  error(msg: string, err?: unknown): void {
    if (this.enabled && this.minLevel <= LogLevel.ERROR) {
      console.error(this._format('ERROR', COLORS.red, msg));
      if (err) console.error(err);
    }
  }
}
