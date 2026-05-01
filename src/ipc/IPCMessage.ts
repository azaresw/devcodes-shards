import { MessageType, RawIPCMessage } from '../types';

/**
 * Wraps a raw IPC payload with type-safe accessors.
 * Both the manager and clusters pass these around internally.
 */
export class IPCMessage {
  readonly _type: MessageType;
  readonly _nonce: string | undefined;
  readonly _clusterId: number | undefined;
  readonly [key: string]: unknown;

  constructor(data: RawIPCMessage) {
    // Copy all properties (including arbitrary payload keys)
    for (const [k, v] of Object.entries(data)) {
      (this as Record<string, unknown>)[k] = v;
    }
    this._type = data._type;
    this._nonce = data._nonce;
    this._clusterId = data._clusterId;
  }

  get isCustomRequest(): boolean {
    return this._type === MessageType.CUSTOM_REQUEST;
  }

  get isCustomReply(): boolean {
    return this._type === MessageType.CUSTOM_REPLY;
  }

  get isCustomMessage(): boolean {
    return this._type === MessageType.CUSTOM_MESSAGE;
  }

  get isHeartbeat(): boolean {
    return this._type === MessageType.HEARTBEAT;
  }

  /** Convenience: build a reply payload for this request. */
  buildReply(payload: Record<string, unknown>): RawIPCMessage {
    return {
      ...payload,
      _type: MessageType.CUSTOM_REPLY,
      _nonce: this._nonce,
      _clusterId: this._clusterId,
    };
  }

  toJSON(): RawIPCMessage {
    return { ...this } as unknown as RawIPCMessage;
  }
}
