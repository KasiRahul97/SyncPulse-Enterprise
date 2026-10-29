import type { DeviceKind, VClock, UpdateResponse } from './types';

/** In-browser device simulator state. Each device maintains its own vector clock. */
export interface Sim {
  deviceId: string;
  kind: DeviceKind;
  position: number;
  clock: VClock;
  online: boolean;
  playing: boolean;
  updatesSent: number;
  lastDecision: string | null;
  lastResponseMs: number | null;
  flash: boolean;
}

export type SimAction =
  | { type: 'PLAY' }
  | { type: 'PAUSE' }
  | { type: 'SEEK'; seconds: number }
  | { type: 'OFFLINE' }
  | { type: 'RECONNECT' }
  | { type: 'SEND'; force?: boolean }
  | { type: 'RESET' };

export function initialSims(devices: { deviceId: string; type: DeviceKind }[]): Sim[] {
  return devices.map((d) => ({
    deviceId: d.deviceId,
    kind: d.type,
    position: 0,
    clock: {},
    online: true,
    playing: false,
    updatesSent: 0,
    lastDecision: null,
    lastResponseMs: null,
    flash: false,
  }));
}
