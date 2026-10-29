import type { VClock } from './types';

// Client-side mirror of the backend vector-clock algebra, so the device
// simulators can maintain faithful per-device clocks and the visualizer can
// explain comparisons locally.

export type Comparison = 'BEFORE' | 'AFTER' | 'EQUAL' | 'CONCURRENT';

export function increment(clock: VClock, deviceId: string): VClock {
  const next = { ...clock };
  next[deviceId] = (next[deviceId] ?? 0) + 1;
  return next;
}

export function merge(a: VClock, b: VClock): VClock {
  const out: VClock = { ...a };
  for (const k of Object.keys(b)) {
    out[k] = Math.max(out[k] ?? 0, b[k] ?? 0);
  }
  return out;
}

export function compare(a: VClock, b: VClock): Comparison {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  let aLess = false;
  let aGreater = false;
  for (const k of keys) {
    const av = a[k] ?? 0;
    const bv = b[k] ?? 0;
    if (av < bv) aLess = true;
    else if (av > bv) aGreater = true;
  }
  if (!aLess && !aGreater) return 'EQUAL';
  if (aLess && !aGreater) return 'BEFORE';
  if (aGreater && !aLess) return 'AFTER';
  return 'CONCURRENT';
}

export function formatClock(clock: VClock): string {
  const keys = Object.keys(clock).sort();
  if (keys.length === 0) return '∅';
  return keys.map((k) => `${k}:${clock[k]}`).join('  ');
}
