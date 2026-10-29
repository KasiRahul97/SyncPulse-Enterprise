import type { DeviceKind } from './types';

export function formatClockTime(totalSeconds: number): string {
  const s = Math.max(0, Math.floor(totalSeconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const pad = (n: number) => n.toString().padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(sec)}` : `${pad(m)}:${pad(sec)}`;
}

export function deviceIcon(kind: DeviceKind): string {
  switch (kind) {
    case 'PHONE': return '📱';
    case 'TV': return '📺';
    case 'WEB': return '💻';
    case 'TABLET': return '📲';
    default: return '🖥️';
  }
}

export function hhmmss(epochMs: number): string {
  const d = new Date(epochMs);
  const pad = (n: number) => n.toString().padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

export function uuid(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) return crypto.randomUUID();
  return 'req-' + Math.random().toString(36).slice(2) + Date.now().toString(36);
}
