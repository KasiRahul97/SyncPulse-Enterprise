import type {
  UpdateRequest, UpdateResponse, Metrics, SystemStatus, LiveEvent,
  ConflictRow, ConsumedEvent, PlayheadStateDto, DemoInfo,
} from './types';

async function jsonFetch<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    let detail = '';
    try {
      detail = JSON.stringify(await res.json());
    } catch {
      detail = res.statusText;
    }
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  update: (body: UpdateRequest) =>
    jsonFetch<UpdateResponse>('/api/v1/playhead/update', {
      method: 'POST',
      body: JSON.stringify(body),
    }),

  getState: (userId: string, contentId: string) =>
    fetch(`/api/v1/playhead/${encodeURIComponent(userId)}/${encodeURIComponent(contentId)}`).then(
      (r) => (r.status === 404 ? null : (r.json() as Promise<PlayheadStateDto>)),
    ),

  metrics: () => jsonFetch<Metrics>('/api/v1/metrics'),

  status: () => jsonFetch<SystemStatus>('/api/v1/status'),

  events: (since: number) =>
    jsonFetch<{ cursor: number; events: LiveEvent[] }>(`/api/v1/events?since=${since}`),

  conflicts: (userId: string, limit = 50) =>
    jsonFetch<ConflictRow[]>(`/api/v1/conflicts/${encodeURIComponent(userId)}?limit=${limit}`),

  registerDevice: (userId: string, deviceId: string, deviceType: string) =>
    jsonFetch(`/api/v1/devices/register`, {
      method: 'POST',
      body: JSON.stringify({ userId, deviceId, deviceType }),
    }),

  demoInfo: () => jsonFetch<DemoInfo>('/api/v1/admin/demo-info'),

  reset: (clearState = true, reseed = true) =>
    jsonFetch(`/api/v1/admin/reset?clearState=${clearState}&reseed=${reseed}`, { method: 'POST' }),

  benchmark: (body: Record<string, number>) =>
    jsonFetch<any>('/api/v1/benchmark/run', { method: 'POST', body: JSON.stringify(body) }),

  consumerEvents: (since: number) =>
    jsonFetch<{ cursor: number; events: ConsumedEvent[] }>(`/consumer/events?since=${since}`),

  consumerMetrics: () =>
    jsonFetch<{ totalConsumed: number; propagationLatencyMs: any }>('/consumer/metrics'),
};
