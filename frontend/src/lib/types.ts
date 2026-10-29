export type VClock = Record<string, number>;

export type DeviceKind = 'PHONE' | 'TABLET' | 'TV' | 'WEB';

export interface UpdateRequest {
  userId: string;
  contentId: string;
  deviceId: string;
  positionSeconds: number;
  vectorClock?: VClock;
  clientTimestamp?: number;
  requestId?: string;
  force?: boolean;
  contentDurationSeconds?: number;
}

export interface UpdateResponse {
  decision: string;
  reason: string;
  comparison: string | null;
  debounceDecision: string;
  debounceReason: string;
  persisted: boolean;
  idempotent: boolean;
  eventEmitted: boolean;
  authoritativePosition: number;
  authoritativeVectorClock: VClock;
  revision: number;
  requestId: string | null;
  processingMs: number;
}

export interface Metrics {
  totalPings: number;
  debounced: number;
  persistCandidates: number;
  accepted: number;
  rejectedStale: number;
  concurrentConflicts: number;
  idempotentHits: number;
  cassandraWrites: number;
  conflictsLogged: number;
  kafkaEmitted: number;
  kafkaFailed: number;
  writeReductionPercentage: number;
  pingsPerSecond: number;
  writesPerSecond: number;
  eventsPerSecond: number;
  latencyMs: { count: number; avg: number; p50: number; p95: number; p99: number; max: number };
}

export interface SystemStatus {
  status: string;
  components: Record<string, boolean | string>;
}

export interface LiveEvent {
  seq: number;
  timestamp: number;
  category: string;
  userId: string | null;
  contentId: string | null;
  deviceId: string | null;
  decision: string | null;
  comparison: string | null;
  positionSeconds: number | null;
  message: string;
}

export interface ConflictRow {
  conflictId: string;
  resolvedAt: string;
  userId: string;
  contentId: string;
  incomingDeviceId: string;
  incomingVectorClock: VClock;
  storedVectorClock: VClock;
  comparison: string;
  decision: string;
  reason: string;
  incomingPosition: number;
  authoritativePosition: number;
  requestId: string | null;
}

export interface ConsumedEvent {
  seq: number;
  eventId: string;
  eventType: string;
  userId: string;
  contentId: string;
  deviceId: string;
  positionSeconds: number;
  vectorClock: VClock;
  decision: string;
  cdcSource: string;
  occurredAt: string;
  receivedAtEpochMs: number;
  propagationLatencyMs: number;
}

export interface PlayheadStateDto {
  userId: string;
  contentId: string;
  deviceId: string;
  positionSeconds: number;
  vectorClock: VClock;
  status: string;
  revision: number;
  lastRequestId: string | null;
  lastUpdatedAt: string;
}

export interface DemoInfo {
  userId: string;
  contentId: string;
  contentDurationSeconds: number;
  devices: { deviceId: string; type: DeviceKind }[];
}

export interface ComparisonSnapshot {
  incoming: VClock;
  stored: VClock;
  comparison: string;
  decision: string;
  incomingPosition: number;
  authoritativePosition: number;
  deviceId: string;
  at: number;
}
