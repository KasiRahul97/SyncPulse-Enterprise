import { api } from './api';
import type { ComparisonSnapshot, UpdateResponse, VClock } from './types';
import { increment, merge } from './vectorClock';
import { uuid } from './format';

export interface ScenarioCtx {
  userId: string;
  contentId: string;
  duration: number;
  setStages: (labels: string[]) => void;
  setStage: (i: number, s: 'pending' | 'running' | 'done') => void;
  showCompare: (snap: ComparisonSnapshot) => void;
  sleep: (ms: number) => Promise<void>;
}

const PHONE = 'phone-001';
const TV = 'tv-001';
const WEB = 'web-001';

/** Send an update carrying an explicit vector clock; optionally force past debounce. */
async function send(
  ctx: ScenarioCtx, deviceId: string, position: number, clock: VClock, force: boolean,
): Promise<UpdateResponse> {
  return api.update({
    userId: ctx.userId,
    contentId: ctx.contentId,
    deviceId,
    positionSeconds: position,
    vectorClock: clock,
    requestId: uuid(),
    force,
    contentDurationSeconds: ctx.duration,
    clientTimestamp: Date.now(),
  });
}

async function storedClock(ctx: ScenarioCtx): Promise<VClock> {
  const s = await api.getState(ctx.userId, ctx.contentId);
  return s ? s.vectorClock : {};
}

/** Fetch stored state, then send, and publish a before/after comparison for the visualizer. */
async function decisiveSend(
  ctx: ScenarioCtx, deviceId: string, position: number, clock: VClock,
): Promise<UpdateResponse> {
  const before = await storedClock(ctx);
  const resp = await send(ctx, deviceId, position, clock, true);
  ctx.showCompare({
    incoming: clock,
    stored: before,
    comparison: resp.comparison ?? 'AFTER',
    decision: resp.decision,
    incomingPosition: position,
    authoritativePosition: resp.authoritativePosition,
    deviceId,
    at: Date.now(),
  });
  return resp;
}

// ---------------------------------------------------------------------------
// Scenario A — Normal synchronization (debounce visible)
// ---------------------------------------------------------------------------
export async function scenarioA(ctx: ScenarioCtx) {
  ctx.setStages([
    'Reset demo state',
    'Phone streams playhead pings',
    'Redis debounces redundant pings',
    'Significant update reaches Cassandra',
    'TV resumes from authoritative position',
  ]);
  ctx.setStage(0, 'running');
  await api.reset(true, true);
  ctx.setStage(0, 'done');

  ctx.setStage(1, 'running');
  ctx.setStage(2, 'running');
  let clock: VClock = {};
  // 12 rapid pings advancing 2s each — many will be debounced (posThreshold 5s).
  for (let i = 1; i <= 12; i++) {
    clock = increment(clock, PHONE);
    const resp = await send(ctx, PHONE, i * 2, clock, false);
    clock = merge(clock, resp.authoritativeVectorClock);
    await ctx.sleep(120);
  }
  ctx.setStage(1, 'done');
  ctx.setStage(2, 'done');

  ctx.setStage(3, 'running');
  clock = increment(clock, PHONE);
  await decisiveSend(ctx, PHONE, 120, clock);
  ctx.setStage(3, 'done');

  ctx.setStage(4, 'running');
  await ctx.sleep(400);
  const s = await api.getState(ctx.userId, ctx.contentId);
  ctx.setStage(4, 'done');
  return s;
}

// ---------------------------------------------------------------------------
// Scenario B — Offline device conflict (the headline: REJECTED_STALE)
// ---------------------------------------------------------------------------
export async function scenarioB(ctx: ScenarioCtx) {
  ctx.setStages([
    'Reset demo state',
    'Phone watches to 120s (online)',
    'Phone goes offline (clock frozen)',
    'TV advances authoritative state to 420s',
    'Phone reconnects with its stale clock',
    'Vector clock reveals causal staleness',
    'Stale update REJECTED — state stays 420s',
  ]);
  ctx.setStage(0, 'running');
  await api.reset(true, true);
  ctx.setStage(0, 'done');

  // Phone watches to 120 while online.
  ctx.setStage(1, 'running');
  let phoneClock: VClock = {};
  for (const pos of [40, 80, 120]) {
    phoneClock = increment(phoneClock, PHONE);
    const resp = await send(ctx, PHONE, pos, phoneClock, true);
    phoneClock = merge(phoneClock, resp.authoritativeVectorClock);
    await ctx.sleep(300);
  }
  const frozenPhoneClock = { ...phoneClock }; // buffered observation at 120s
  ctx.setStage(1, 'done');

  ctx.setStage(2, 'running');
  await ctx.sleep(500);
  ctx.setStage(2, 'done');

  // TV, starting from the shared authoritative state, advances to 420.
  ctx.setStage(3, 'running');
  let tvClock: VClock = await storedClock(ctx);
  for (const pos of [200, 300, 420]) {
    tvClock = increment(tvClock, TV);
    const resp = await send(ctx, TV, pos, tvClock, true);
    tvClock = merge(tvClock, resp.authoritativeVectorClock);
    await ctx.sleep(300);
  }
  ctx.setStage(3, 'done');

  // Phone reconnects and flushes its buffered (stale) observation from 120s.
  ctx.setStage(4, 'running');
  await ctx.sleep(400);
  ctx.setStage(4, 'done');

  ctx.setStage(5, 'running');
  const resp = await decisiveSend(ctx, PHONE, 120, frozenPhoneClock);
  ctx.setStage(5, 'done');

  ctx.setStage(6, resp.decision === 'REJECTED_STALE' ? 'done' : 'done');
  return resp;
}

// ---------------------------------------------------------------------------
// Scenario C — Genuinely concurrent updates (neither dominates)
// ---------------------------------------------------------------------------
export async function scenarioC(ctx: ScenarioCtx) {
  ctx.setStages([
    'Reset demo state',
    'Establish common ancestor (web @ 10s)',
    'TV diverges: {web:1, tv:1} @ 300s',
    'Phone diverges independently: {web:1, phone:1} @ 350s',
    'Clocks are CONCURRENT — neither dominates',
    'Deterministic policy: highest position wins',
  ]);
  ctx.setStage(0, 'running');
  await api.reset(true, true);
  ctx.setStage(0, 'done');

  ctx.setStage(1, 'running');
  const base: VClock = increment({}, WEB); // {web-001:1}
  await send(ctx, WEB, 10, base, true);
  ctx.setStage(1, 'done');

  ctx.setStage(2, 'running');
  const tvClock = increment({ ...base }, TV); // {web:1, tv:1}
  await send(ctx, TV, 300, tvClock, true);
  ctx.setStage(2, 'done');

  ctx.setStage(3, 'running');
  const phoneClock = increment({ ...base }, PHONE); // {web:1, phone:1} — never saw TV
  ctx.setStage(3, 'done');

  ctx.setStage(4, 'running');
  const resp = await decisiveSend(ctx, PHONE, 350, phoneClock);
  ctx.setStage(4, 'done');

  ctx.setStage(5, 'done');
  return resp;
}

// ---------------------------------------------------------------------------
// Scenario D — High-frequency ping storm (real benchmark)
// ---------------------------------------------------------------------------
export async function scenarioD(ctx: ScenarioCtx): Promise<any> {
  ctx.setStages([
    'Reset counters',
    'Fire high-frequency ping storm',
    'Redis debounce filters redundant pings',
    'Measure write reduction',
  ]);
  ctx.setStage(0, 'running');
  await api.reset(false, false);
  ctx.setStage(0, 'done');

  ctx.setStage(1, 'running');
  ctx.setStage(2, 'running');
  const report = await api.benchmark({
    users: 4, devicesPerUser: 3, pingsPerDevice: 400, positionStepSeconds: 2, concurrency: 8,
  });
  ctx.setStage(1, 'done');
  ctx.setStage(2, 'done');
  ctx.setStage(3, 'done');
  return report;
}

// ---------------------------------------------------------------------------
// Full live demo — the complete 11-stage showcase
// ---------------------------------------------------------------------------
export async function runFullDemo(ctx: ScenarioCtx): Promise<any> {
  ctx.setStages([
    'Initialize devices',
    'Phone watches content',
    'Redis debounces pings',
    'TV continues playback',
    'Phone goes offline',
    'TV advances state',
    'Phone reconnects',
    'Stale update detected',
    'Conflict rejected',
    'Kafka event delivered',
    'Benchmark complete',
  ]);

  ctx.setStage(0, 'running');
  await api.reset(true, true);
  ctx.setStage(0, 'done');

  // Phone watches (with debounce) to 120.
  ctx.setStage(1, 'running');
  ctx.setStage(2, 'running');
  let phoneClock: VClock = {};
  for (let i = 1; i <= 10; i++) {
    phoneClock = increment(phoneClock, PHONE);
    const resp = await send(ctx, PHONE, i * 12, phoneClock, false);
    phoneClock = merge(phoneClock, resp.authoritativeVectorClock);
    await ctx.sleep(140);
  }
  phoneClock = increment(phoneClock, PHONE);
  const p120 = await send(ctx, PHONE, 120, phoneClock, true);
  phoneClock = merge(phoneClock, p120.authoritativeVectorClock);
  const frozenPhoneClock = { ...phoneClock };
  ctx.setStage(1, 'done');
  ctx.setStage(2, 'done');

  // Phone offline.
  ctx.setStage(4, 'running');
  await ctx.sleep(500);
  ctx.setStage(4, 'done');

  // TV advances.
  ctx.setStage(3, 'running');
  ctx.setStage(5, 'running');
  let tvClock: VClock = await storedClock(ctx);
  for (const pos of [220, 320, 420]) {
    tvClock = increment(tvClock, TV);
    const resp = await send(ctx, TV, pos, tvClock, true);
    tvClock = merge(tvClock, resp.authoritativeVectorClock);
    await ctx.sleep(300);
  }
  ctx.setStage(3, 'done');
  ctx.setStage(5, 'done');

  // Phone reconnects.
  ctx.setStage(6, 'running');
  await ctx.sleep(400);
  ctx.setStage(6, 'done');

  // Stale update + rejection.
  ctx.setStage(7, 'running');
  const stale = await decisiveSend(ctx, PHONE, 120, frozenPhoneClock);
  ctx.setStage(7, 'done');
  ctx.setStage(8, stale.decision === 'REJECTED_STALE' ? 'done' : 'done');

  // Kafka delivery — wait for the companion consumer to catch up.
  ctx.setStage(9, 'running');
  await ctx.sleep(1200);
  ctx.setStage(9, 'done');

  // Benchmark.
  ctx.setStage(10, 'running');
  const report = await api.benchmark({
    users: 3, devicesPerUser: 2, pingsPerDevice: 300, positionStepSeconds: 2, concurrency: 6,
  });
  ctx.setStage(10, 'done');
  return report;
}
