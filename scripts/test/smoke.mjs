#!/usr/bin/env node
// End-to-end smoke test: API -> Redis debounce -> conflict engine -> Cassandra
// -> CDC -> Kafka -> companion consumer. Exercises the whole pipeline with real
// assertions against a running stack. No dependencies (Node 18+ global fetch).
//
//   node scripts/test/smoke.mjs
//
// Env: BACKEND (default http://localhost:8080), CONSUMER (http://localhost:8081)

const BACKEND = process.env.BACKEND || 'http://localhost:8080';
const CONSUMER = process.env.CONSUMER || 'http://localhost:8081';

let pass = 0, fail = 0;
const ok = (c, m) => { if (c) { pass++; console.log(`  \x1b[32m✓\x1b[0m ${m}`); } else { fail++; console.log(`  \x1b[31m✗ ${m}\x1b[0m`); } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const uuid = () => (globalThis.crypto?.randomUUID?.() ?? 'req-' + Math.random().toString(36).slice(2));

async function j(url, opts) {
  const r = await fetch(url, { headers: { 'Content-Type': 'application/json' }, ...opts });
  if (!r.ok && r.status !== 404) throw new Error(`${r.status} ${url} ${await r.text()}`);
  return r.status === 404 ? null : r.json();
}
const post = (url, body) => j(url, { method: 'POST', body: body ? JSON.stringify(body) : undefined });
const update = (b) => post(`${BACKEND}/api/v1/playhead/update`, b);

// Unique user per run so streams are always fresh (no cross-run carryover in
// persistent Cassandra), plus a fresh content stream per scenario.
const USER = 'smoke-user-' + Date.now().toString(36);
let CONTENT = 'smoke-debounce';
const inc = (c, d) => ({ ...c, [d]: (c[d] ?? 0) + 1 });

async function waitForHealth() {
  for (let i = 0; i < 60; i++) {
    try { const h = await j(`${BACKEND}/api/v1/health`); if (h?.status === 'UP') return; } catch {}
    await sleep(2000);
  }
  throw new Error('backend never became healthy');
}

async function main() {
  console.log(`\n\x1b[1mSyncPulse smoke test\x1b[0m  backend=${BACKEND} consumer=${CONSUMER}\n`);
  await waitForHealth();
  await post(`${BACKEND}/api/v1/admin/reset?clearState=true&reseed=true`);

  // 1) Debounce / write reduction on a single stream.
  console.log('1. Redis debounce reduces durable writes');
  let clock = {};
  let persisted = 0;
  const PINGS = 30;
  for (let i = 1; i <= PINGS; i++) {
    clock = inc(clock, 'phone-x');
    const r = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-x',
      positionSeconds: i, vectorClock: clock, requestId: uuid() });
    clock = { ...clock, ...r.authoritativeVectorClock };
    if (r.persisted) persisted++;
  }
  ok(persisted < PINGS, `persisted ${persisted}/${PINGS} pings (debounce dropped ${PINGS - persisted})`);

  // 2) Idempotency.
  console.log('2. Idempotency on duplicate requestId');
  const rid = uuid();
  const first = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-x',
    positionSeconds: 500, vectorClock: inc(clock, 'phone-x'), requestId: rid, force: true });
  const dup = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-x',
    positionSeconds: 500, vectorClock: inc(clock, 'phone-x'), requestId: rid, force: true });
  ok(dup.idempotent === true || dup.decision === 'IDEMPOTENT', `duplicate requestId returned idempotent (first=${first.decision}, dup=${dup.decision})`);

  // 3) Stale offline device -> REJECTED_STALE (state must not regress).
  console.log('3. Stale offline update is rejected');
  CONTENT = 'smoke-stale';
  let phone = inc({}, 'phone-001');
  await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-001', positionSeconds: 120, vectorClock: phone, force: true });
  const frozen = { ...phone };
  // TV advances from the shared state.
  let stored = (await j(`${BACKEND}/api/v1/playhead/${USER}/${CONTENT}`)).vectorClock;
  let tv = { ...stored };
  for (const pos of [200, 300, 420]) { tv = inc(tv, 'tv-001'); const r = await update({ userId: USER, contentId: CONTENT, deviceId: 'tv-001', positionSeconds: pos, vectorClock: tv, force: true }); tv = { ...tv, ...r.authoritativeVectorClock }; }
  const stale = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-001', positionSeconds: 120, vectorClock: frozen, force: true });
  ok(stale.decision === 'REJECTED_STALE', `stale phone update decision=${stale.decision}`);
  ok(stale.authoritativePosition === 420, `authoritative position stayed 420 (got ${stale.authoritativePosition})`);
  const conflicts = await j(`${BACKEND}/api/v1/conflicts/${USER}`);
  ok(Array.isArray(conflicts) && conflicts.some((c) => c.decision === 'REJECTED_STALE'), 'conflict recorded in conflict_log');

  // 4) Concurrent updates -> CONCURRENT.
  console.log('4. Concurrent updates are detected and resolved');
  CONTENT = 'smoke-concurrent';
  const base = inc({}, 'web-001');
  await update({ userId: USER, contentId: CONTENT, deviceId: 'web-001', positionSeconds: 10, vectorClock: base, force: true });
  await update({ userId: USER, contentId: CONTENT, deviceId: 'tv-001', positionSeconds: 300, vectorClock: inc({ ...base }, 'tv-001'), force: true });
  const conc = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-001', positionSeconds: 350, vectorClock: inc({ ...base }, 'phone-001'), force: true });
  ok(conc.comparison === 'CONCURRENT', `comparison=${conc.comparison}`);
  ok(conc.decision.startsWith('CONCURRENT'), `decision=${conc.decision}`);
  ok(conc.authoritativePosition === 350, `highest-position-wins selected 350 (got ${conc.authoritativePosition})`);

  // 5) Kafka propagation to the companion consumer.
  console.log('5. Events propagate to the companion consumer');
  let consumed = 0;
  for (let i = 0; i < 10; i++) { const m = await j(`${CONSUMER}/consumer/metrics`).catch(() => null); consumed = m?.totalConsumed ?? 0; if (consumed > 0) break; await sleep(1000); }
  ok(consumed > 0, `companion consumed ${consumed} events from Kafka`);

  // 6) Metrics are real.
  console.log('6. Metrics reflect real activity');
  const metrics = await j(`${BACKEND}/api/v1/metrics`);
  ok(metrics.totalPings > 0 && metrics.cassandraWrites > 0, `pings=${metrics.totalPings} writes=${metrics.cassandraWrites} reduction=${metrics.writeReductionPercentage}%`);

  console.log(`\n\x1b[1mResult:\x1b[0m ${pass} passed, ${fail} failed\n`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error('\x1b[31mSMOKE TEST ERROR:\x1b[0m', e.message); process.exit(2); });
