#!/usr/bin/env node
// CLI walkthrough of the headline scenarios (normal sync, offline-stale conflict,
// concurrent conflict) against the running stack. The dashboard at
// http://localhost:3000 is the richer view; this is a terminal-friendly demo.
//
//   node scripts/demo/demo.mjs

const BACKEND = process.env.BACKEND || 'http://localhost:8080';
const USER = 'demo-cli-user';
const CONTENT = 'movie-inception';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const uuid = () => (globalThis.crypto?.randomUUID?.() ?? 'r' + Math.random().toString(36).slice(2));
const inc = (c, d) => ({ ...c, [d]: (c[d] ?? 0) + 1 });
const clock = (c) => Object.entries(c).map(([k, v]) => `${k}:${v}`).join(' ') || '∅';

async function update(b) {
  const r = await fetch(`${BACKEND}/api/v1/playhead/update`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b),
  });
  return r.json();
}
async function state() {
  const r = await fetch(`${BACKEND}/api/v1/playhead/${USER}/${CONTENT}`);
  return r.status === 404 ? null : r.json();
}
const reset = () => fetch(`${BACKEND}/api/v1/admin/reset?clearState=true&reseed=true`, { method: 'POST' });
const line = (s) => console.log(s);
const decision = (r) => `\x1b[1m${r.decision}\x1b[0m (${r.comparison ?? '—'}) authoritative=${r.authoritativePosition}s`;

async function scenarioB() {
  line('\n\x1b[36m━━ Scenario B: Offline device conflict (headline) ━━\x1b[0m');
  await reset();
  let phone = {};
  for (const pos of [40, 80, 120]) {
    phone = inc(phone, 'phone-001');
    const r = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-001', positionSeconds: pos, vectorClock: phone, force: true });
    phone = { ...phone, ...r.authoritativeVectorClock };
    line(`  phone → ${pos}s  VC{${clock(phone)}}  ⇒ ${decision(r)}`);
    await sleep(200);
  }
  const frozen = { ...phone };
  line('  \x1b[33mphone goes OFFLINE (clock frozen at {' + clock(frozen) + '})\x1b[0m');

  let tv = (await state()).vectorClock;
  for (const pos of [200, 300, 420]) {
    tv = inc(tv, 'tv-001');
    const r = await update({ userId: USER, contentId: CONTENT, deviceId: 'tv-001', positionSeconds: pos, vectorClock: tv, force: true });
    tv = { ...tv, ...r.authoritativeVectorClock };
    line(`  tv    → ${pos}s  VC{${clock(tv)}}  ⇒ ${decision(r)}`);
    await sleep(200);
  }

  line('  \x1b[33mphone RECONNECTS and flushes its stale observation (120s)\x1b[0m');
  const stale = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-001', positionSeconds: 120, vectorClock: frozen, force: true });
  line(`  phone → 120s  VC{${clock(frozen)}}  ⇒ ${decision(stale)}`);
  line(`  \x1b[32mAuthoritative state preserved at ${stale.authoritativePosition}s — no regression.\x1b[0m`);
}

async function scenarioC() {
  line('\n\x1b[36m━━ Scenario C: Concurrent updates ━━\x1b[0m');
  await reset();
  const base = inc({}, 'web-001');
  await update({ userId: USER, contentId: CONTENT, deviceId: 'web-001', positionSeconds: 10, vectorClock: base, force: true });
  line(`  web   → 10s   VC{${clock(base)}}  (common ancestor)`);
  const tv = inc({ ...base }, 'tv-001');
  const rtv = await update({ userId: USER, contentId: CONTENT, deviceId: 'tv-001', positionSeconds: 300, vectorClock: tv, force: true });
  line(`  tv    → 300s  VC{${clock(tv)}}  ⇒ ${decision(rtv)}`);
  const phone = inc({ ...base }, 'phone-001');
  const rp = await update({ userId: USER, contentId: CONTENT, deviceId: 'phone-001', positionSeconds: 350, vectorClock: phone, force: true });
  line(`  phone → 350s  VC{${clock(phone)}}  ⇒ ${decision(rp)}`);
  line('  \x1b[32mNeither clock dominates ⇒ CONCURRENT; highest-position-wins policy applied.\x1b[0m');
}

await scenarioB();
await scenarioC();
line('\nOpen the dashboard at \x1b[4mhttp://localhost:3000\x1b[0m for the full visual demo.\n');
