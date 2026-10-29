#!/usr/bin/env node
// Runs the SyncPulse load benchmark against the running stack and prints the
// measured naive-vs-SyncPulse comparison.
//
//   node scripts/benchmark/benchmark.mjs [users] [devicesPerUser] [pingsPerDevice] [step] [concurrency]

const BACKEND = process.env.BACKEND || 'http://localhost:8080';
const [, , users = 5, devicesPerUser = 3, pingsPerDevice = 400, positionStepSeconds = 2, concurrency = 8] = process.argv;

const cfg = {
  users: +users, devicesPerUser: +devicesPerUser, pingsPerDevice: +pingsPerDevice,
  positionStepSeconds: +positionStepSeconds, concurrency: +concurrency,
};

console.log('Running benchmark with config:', cfg, '\n');

const r = await fetch(`${BACKEND}/api/v1/benchmark/run`, {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg),
});
if (!r.ok) { console.error('Benchmark failed:', r.status, await r.text()); process.exit(1); }
const report = await r.json();

const b = report.naiveBaseline, s = report.syncpulse, res = report.results;
console.log('── Naive baseline ─────────────────────────────');
console.log(`  ${b.model}`);
console.log(`  Cassandra writes : ${b.cassandraWrites.toLocaleString()}`);
console.log('\n── SyncPulse ──────────────────────────────────');
console.log(`  ${s.model}`);
console.log(`  Total requests   : ${s.totalRequests.toLocaleString()}`);
console.log(`  Cassandra writes : ${s.cassandraWrites.toLocaleString()}`);
console.log(`  Debounced        : ${s.debouncedRequests.toLocaleString()}`);
console.log(`  Kafka events     : ${s.kafkaEventsEmitted.toLocaleString()}`);
console.log('\n── Results (MEASURED) ─────────────────────────');
console.log(`  Write reduction  : ${res.writeReductionPercentage}%`);
console.log(`  Throughput       : ${res.throughputRequestsPerSecond.toLocaleString()} req/s`);
console.log(`  Elapsed          : ${res.elapsedMs} ms`);
console.log(`  Latency p50/p95/p99/max : ${res.latencyMs.p50}/${res.latencyMs.p95}/${res.latencyMs.p99}/${res.latencyMs.max} ms`);
console.log('');
