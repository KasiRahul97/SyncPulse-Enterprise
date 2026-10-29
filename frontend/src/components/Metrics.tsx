import type { Metrics } from '../lib/types';

export function MetricCards({ metrics }: { metrics: Metrics | null }) {
  const m = metrics;
  const card = (
    cls: string, label: string, value: string, unit?: string, sub?: string,
  ) => (
    <div className={`metric ${cls}`}>
      <div className="label">{label}</div>
      <div className="value">{value}{unit && <span className="unit">{unit}</span>}</div>
      {sub && <div className="sub">{sub}</div>}
    </div>
  );
  return (
    <div className="metrics-row">
      {card('accent', 'Pings / sec', fmt(m?.pingsPerSecond), '', `total ${fmt(m?.totalPings)}`)}
      {card('cyan', 'DB Writes / sec', fmt(m?.writesPerSecond), '', `total ${fmt(m?.cassandraWrites)}`)}
      {card('green', 'Write Reduction', m ? m.writeReductionPercentage.toFixed(1) : '0.0', '%',
        `${fmt(m?.debounced)} debounced`)}
      {card('amber', 'Conflicts', fmt(m?.conflictsLogged), '',
        `${fmt(m?.rejectedStale)} stale · ${fmt(m?.concurrentConflicts)} concurrent`)}
      {card('purple', 'Events / sec', fmt(m?.eventsPerSecond), '', `total ${fmt(m?.kafkaEmitted)}`)}
      {card('accent', 'P95 Latency', m ? String(m.latencyMs.p95) : '0', 'ms',
        `p99 ${m ? m.latencyMs.p99 : 0}ms · avg ${m ? m.latencyMs.avg : 0}ms`)}
    </div>
  );
}

export function Pipeline({ metrics, active }: { metrics: Metrics | null; active: string | null }) {
  const m = metrics;
  const stages = [
    { id: 'ingest', ico: '📡', nm: 'Pings', ct: fmt(m?.totalPings) },
    { id: 'debounce', ico: '⚡', nm: 'Redis Debounce', ct: fmt(m?.debounced) },
    { id: 'conflict', ico: '🕰️', nm: 'Conflict Engine', ct: fmt(m?.persistCandidates) },
    { id: 'cassandra', ico: '🗄️', nm: 'Cassandra Writes', ct: fmt(m?.cassandraWrites) },
    { id: 'kafka', ico: '🔀', nm: 'Kafka Events', ct: fmt(m?.kafkaEmitted) },
    { id: 'consumer', ico: '📺', nm: 'Companion', ct: fmt(m?.kafkaEmitted) },
  ];
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Live Synchronization Pipeline</h3>
        <span className="hint">ping → debounce → conflict → Cassandra → CDC/Kafka → consumer</span>
      </div>
      <div className="pipeline">
        {stages.map((s, i) => (
          <div className={`stage ${active === s.id ? 'active' : ''}`} key={s.id}>
            <div className="ico">{s.ico}</div>
            <div className="nm">{s.nm}</div>
            <div className="ct">{s.ct}</div>
            {i < stages.length - 1 && <span className="arrow">→</span>}
          </div>
        ))}
      </div>
    </div>
  );
}

export function WriteReduction({ metrics }: { metrics: Metrics | null }) {
  const m = metrics;
  const total = m?.totalPings ?? 0;
  const writes = m?.cassandraWrites ?? 0;
  const pct = m?.writeReductionPercentage ?? 0;
  const naiveW = 100;
  const spW = total > 0 ? Math.max(2, (writes / total) * 100) : 0;
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Write Reduction</h3>
        <span className="hint">100 × (1 − writes / pings)</span>
      </div>
      <div className="wr-wrap">
        <div>
          <div className="wr-big">{pct.toFixed(1)}%</div>
          <div className="muted" style={{ fontSize: 12 }}>
            {fmt(writes)} durable writes for {fmt(total)} pings
          </div>
        </div>
        <div>
          <div className="wr-bar-label"><span>Naive (every ping → Cassandra)</span><span>{fmt(total)}</span></div>
          <div className="wr-bar naive"><span style={{ width: `${naiveW}%` }} /></div>
        </div>
        <div>
          <div className="wr-bar-label"><span>SyncPulse (debounced)</span><span>{fmt(writes)}</span></div>
          <div className="wr-bar sp"><span style={{ width: `${spW}%` }} /></div>
        </div>
      </div>
    </div>
  );
}

function fmt(n: number | undefined | null): string {
  if (n == null) return '—';
  if (n >= 1000) return n.toLocaleString();
  return String(n);
}
