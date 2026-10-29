import { useState } from 'react';

export interface DemoStage { label: string; state: 'pending' | 'running' | 'done'; }

export function DemoControl({
  stages, running, onRun, onReset, onScenario,
}: {
  stages: DemoStage[];
  running: boolean;
  onRun: () => void;
  onReset: () => void;
  onScenario: (s: 'A' | 'B' | 'C' | 'D') => void;
}) {
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Demo Orchestrator</h3>
        <span className="hint">real API calls · deterministic</span>
      </div>
      <div className="btn-row" style={{ marginBottom: 12 }}>
        <button className="btn primary" disabled={running} onClick={onRun}>
          {running ? <><span className="spinner" /> Running…</> : '▶ Run Live Demo'}
        </button>
        <button className="btn" disabled={running} onClick={onReset}>↺ Reset</button>
      </div>
      <div className="btn-row" style={{ marginBottom: 14 }}>
        <button className="btn sm" disabled={running} onClick={() => onScenario('A')}>A · Normal Sync</button>
        <button className="btn sm" disabled={running} onClick={() => onScenario('B')}>B · Offline Conflict</button>
        <button className="btn sm" disabled={running} onClick={() => onScenario('C')}>C · Concurrent</button>
        <button className="btn sm" disabled={running} onClick={() => onScenario('D')}>D · Ping Storm</button>
      </div>
      <div className="demo-stages">
        {stages.map((s, i) => (
          <div className={`demo-stage ${s.state}`} key={i}>
            <span className="num">{String(i + 1).padStart(2, '0')}</span>
            <span>{s.label}</span>
            <span className="status-ico">
              {s.state === 'done' ? '✓' : s.state === 'running' ? <span className="spinner" /> : '·'}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function Benchmark({
  running, report, onRun,
}: {
  running: boolean;
  report: any | null;
  onRun: (cfg: Record<string, number>) => void;
}) {
  const [cfg, setCfg] = useState({ users: 5, devicesPerUser: 2, pingsPerDevice: 200, positionStepSeconds: 2, concurrency: 8 });
  const field = (key: keyof typeof cfg, label: string) => (
    <label className="field">
      {label}
      <input type="number" min={1} value={cfg[key]}
        onChange={(e) => setCfg({ ...cfg, [key]: Math.max(1, parseInt(e.target.value || '1', 10)) })} />
    </label>
  );
  const res = report?.results;
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Benchmark · Naive vs SyncPulse</h3>
        <span className="hint">measured against the running stack</span>
      </div>
      <div className="bench-grid" style={{ marginBottom: 12 }}>
        {field('users', 'Users')}
        {field('devicesPerUser', 'Devices/user')}
        {field('pingsPerDevice', 'Pings/device')}
        {field('positionStepSeconds', 'Step (s)')}
        {field('concurrency', 'Concurrency')}
      </div>
      <button className="btn primary" disabled={running} onClick={() => onRun(cfg)}>
        {running ? <><span className="spinner" /> Running benchmark…</> : '▶ Run Benchmark'}
      </button>

      {report && res && (
        <div className="section-gap">
          <div className="bench-grid">
            <Stat l="Write Reduction" v={`${res.writeReductionPercentage}%`} accent />
            <Stat l="Naive writes" v={num(report.naiveBaseline.cassandraWrites)} />
            <Stat l="SyncPulse writes" v={num(report.syncpulse.cassandraWrites)} />
            <Stat l="Throughput" v={`${num(res.throughputRequestsPerSecond)}/s`} />
            <Stat l="Total requests" v={num(report.syncpulse.totalRequests)} />
            <Stat l="Kafka events" v={num(report.syncpulse.kafkaEventsEmitted)} />
            <Stat l="Latency p95" v={`${res.latencyMs.p95}ms`} />
            <Stat l="Latency p99" v={`${res.latencyMs.p99}ms`} />
          </div>
          <div className="muted" style={{ fontSize: 11, marginTop: 10 }}>
            Elapsed {num(res.elapsedMs)}ms · p50 {res.latencyMs.p50}ms · avg {res.latencyMs.avg}ms · MEASURED
          </div>
        </div>
      )}
    </div>
  );
}

function Stat({ l, v, accent }: { l: string; v: string; accent?: boolean }) {
  return (
    <div className="bench-stat">
      <div className="l">{l}</div>
      <div className="v" style={accent ? { color: 'var(--green)' } : undefined}>{v}</div>
    </div>
  );
}

function num(n: number): string {
  return n == null ? '—' : n.toLocaleString();
}
