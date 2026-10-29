import { useCallback, useEffect, useRef, useState } from 'react';
import { api } from './lib/api';
import { increment, merge } from './lib/vectorClock';
import { uuid } from './lib/format';
import { initialSims, type Sim, type SimAction } from './lib/sim';
import type {
  Metrics, SystemStatus, LiveEvent, ConflictRow, ConsumedEvent,
  PlayheadStateDto, ComparisonSnapshot, DemoInfo,
} from './lib/types';
import {
  runFullDemo, scenarioA, scenarioB, scenarioC, scenarioD, type ScenarioCtx,
} from './lib/scenarios';

import { TopBar } from './components/TopBar';
import { MetricCards, Pipeline, WriteReduction } from './components/Metrics';
import { Simulators } from './components/Simulators';
import { VectorClockPanel } from './components/VectorClockPanel';
import { LiveFeed, ConflictTimeline, EventStream } from './components/Feeds';
import { DemoControl, Benchmark, type DemoStage } from './components/Demo';

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export default function App() {
  const [demoInfo, setDemoInfo] = useState<DemoInfo | null>(null);
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [conflicts, setConflicts] = useState<ConflictRow[]>([]);
  const [feed, setFeed] = useState<LiveEvent[]>([]);
  const [consumerFeed, setConsumerFeed] = useState<ConsumedEvent[]>([]);
  const [consumerP95, setConsumerP95] = useState<number | null>(null);
  const [, setAuthState] = useState<PlayheadStateDto | null>(null);
  const [sims, setSims] = useState<Sim[]>([]);
  const [comparison, setComparison] = useState<ComparisonSnapshot | null>(null);

  const [demoStages, setDemoStages] = useState<DemoStage[]>([]);
  const [demoRunning, setDemoRunning] = useState(false);
  const [benchReport, setBenchReport] = useState<any | null>(null);
  const [benchRunning, setBenchRunning] = useState(false);

  const simsRef = useRef<Sim[]>([]);
  simsRef.current = sims;
  const eventCursor = useRef(0);
  const consumerCursor = useRef(0);
  const userId = demoInfo?.userId ?? 'user-001';
  const contentId = demoInfo?.contentId ?? 'movie-inception';
  const duration = demoInfo?.contentDurationSeconds ?? 8880;

  // ---- bootstrap ----
  useEffect(() => {
    (async () => {
      try {
        const info = await api.demoInfo();
        setDemoInfo(info);
        setSims(initialSims(info.devices));
      } catch {
        setSims(initialSims([
          { deviceId: 'phone-001', type: 'PHONE' },
          { deviceId: 'tv-001', type: 'TV' },
          { deviceId: 'web-001', type: 'WEB' },
          { deviceId: 'tablet-001', type: 'TABLET' },
        ]));
      }
    })();
  }, []);

  // ---- slow poll: status / metrics / conflicts / state / consumer metrics ----
  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const [st, mt] = await Promise.all([api.status(), api.metrics()]);
        if (!alive) return;
        setStatus(st); setMetrics(mt);
      } catch { /* transient */ }
      try { setConflicts(await api.conflicts(userId, 40)); } catch { /* */ }
      try { setAuthState(await api.getState(userId, contentId)); } catch { /* */ }
      try {
        const cm = await api.consumerMetrics();
        setConsumerP95(cm.propagationLatencyMs?.p95 ?? null);
      } catch { /* */ }
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => { alive = false; clearInterval(id); };
  }, [userId, contentId]);

  // ---- fast poll: live feed + consumer feed ----
  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const r = await api.events(eventCursor.current);
        if (!alive) return;
        if (r.events.length) {
          eventCursor.current = r.cursor;
          setFeed((prev) => [...r.events.reverse(), ...prev].slice(0, 120));
        }
      } catch { /* */ }
      try {
        const r = await api.consumerEvents(consumerCursor.current);
        if (!alive) return;
        if (r.events.length) {
          consumerCursor.current = r.cursor;
          setConsumerFeed((prev) => [...r.events.reverse(), ...prev].slice(0, 60));
        }
      } catch { /* */ }
    };
    tick();
    const id = setInterval(tick, 700);
    return () => { alive = false; clearInterval(id); };
  }, []);

  const patchSim = useCallback((deviceId: string, patch: Partial<Sim> | ((s: Sim) => Partial<Sim>)) => {
    setSims((prev) => prev.map((s) => {
      if (s.deviceId !== deviceId) return s;
      const p = typeof patch === 'function' ? patch(s) : patch;
      return { ...s, ...p };
    }));
  }, []);

  // ---- send a real update for a simulator ----
  const sendForSim = useCallback(async (deviceId: string, position: number, force: boolean) => {
    const sim = simsRef.current.find((s) => s.deviceId === deviceId);
    if (!sim || !sim.online) return;
    const incoming = increment(sim.clock, deviceId);
    try {
      const resp = await api.update({
        userId, contentId, deviceId, positionSeconds: position,
        vectorClock: incoming, requestId: uuid(), force, contentDurationSeconds: duration,
        clientTimestamp: Date.now(),
      });
      patchSim(deviceId, (s) => ({
        clock: s.online ? merge(incoming, resp.authoritativeVectorClock) : incoming,
        updatesSent: s.updatesSent + 1,
        lastDecision: resp.decision,
        lastResponseMs: resp.processingMs,
        flash: true,
      }));
      setComparison({
        incoming, stored: resp.authoritativeVectorClock,
        comparison: resp.comparison ?? 'AFTER', decision: resp.decision,
        incomingPosition: position, authoritativePosition: resp.authoritativePosition,
        deviceId, at: Date.now(),
      });
      setTimeout(() => patchSim(deviceId, { flash: false }), 500);
    } catch { /* surfaced via status */ }
  }, [userId, contentId, duration, patchSim]);

  // ---- device controls ----
  const handleSimAction = useCallback((deviceId: string, action: SimAction) => {
    switch (action.type) {
      case 'PLAY': patchSim(deviceId, { playing: true }); break;
      case 'PAUSE': patchSim(deviceId, { playing: false }); break;
      case 'OFFLINE': patchSim(deviceId, { online: false, playing: false }); break;
      case 'RECONNECT': patchSim(deviceId, { online: true }); break;
      case 'RESET': patchSim(deviceId, { position: 0, clock: {}, updatesSent: 0, lastDecision: null, playing: false }); break;
      case 'SEEK': {
        const sim = simsRef.current.find((s) => s.deviceId === deviceId);
        const next = Math.min(duration, (sim?.position ?? 0) + action.seconds);
        patchSim(deviceId, { position: next });
        sendForSim(deviceId, next, true);
        break;
      }
      case 'SEND': {
        const sim = simsRef.current.find((s) => s.deviceId === deviceId);
        sendForSim(deviceId, sim?.position ?? 0, action.force ?? false);
        break;
      }
    }
  }, [duration, patchSim, sendForSim]);

  // ---- auto-play ticker: advance + ping playing devices ----
  useEffect(() => {
    const id = setInterval(() => {
      for (const s of simsRef.current) {
        if (s.playing && s.online) {
          const next = Math.min(duration, s.position + 2);
          patchSim(s.deviceId, { position: next });
          sendForSim(s.deviceId, next, false);
        }
      }
    }, 1000);
    return () => clearInterval(id);
  }, [duration, patchSim, sendForSim]);

  // ---- demo orchestration ----
  const buildCtx = useCallback((): ScenarioCtx => ({
    userId, contentId, duration, sleep,
    setStages: (labels) => setDemoStages(labels.map((l) => ({ label: l, state: 'pending' as const }))),
    setStage: (i, st) => setDemoStages((prev) => prev.map((s, idx) => (idx === i ? { ...s, state: st } : s))),
    showCompare: (snap) => setComparison(snap),
  }), [userId, contentId, duration]);

  const stopSims = () => setSims((prev) => prev.map((s) => ({ ...s, playing: false, position: 0, clock: {}, updatesSent: 0, lastDecision: null, online: true })));

  const runWrapped = useCallback(async (fn: (ctx: ScenarioCtx) => Promise<any>) => {
    setDemoRunning(true);
    stopSims();
    try {
      const result = await fn(buildCtx());
      if (result && result.results) setBenchReport(result);
    } catch (e) {
      console.error('demo failed', e);
    } finally {
      setDemoRunning(false);
    }
  }, [buildCtx]);

  const handleReset = useCallback(async () => {
    try { await api.reset(true, true); } catch { /* */ }
    setFeed([]); setConflicts([]); setComparison(null); setConsumerFeed([]);
    eventCursor.current = 0; consumerCursor.current = 0;
    stopSims();
  }, []);

  const handleBenchmark = useCallback(async (cfg: Record<string, number>) => {
    setBenchRunning(true);
    try { setBenchReport(await api.benchmark(cfg)); }
    catch (e) { console.error(e); }
    finally { setBenchRunning(false); }
  }, []);

  const activeStage = deriveActiveStage(feed);

  return (
    <div className="app">
      <TopBar status={status} />
      <MetricCards metrics={metrics} />
      <Pipeline metrics={metrics} active={activeStage} />

      <div className="grid cols-3 section-gap">
        <div style={{ gridColumn: 'span 2' }} className="grid">
          <Simulators sims={sims} onAction={handleSimAction} />
          <VectorClockPanel snap={comparison} />
        </div>
        <div className="grid">
          <DemoControl
            stages={demoStages}
            running={demoRunning}
            onRun={() => runWrapped(runFullDemo)}
            onReset={handleReset}
            onScenario={(s) => runWrapped(
              s === 'A' ? scenarioA : s === 'B' ? scenarioB : s === 'C' ? scenarioC : scenarioD,
            )}
          />
        </div>
      </div>

      <div className="grid cols-2 section-gap">
        <LiveFeed feed={feed} />
        <ConflictTimeline conflicts={conflicts} />
      </div>

      <div className="grid cols-2 section-gap">
        <WriteReduction metrics={metrics} />
        <EventStream events={consumerFeed} latencyP95={consumerP95} />
      </div>

      <div className="section-gap">
        <Benchmark running={benchRunning} report={benchReport} onRun={handleBenchmark} />
      </div>

      <footer className="muted" style={{ textAlign: 'center', fontSize: 11, marginTop: 28 }}>
        SyncPulse Enterprise · Redis debounce · vector-clock conflict engine · Cassandra · CDC → Kafka → companion consumer
        {status?.components?.cdcAdapter && <> · CDC adapter: <span className="kbd">{String(status.components.cdcAdapter)}</span></>}
      </footer>
    </div>
  );
}

function deriveActiveStage(feed: LiveEvent[]): string | null {
  const latest = feed[0];
  if (!latest || Date.now() - latest.timestamp > 2000) return null;
  switch (latest.category) {
    case 'PING': return 'ingest';
    case 'DEBOUNCE': return 'debounce';
    case 'DECISION': return 'cassandra';
    case 'EVENT': return 'kafka';
    default: return null;
  }
}
