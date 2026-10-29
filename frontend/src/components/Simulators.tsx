import type { Sim, SimAction } from '../lib/sim';
import { deviceIcon, formatClockTime } from '../lib/format';
import { formatClock } from '../lib/vectorClock';

const DURATION = 8880;

export function Simulators({
  sims, onAction,
}: {
  sims: Sim[];
  onAction: (deviceId: string, action: SimAction) => void;
}) {
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Device Simulators</h3>
        <span className="hint">each device keeps its own vector clock</span>
      </div>
      <div className="devices">
        {sims.map((s) => (
          <DeviceCard key={s.deviceId} sim={s} onAction={(a) => onAction(s.deviceId, a)} />
        ))}
      </div>
    </div>
  );
}

function DeviceCard({ sim, onAction }: { sim: Sim; onAction: (a: SimAction) => void }) {
  const pct = Math.min(100, (sim.position / DURATION) * 100);
  return (
    <div className={`device ${sim.online ? 'online' : 'offline'} ${sim.flash ? 'flash' : ''}`}>
      <div className="device-head">
        <div className="device-id">
          <span className="device-icon">{deviceIcon(sim.kind)}</span>
          <div>
            <div className="name">{sim.deviceId}</div>
            <div className="kind">{sim.kind}</div>
          </div>
        </div>
        <span className={`online-tag ${sim.online ? 'on' : 'off'}`}>
          {sim.online ? '● ONLINE' : '○ OFFLINE'}
        </span>
      </div>

      <div className="position-display">
        {formatClockTime(sim.position)} <span className="sm">/ {formatClockTime(DURATION)}</span>
      </div>
      <div className="progress"><span style={{ width: `${pct}%` }} /></div>

      <div className="clock-chip">VC {formatClock(sim.clock)}</div>

      <div className="device-meta">
        <span>updates sent: <b>{sim.updatesSent}</b></span>
        {sim.lastResponseMs != null && <span>{sim.lastResponseMs}ms</span>}
      </div>

      {sim.lastDecision && (
        <div className="last-decision">
          last: <span className={`decision-badge d-${sim.lastDecision}`}>{sim.lastDecision}</span>
        </div>
      )}

      <div className="device-controls">
        {sim.playing
          ? <button className="btn sm" onClick={() => onAction({ type: 'PAUSE' })}>⏸ Pause</button>
          : <button className="btn sm" onClick={() => onAction({ type: 'PLAY' })}>▶ Play</button>}
        <button className="btn sm" onClick={() => onAction({ type: 'SEEK', seconds: 5 })}>+5s</button>
        <button className="btn sm" onClick={() => onAction({ type: 'SEEK', seconds: 30 })}>+30s</button>
        <button className="btn sm" onClick={() => onAction({ type: 'SEND' })}>Send</button>
        {sim.online
          ? <button className="btn sm warn" onClick={() => onAction({ type: 'OFFLINE' })}>Go Offline</button>
          : <button className="btn sm primary" onClick={() => onAction({ type: 'RECONNECT' })}>Reconnect</button>}
        <button className="btn sm ghost" onClick={() => onAction({ type: 'RESET' })}>Reset</button>
      </div>
    </div>
  );
}
