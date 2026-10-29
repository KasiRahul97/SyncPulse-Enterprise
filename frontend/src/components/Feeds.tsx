import type { LiveEvent, ConflictRow, ConsumedEvent } from '../lib/types';
import { hhmmss } from '../lib/format';
import { formatClock } from '../lib/vectorClock';

export function LiveFeed({ feed }: { feed: LiveEvent[] }) {
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Live Pipeline Activity</h3>
        <span className="hint">newest first</span>
      </div>
      {feed.length === 0 ? (
        <div className="empty">Waiting for pipeline activity…</div>
      ) : (
        <div className="feed">
          {feed.map((e) => (
            <div className="feed-row" key={e.seq}>
              <span className="time">{hhmmss(e.timestamp)}</span>
              <span className={`tag ${e.category}`}>{e.category}</span>
              <span className="msg" title={e.message}>
                {e.deviceId && <span className="dev">{e.deviceId} </span>}
                {e.message}
              </span>
              {e.decision && <span className={`decision-badge d-${e.decision}`}>{e.decision}</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function ConflictTimeline({ conflicts }: { conflicts: ConflictRow[] }) {
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Conflict Audit Timeline</h3>
        <span className="hint">persisted to Cassandra · conflict_log</span>
      </div>
      {conflicts.length === 0 ? (
        <div className="empty">No conflicts yet. Run the Offline or Concurrent scenario.</div>
      ) : (
        <div className="feed">
          {conflicts.map((c) => (
            <details className="feed-row" key={c.conflictId} style={{ display: 'block' }}>
              <summary style={{ cursor: 'pointer', listStyle: 'none', display: 'grid',
                gridTemplateColumns: '62px 90px 1fr auto', gap: 10, alignItems: 'center' }}>
                <span className="time">{hhmmss(new Date(c.resolvedAt).getTime())}</span>
                <span className="dev">{c.incomingDeviceId}</span>
                <span className="msg" title={c.reason}>{c.reason}</span>
                <span className={`decision-badge d-${c.decision}`}>{c.decision}</span>
              </summary>
              <div className="mono" style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 8, paddingLeft: 4 }}>
                <div>comparison: <b>{c.comparison}</b></div>
                <div>incoming VC: {formatClock(c.incomingVectorClock)} @ {c.incomingPosition}s</div>
                <div>stored&nbsp;&nbsp;VC: {formatClock(c.storedVectorClock)} → authoritative {c.authoritativePosition}s</div>
              </div>
            </details>
          ))}
        </div>
      )}
    </div>
  );
}

export function EventStream({
  events, latencyP95,
}: {
  events: ConsumedEvent[];
  latencyP95: number | null;
}) {
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Kafka Event Stream → Companion</h3>
        <span className="hint">{latencyP95 != null ? `p95 propagation ${latencyP95}ms` : 'downstream consumer'}</span>
      </div>
      {events.length === 0 ? (
        <div className="empty">No events consumed yet. Accepted updates emit events here.</div>
      ) : (
        <div className="feed">
          {events.map((e) => (
            <div className="feed-row" key={e.seq} style={{ gridTemplateColumns: '62px 1fr auto' }}>
              <span className="time">{hhmmss(e.receivedAtEpochMs)}</span>
              <span className="msg">
                <span className="tag EVENT">{e.eventType}</span>{' '}
                <span className="dev">{e.deviceId}</span> pos={e.positionSeconds}s{' '}
                <span className="muted">via {e.cdcSource}</span>
              </span>
              <span className="mono" style={{ fontSize: 11, color: 'var(--cyan)' }}>
                ✓ {e.propagationLatencyMs}ms
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
