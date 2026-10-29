import type { ComparisonSnapshot } from '../lib/types';

export function VectorClockPanel({ snap }: { snap: ComparisonSnapshot | null }) {
  return (
    <div className="panel">
      <div className="panel-title">
        <h3>Vector Clock Visualizer</h3>
        <span className="hint">causal comparison of the latest update</span>
      </div>
      {!snap ? (
        <div className="empty">Send an update or run a scenario to visualize a causal comparison.</div>
      ) : (
        <Compare snap={snap} />
      )}
    </div>
  );
}

function Compare({ snap }: { snap: ComparisonSnapshot }) {
  const keys = Array.from(new Set([...Object.keys(snap.incoming), ...Object.keys(snap.stored)])).sort();
  const verdict = snap.comparison;
  const explain: Record<string, string> = {
    BEFORE: 'Incoming is causally behind — the update would regress state, so it is REJECTED.',
    AFTER: 'Incoming causally dominates stored — safe to ACCEPT.',
    EQUAL: 'Identical causal history — idempotent, no state change.',
    CONCURRENT: 'Neither dominates — a real conflict; resolved by the highest-position-wins policy.',
  };
  return (
    <div>
      <div className="vc-compare">
        <div className="vc-col">
          <h4>Incoming · {snap.deviceId}</h4>
          {keys.map((k) => {
            const iv = snap.incoming[k] ?? 0;
            const sv = snap.stored[k] ?? 0;
            const cls = iv > sv ? 'hi-up' : iv < sv ? 'hi-down' : '';
            return <div className={`vc-row ${cls}`} key={k}><span>{k}</span><span>{iv}</span></div>;
          })}
          <div className="vc-row" style={{ borderTop: '1px solid var(--border)', marginTop: 6, paddingTop: 8 }}>
            <span className="muted">position</span><span>{snap.incomingPosition}s</span>
          </div>
        </div>

        <div className="vc-verdict">
          <div className="vc-arrow">⟶</div>
          <div className={`verdict-chip verdict-${verdict}`}>{verdict}</div>
          <div className="vc-arrow">⟶</div>
          <div className={`decision-badge d-${snap.decision}`} style={{ marginTop: 8, display: 'inline-block', padding: '4px 8px' }}>
            {snap.decision}
          </div>
        </div>

        <div className="vc-col">
          <h4>Authoritative (stored)</h4>
          {keys.map((k) => {
            const iv = snap.incoming[k] ?? 0;
            const sv = snap.stored[k] ?? 0;
            const cls = sv > iv ? 'hi-up' : sv < iv ? 'hi-down' : '';
            return <div className={`vc-row ${cls}`} key={k}><span>{k}</span><span>{sv}</span></div>;
          })}
          <div className="vc-row" style={{ borderTop: '1px solid var(--border)', marginTop: 6, paddingTop: 8 }}>
            <span className="muted">position</span><span>{snap.authoritativePosition}s</span>
          </div>
        </div>
      </div>
      <p className="muted" style={{ fontSize: 12, marginTop: 14, marginBottom: 0 }}>{explain[verdict]}</p>
    </div>
  );
}
