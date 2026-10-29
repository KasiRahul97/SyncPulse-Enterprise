import type { SystemStatus } from '../lib/types';

const COMPONENTS: { key: string; label: string }[] = [
  { key: 'api', label: 'API' },
  { key: 'redis', label: 'Redis' },
  { key: 'cassandra', label: 'Cassandra' },
  { key: 'kafka', label: 'Kafka' },
  { key: 'cdc', label: 'CDC' },
];

export function TopBar({ status }: { status: SystemStatus | null }) {
  const healthy = status?.status === 'HEALTHY';
  const adapter = (status?.components?.cdcAdapter as string) ?? 'LOCAL_ADAPTER';
  return (
    <div className="topbar">
      <div className="brand">
        <div className="logo">S</div>
        <div>
          <h1>SyncPulse Enterprise</h1>
          <div className="sub">Control Center · Cross-Device Playhead Synchronization</div>
        </div>
      </div>
      <div className="status-cluster">
        {status && (
          <div className={`system-badge ${healthy ? 'healthy' : 'degraded'}`}>
            SYSTEM {status.status}
          </div>
        )}
        {COMPONENTS.map((c) => {
          const up = status?.components?.[c.key] === true;
          return (
            <div className="status-pill" key={c.key} title={c.key === 'cdc' ? `adapter: ${adapter}` : ''}>
              <span className={`dot ${status ? (up ? 'up' : 'down') : ''}`} />
              {c.label}
            </div>
          );
        })}
      </div>
    </div>
  );
}
