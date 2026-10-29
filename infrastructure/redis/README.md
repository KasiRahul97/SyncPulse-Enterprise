# Redis — debounce layer

Redis is the high-speed, non-durable gate in front of Cassandra. It is **not** the
system of record.

The debounce decision is made by an **atomic Lua script** executed inside Redis via
`EVALSHA`, so concurrent pings for the same stream cannot race. The script lives with the
backend so it is versioned alongside the code that calls it:

- Script: [`backend/src/main/resources/redis/debounce.lua`](../../backend/src/main/resources/redis/debounce.lua)
- Caller: [`DebounceService`](../../backend/src/main/java/com/syncpulse/debounce/DebounceService.java)

## Keys

| Key pattern | Type | Purpose |
|---|---|---|
| `sp:debounce:{user}:{content}:{device}` | hash `{pos, ts}` | last-persisted position/time per stream |
| `sp:idem:{requestId}` | string | idempotency claim (`PENDING` → final decision) |

Both carry TTLs (`syncpulse.debounce.state-ttl-seconds`, `syncpulse.idempotency.ttl-seconds`)
so Redis stays bounded. Persistence is disabled (`--appendonly no --save ""`) because Redis
holds only transient state.
