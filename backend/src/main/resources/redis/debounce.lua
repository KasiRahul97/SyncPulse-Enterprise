-- ===========================================================================
-- SyncPulse — atomic debounce decision (runs entirely inside Redis)
--
-- This is the REAL debounce gate. The decision is made server-side in Redis
-- under a single atomic script execution, so concurrent pings for the same
-- stream can never race each other. The application never makes the debounce
-- decision in its own memory.
--
-- KEYS[1] = debounce state hash  (fields: pos, ts)
-- ARGV[1] = incoming position (seconds)
-- ARGV[2] = now (epoch millis)
-- ARGV[3] = position threshold (seconds)
-- ARGV[4] = time threshold (seconds)
-- ARGV[5] = force flag (1 = caller forces persistence, e.g. pause/seek/first)
-- ARGV[6] = state TTL (seconds)
--
-- Returns: { CODE, REASON, lastPos, lastTs, deltaPos, elapsedMs }
--   CODE in { PERSIST, DEBOUNCED, FORCE_PERSIST }
-- The stored last-persisted (pos, ts) is advanced ONLY when we decide to
-- persist, so the time threshold accumulates correctly across debounced pings.
-- ===========================================================================

local key        = KEYS[1]
local incomingPos = tonumber(ARGV[1])
local now         = tonumber(ARGV[2])
local posThresh   = tonumber(ARGV[3])
local timeThresh  = tonumber(ARGV[4])
local force       = tonumber(ARGV[5])
local ttl         = tonumber(ARGV[6])

local lastPos = redis.call('HGET', key, 'pos')
local lastTs  = redis.call('HGET', key, 'ts')

-- First ping for this stream, or an explicit force -> always persist.
if lastPos == false or lastTs == false then
  redis.call('HSET', key, 'pos', incomingPos, 'ts', now)
  redis.call('EXPIRE', key, ttl)
  return { 'FORCE_PERSIST', 'first ping for stream', incomingPos, now, 0, 0 }
end

lastPos = tonumber(lastPos)
lastTs  = tonumber(lastTs)

local deltaPos  = math.abs(incomingPos - lastPos)
local elapsedMs = now - lastTs

if force == 1 then
  redis.call('HSET', key, 'pos', incomingPos, 'ts', now)
  redis.call('EXPIRE', key, ttl)
  return { 'FORCE_PERSIST', 'forced persistence (seek/pause/checkpoint)', lastPos, lastTs, deltaPos, elapsedMs }
end

if deltaPos >= posThresh then
  redis.call('HSET', key, 'pos', incomingPos, 'ts', now)
  redis.call('EXPIRE', key, ttl)
  return { 'PERSIST', 'position delta >= threshold', lastPos, lastTs, deltaPos, elapsedMs }
end

if elapsedMs >= (timeThresh * 1000) then
  redis.call('HSET', key, 'pos', incomingPos, 'ts', now)
  redis.call('EXPIRE', key, ttl)
  return { 'PERSIST', 'time since last persist >= threshold', lastPos, lastTs, deltaPos, elapsedMs }
end

-- Not significant enough — drop before it reaches Cassandra.
return { 'DEBOUNCED', 'below position and time thresholds', lastPos, lastTs, deltaPos, elapsedMs }
