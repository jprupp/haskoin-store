-- Note the recent blocks of a store under a lease, if the store may report:
-- reportScript in Haskoin.Store.Fleet.Redis, and mayReport and report in
-- Haskoin.Store.Fleet.
--
-- KEYS: the tip, the leases, the recent blocks.
-- ARGV: the store, the length of the lease in milliseconds, the time in
-- milliseconds or '' for the clock of Redis, the size of a block in bytes,
-- the recent blocks of the store, newest first, and the blocks that it has
-- views of, each as blocks one after the other.
--
-- Returns 1 if the report was taken and 0 if the store may not report.
local store, lease, clock, size = ARGV[1], tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4])
local recent, views = ARGV[5], ARGV[6]

local now
if clock == '' then
  local t = redis.call('TIME')
  now = t[1] * 1000 + math.floor(t[2] / 1000)
else
  now = tonumber(clock)
end

-- whether the store has a view of the tip
local tip = redis.call('GET', KEYS[1])
local has = not tip
if tip then
  for i = 1, #views, size do
    if string.sub(views, i, i + size - 1) == tip then
      has = true
      break
    end
  end
end

-- whether another store is live; leases that ran out are forgotten
local alone = true
local leases = redis.call('HGETALL', KEYS[2])
for i = 1, #leases, 2 do
  if tonumber(leases[i + 1]) > now then
    if leases[i] ~= store then
      alone = false
    end
  else
    redis.call('HDEL', KEYS[2], leases[i])
    redis.call('HDEL', KEYS[3], leases[i])
  end
end

if not (has or alone) then
  return 0
end
redis.call('HSET', KEYS[2], store, now + lease)
redis.call('HSET', KEYS[3], store, recent)
return 1
