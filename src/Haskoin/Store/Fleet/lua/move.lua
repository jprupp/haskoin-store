-- Move the tip to the newest block that every live store reported, if the
-- tip is still the one that the caller saw: moveScript in
-- Haskoin.Store.Fleet.Redis, and agreed and move in Haskoin.Store.Fleet.
--
-- KEYS: the tip, the leases, the recent blocks.
-- ARGV: the tip that the caller saw, or '' for none, the time in
-- milliseconds or '' for the clock of Redis, and the size of a block in
-- bytes.
--
-- Returns {'moved', block}, {'stays'} or {'raced'}.
local seen, clock, size = ARGV[1], ARGV[2], tonumber(ARGV[3])

local tip = redis.call('GET', KEYS[1]) or ''
if tip ~= seen then
  return {'raced'}
end

local now
if clock == '' then
  local t = redis.call('TIME')
  now = t[1] * 1000 + math.floor(t[2] / 1000)
else
  now = tonumber(clock)
end

-- the stores whose lease has not run out, in the order of their names
local live = {}
local leases = redis.call('HGETALL', KEYS[2])
for i = 1, #leases, 2 do
  if tonumber(leases[i + 1]) > now then
    live[#live + 1] = leases[i]
  end
end
if #live == 0 then
  return {'stays'}
end
table.sort(live)

local reports = {}
for i, store in ipairs(live) do
  reports[i] = redis.call('HGET', KEYS[3], store) or ''
end

-- the first block of one report that every other report has
local first = reports[1]
for i = 1, #first, size do
  local block = string.sub(first, i, i + size - 1)
  local everywhere = true
  for j = 2, #reports do
    local found = false
    for k = 1, #reports[j], size do
      if string.sub(reports[j], k, k + size - 1) == block then
        found = true
        break
      end
    end
    if not found then
      everywhere = false
      break
    end
  end
  if everywhere then
    if block == tip then
      return {'stays'}
    end
    redis.call('SET', KEYS[1], block)
    return {'moved', block}
  end
end
return {'stays'}
