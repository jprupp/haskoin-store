-- The shared mempool of a fleet: poolScript in Haskoin.Store.Fleet.Redis,
-- and offer, withdraw and advance in Haskoin.Store.Fleet.Pool. The three are
-- one script because they share how a transaction is put in and taken out.
--
-- ARGV[1] is what to do, ARGV[2] the prefix of the keys, and ARGV[3] the
-- size of a transaction id in bytes. An output is the id of the transaction
-- that made it and four more bytes. Lists are their items one after the
-- other.
--
-- 'offer': the tip that the store saw or '', the transaction, the outputs
-- that it spends, a string with '1' for each of them that the store finds
-- unspent at the tip and '0' for the others, its body, and the transactions
-- that it replaces. Returns {'taken'}, {'known'}, {'conflict', ...},
-- {'orphan', ...} or {'raced'}.
--
-- 'withdraw': the transactions. Returns those that left.
--
-- 'advance': the tip that the store saw or '', the block to move to, the
-- time in milliseconds or '' for the clock of Redis, the size of a block in
-- bytes, the number of transactions that come back, then for each of them
-- its id, the outputs that it spends and its body, and then the
-- transactions that are gone, those that are confirmed, and the outputs
-- that are spent. Returns {'moved'}, {'stays'} or {'raced'}.
local what, prefix, tsize = ARGV[1], ARGV[2], tonumber(ARGV[3])
local osize = tsize + 4
local key = {
  tip = prefix .. 'fleet:tip',
  lease = prefix .. 'fleet:lease',
  recent = prefix .. 'fleet:recent',
  spends = prefix .. 'pool:spends',
  spender = prefix .. 'pool:spender',
  body = prefix .. 'pool:body',
  kids = prefix .. 'pool:kids:'
}

local function split(s, n)
  local items = {}
  for i = 1, #s, n do
    items[#items + 1] = string.sub(s, i, i + n - 1)
  end
  return items
end

local function member(t)
  return redis.call('HEXISTS', key.spends, t) == 1
end

-- the transactions of the pool among these, and all that spend them
local function descendants(from)
  local seen, order, todo = {}, {}, {}
  for _, t in ipairs(from) do
    if member(t) then
      todo[#todo + 1] = t
    end
  end
  while #todo > 0 do
    local t = table.remove(todo)
    if not seen[t] then
      seen[t] = true
      order[#order + 1] = t
      for _, kid in ipairs(redis.call('SMEMBERS', key.kids .. t)) do
        todo[#todo + 1] = kid
      end
    end
  end
  return order, seen
end

-- take one transaction out, leaving what spends it
local function remove(t)
  local inputs = redis.call('HGET', key.spends, t)
  if not inputs then
    return
  end
  for _, o in ipairs(split(inputs, osize)) do
    redis.call('HDEL', key.spender, o)
    redis.call('SREM', key.kids .. string.sub(o, 1, tsize), t)
  end
  redis.call('HDEL', key.spends, t)
  redis.call('HDEL', key.body, t)
end

local function insert(t, inputs, body)
  redis.call('HSET', key.spends, t, inputs)
  redis.call('HSET', key.body, t, body)
  for _, o in ipairs(split(inputs, osize)) do
    redis.call('HSET', key.spender, o, t)
    redis.call('SADD', key.kids .. string.sub(o, 1, tsize), t)
  end
end

local function withdraw(from)
  local order = descendants(from)
  for _, t in ipairs(order) do
    remove(t)
  end
  return order
end

if what == 'withdraw' then
  return withdraw(split(ARGV[4], tsize))
end

local tip = redis.call('GET', key.tip) or ''
if tip ~= ARGV[4] then
  return {'raced'}
end

if what == 'offer' then
  local t, inputs, flags, body = ARGV[5], split(ARGV[6], osize), ARGV[7], ARGV[8]
  if member(t) then
    return {'known'}
  end
  -- what it replaces leaves first, but only if it is taken
  local leaving, left = descendants(split(ARGV[9], tsize))
  local rivals, counted = {}, {}
  for _, o in ipairs(inputs) do
    local s = redis.call('HGET', key.spender, o)
    if s and not left[s] and not counted[s] then
      counted[s] = true
      rivals[#rivals + 1] = s
    end
  end
  if #rivals > 0 then
    table.insert(rivals, 1, 'conflict')
    return rivals
  end
  local orphans = {'orphan'}
  for i, o in ipairs(inputs) do
    local maker = string.sub(o, 1, tsize)
    local made = member(maker) and not left[maker]
    if not made and string.sub(flags, i, i) ~= '1' then
      orphans[#orphans + 1] = o
    end
  end
  if #orphans > 1 then
    return orphans
  end
  for _, gone in ipairs(leaving) do
    remove(gone)
  end
  insert(t, ARGV[6], body)
  return {'taken'}
end

-- advance: the block must be the one that the live stores agree on
local target, clock, bsize = ARGV[5], ARGV[6], tonumber(ARGV[7])
if target == tip then
  return {'stays'}
end
local now
if clock == '' then
  local time = redis.call('TIME')
  now = time[1] * 1000 + math.floor(time[2] / 1000)
else
  now = tonumber(clock)
end
local live = {}
local leases = redis.call('HGETALL', key.lease)
for i = 1, #leases, 2 do
  if tonumber(leases[i + 1]) > now then
    live[#live + 1] = leases[i]
  end
end
if #live == 0 then
  return {'raced'}
end
table.sort(live)
local reports = {}
for i, store in ipairs(live) do
  reports[i] = redis.call('HGET', key.recent, store) or ''
end
local agreed = ''
for _, block in ipairs(split(reports[1], bsize)) do
  local everywhere = true
  for j = 2, #reports do
    local found = false
    for _, other in ipairs(split(reports[j], bsize)) do
      if other == block then
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
    agreed = block
    break
  end
end
if agreed ~= target then
  return {'raced'}
end

-- the abandoned blocks: what can come back does, and what spends the rest
-- leaves
local at = 9
for _ = 1, tonumber(ARGV[8]) do
  if not member(ARGV[at]) then
    -- it was confirmed: what spends the same in the pool gives way
    local rivals = {}
    for _, o in ipairs(split(ARGV[at + 1], osize)) do
      local s = redis.call('HGET', key.spender, o)
      if s then
        rivals[#rivals + 1] = s
      end
    end
    withdraw(rivals)
    insert(ARGV[at], ARGV[at + 1], ARGV[at + 2])
  end
  at = at + 3
end
local orphaned = {}
for _, t in ipairs(split(ARGV[at], tsize)) do
  for _, kid in ipairs(redis.call('SMEMBERS', key.kids .. t)) do
    orphaned[#orphaned + 1] = kid
  end
end
withdraw(orphaned)

-- the new blocks: what they confirm leaves alone, and what spends what they
-- spend leaves with all that spends it
for _, t in ipairs(split(ARGV[at + 1], tsize)) do
  remove(t)
end
local clashing = {}
for _, o in ipairs(split(ARGV[at + 2], osize)) do
  local s = redis.call('HGET', key.spender, o)
  if s then
    clashing[#clashing + 1] = s
  end
end
withdraw(clashing)

redis.call('SET', key.tip, target)
return {'moved'}
