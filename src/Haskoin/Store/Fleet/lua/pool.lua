-- The shared mempool of a fleet: poolScript in Haskoin.Store.Fleet.Redis,
-- and offer, withdraw and advance in Haskoin.Store.Fleet.Pool. The three are
-- one script because they share how a transaction is put in and taken out.
--
-- ARGV[1] is what to do, ARGV[2] the prefix of the keys, and ARGV[3] the
-- size of a transaction id in bytes. An output is the id of the transaction
-- that made it and four more bytes. Lists are their items one after the
-- other.
--
-- A transaction is given as its id, the time it entered a mempool, the
-- outputs that it spends, its body, '1' if it says itself that it can be
-- replaced by fee or '0', the number of its entries by address, and then
-- five arguments for each entry: its kind, the address, an output
-- or '', an amount or '', and a script or ''. The kinds are 't', the
-- transaction touches the address; 's', it spends that output of the
-- address, of that amount; and 'o', it pays that output to the address,
-- with that amount and script.
--
-- 'offer': the tip that the store saw or '', a string with '1' for each
-- output that the transaction spends and that the store finds unspent at
-- the tip and '0' for the others, the transactions that it replaces, and
-- the transaction. Returns {'taken'}, {'known'}, {'conflict', ...},
-- {'orphan', ...} or {'raced'}.
--
-- 'withdraw': the transactions. Returns those that left.
--
-- 'advance': the tip that the store saw or '', the block to move to, the
-- time in milliseconds or '' for the clock of Redis, the size of a block in
-- bytes, '1' to empty the pool instead of changing it or '0', the
-- transactions that are gone, those that are confirmed, the outputs that
-- are spent, the number of transactions that come back, those
-- transactions, and the blocks that the tip moves through, oldest first,
-- or ''. Returns {'moved'}, {'stays'} or {'raced'}.
--
-- The pool is emptied instead of changed in two cases. With '1' the store
-- cannot say what the blocks between the two tips did, because it does
-- not have the old tip: only when no other store is live, and {'raced'}
-- otherwise, since another store may know. With '2' the store has both
-- tips and the way between them is longer than a store goes through: that
-- is so for every store, so any of them may, or the tip would never move
-- again. Each time the pool is emptied a counter goes up, next to the
-- tip, which tells the stores to offer their mempools again.
--
-- What the pool holds for each address, kept in step with its transactions:
-- its transactions and the outputs paid to it, both ordered newest first,
-- the outputs of it that are spent, and the sums of all that, which are the
-- summary of Haskoin.Store.Database.Types.MemSummary. It also holds every
-- transaction by the time it entered a mempool, which is the list of the
-- mempool, and by its id alone, to find those whose id starts with some
-- bytes.
--
-- A transaction can be replaced by fee if it says so itself or if it spends
-- from a transaction of the pool that can be. The second changes when a
-- parent is confirmed or comes back from an abandoned block, so the pool
-- keeps the answer apart from the body, and corrects it for what spends a
-- transaction whenever that one comes or goes. An output counts in
-- 'both' while the pool has both the entry that pays it and the entry that
-- spends it, so it is added by the second to come and taken off by the
-- first to go.
--
-- Every move of the tip is counted, in 'fleet:moves'. The cache of extended
-- public keys answers at one move of the tip and not at one block, since
-- the tip can leave a block and come back to it, and what the pool did to
-- the entries on the way is not undone by coming back.
--
-- The cache of extended public keys (xpub.lua) has, for each key that it
-- holds, the transactions of its addresses and the outputs paid to them,
-- those of the pool among them. So what the pool puts in or takes out at an
-- address goes in or out of the entry of every key that has the address,
-- here, in the same step.
local what, prefix, tsize = ARGV[1], ARGV[2], tonumber(ARGV[3])
local osize = tsize + 4
local key = {
  tip = prefix .. 'fleet:tip',
  moves = prefix .. 'fleet:moves',
  restarts = prefix .. 'fleet:restarts',
  events = prefix .. 'fleet:events',
  lease = prefix .. 'fleet:lease',
  recent = prefix .. 'fleet:recent',
  spends = prefix .. 'pool:spends',
  spender = prefix .. 'pool:spender',
  body = prefix .. 'pool:body',
  kids = prefix .. 'pool:kids:',
  entries = prefix .. 'pool:entries',
  time = prefix .. 'pool:time',
  ids = prefix .. 'pool:ids',
  own = prefix .. 'pool:own',
  rbf = prefix .. 'pool:rbf',
  atx = prefix .. 'pool:atx:',
  aout = prefix .. 'pool:aout:',
  aval = prefix .. 'pool:aval:',
  aspent = prefix .. 'pool:aspent:',
  asum = prefix .. 'pool:asum:',
  of = prefix .. 'fx:of:',
  entry = prefix .. 'fx:e:'
}

local function split(s, n)
  local items = {}
  for i = 1, #s, n do
    items[#items + 1] = string.sub(s, i, i + n - 1)
  end
  return items
end

-- Tell every store of the fleet that something happened: 'b' and a block
-- that the tip moved to or through, 't' and a transaction that entered the
-- pool, 'd' and one that left it unconfirmed. The events go in a stream, in
-- the step that makes the change, so that every store serves the same
-- events in the same order, whatever its own node saw and when. The stream
-- keeps about the last ten thousand.
local function emit(kind, what)
  redis.call('XADD', key.events, 'MAXLEN', '~', '10000', '*', 'k', kind, 'h', what)
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

-- a transaction of the arguments, from a position, and the position after
-- it
local function read(at)
  local tx = {id = ARGV[at], time = tonumber(ARGV[at + 1]), inputs = ARGV[at + 2], body = ARGV[at + 3]}
  tx.own = ARGV[at + 4] == '1'
  local entries = {}
  local n = tonumber(ARGV[at + 5])
  at = at + 6
  for i = 1, n do
    entries[i] = {ARGV[at], ARGV[at + 1], ARGV[at + 2], ARGV[at + 3], ARGV[at + 4]}
    at = at + 5
  end
  tx.entries = entries
  return tx, at
end

local function add(a, field, by)
  redis.call('HINCRBY', key.asum .. a, field, string.format('%.0f', by))
end

-- put the entries of a transaction in, with sign 1, or take them out, with
-- sign -1
local function account(t, time, entries, sign)
  local touched = {}
  for _, e in ipairs(entries) do
    local kind, a, o, v = e[1], e[2], e[3], tonumber(e[4])
    touched[a] = true
    -- the cached keys that have the address
    local watching = {}
    if kind ~= 's' then
      watching = redis.call('SMEMBERS', key.of .. a)
    end
    if kind == 't' then
      if sign > 0 then
        redis.call('ZADD', key.atx .. a, -time, t)
      else
        redis.call('ZREM', key.atx .. a, t)
      end
      for _, id in ipairs(watching) do
        if sign > 0 then
          redis.call('ZADD', key.entry .. id .. ':txs', -time, t)
        else
          redis.call('ZREM', key.entry .. id .. ':txs', t)
        end
      end
      add(a, 'txs', sign)
    elseif kind == 's' then
      if sign > 0 then
        redis.call('HSET', key.aspent .. a, o, e[4])
      else
        redis.call('HDEL', key.aspent .. a, o)
      end
      add(a, 'spent', sign * v)
      add(a, 'spentCount', sign)
      if redis.call('HEXISTS', key.aval .. a, o) == 1 then
        add(a, 'both', sign * v)
        add(a, 'bothCount', sign)
      end
    else
      if sign > 0 then
        redis.call('ZADD', key.aout .. a, -time, o)
        redis.call('HSET', key.aval .. a, o, e[4] .. ' ' .. e[5])
      else
        redis.call('ZREM', key.aout .. a, o)
        redis.call('HDEL', key.aval .. a, o)
      end
      for _, id in ipairs(watching) do
        if sign > 0 then
          redis.call('ZADD', key.entry .. id .. ':outs', -time, o)
          redis.call('HSET', key.entry .. id .. ':oval', o, e[4] .. ' ' .. #a .. ' ' .. a .. e[5])
        else
          redis.call('ZREM', key.entry .. id .. ':outs', o)
          redis.call('HDEL', key.entry .. id .. ':oval', o)
        end
      end
      add(a, 'paid', sign * v)
      add(a, 'paidCount', sign)
      if redis.call('HEXISTS', key.aspent .. a, o) == 1 then
        add(a, 'both', sign * v)
        add(a, 'bothCount', sign)
      end
    end
  end
  -- an address that no transaction touches has no summary
  if sign < 0 then
    for a, _ in pairs(touched) do
      if tonumber(redis.call('HGET', key.asum .. a, 'txs')) == 0 then
        redis.call('DEL', key.asum .. a)
      end
    end
  end
end

-- work out again whether a transaction of the pool can be replaced by
-- fee, and if that changed, the same for what spends it
local function replaceable(t)
  local inputs = redis.call('HGET', key.spends, t)
  if not inputs then
    return
  end
  local can = redis.call('SISMEMBER', key.own, t) == 1
  if not can then
    for _, o in ipairs(split(inputs, osize)) do
      if redis.call('SISMEMBER', key.rbf, string.sub(o, 1, tsize)) == 1 then
        can = true
        break
      end
    end
  end
  if can ~= (redis.call('SISMEMBER', key.rbf, t) == 1) then
    if can then
      redis.call('SADD', key.rbf, t)
    else
      redis.call('SREM', key.rbf, t)
    end
    for _, kid in ipairs(redis.call('SMEMBERS', key.kids .. t)) do
      replaceable(kid)
    end
  end
end

-- take one transaction out, leaving what spends it: quietly if a block
-- confirmed it, which is no event of the mempool
local function remove(t, quiet)
  local inputs = redis.call('HGET', key.spends, t)
  if not inputs then
    return
  end
  if not quiet then
    emit('d', t)
  end
  local kept = cmsgpack.unpack(redis.call('HGET', key.entries, t))
  account(t, kept[1], kept[2], -1)
  redis.call('HDEL', key.entries, t)
  redis.call('ZREM', key.time, t)
  redis.call('ZREM', key.ids, t)
  for _, o in ipairs(split(inputs, osize)) do
    redis.call('HDEL', key.spender, o)
    redis.call('SREM', key.kids .. string.sub(o, 1, tsize), t)
  end
  redis.call('HDEL', key.spends, t)
  redis.call('HDEL', key.body, t)
  redis.call('SREM', key.own, t)
  if redis.call('SREM', key.rbf, t) == 1 then
    for _, kid in ipairs(redis.call('SMEMBERS', key.kids .. t)) do
      replaceable(kid)
    end
  end
end

local function insert(tx)
  local t = tx.id
  redis.call('HSET', key.spends, t, tx.inputs)
  redis.call('HSET', key.body, t, tx.body)
  for _, o in ipairs(split(tx.inputs, osize)) do
    redis.call('HSET', key.spender, o, t)
    redis.call('SADD', key.kids .. string.sub(o, 1, tsize), t)
  end
  redis.call('HSET', key.entries, t, cmsgpack.pack({tx.time, tx.entries}))
  redis.call('ZADD', key.time, tx.time, t)
  redis.call('ZADD', key.ids, 0, t)
  account(t, tx.time, tx.entries, 1)
  if tx.own then
    redis.call('SADD', key.own, t)
  end
  replaceable(t)
  emit('t', t)
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
  local flags = ARGV[5]
  local tx = read(7)
  local t, inputs = tx.id, split(tx.inputs, osize)
  if member(t) then
    return {'known'}
  end
  -- what it replaces leaves first, but only if it is taken
  local leaving, left = descendants(split(ARGV[6], tsize))
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
  insert(tx)
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

-- a store that does not know the way from the old tip starts the pool
-- again: if it is alone, or if the way is too long for any store
if ARGV[8] == '1' or ARGV[8] == '2' then
  if ARGV[8] == '1' and #live ~= 1 then
    return {'raced'}
  end
  redis.call('INCR', key.restarts)
  for _, k in ipairs(redis.call('KEYS', prefix .. 'pool:*')) do
    redis.call('DEL', k)
  end
  redis.call('SET', key.tip, target)
  redis.call('INCR', key.moves)
  emit('b', target)
  return {'moved'}
end

-- the abandoned blocks: what can come back does, and what spends the rest
-- leaves
local at = 13
for _ = 1, tonumber(ARGV[12]) do
  local tx
  tx, at = read(at)
  if not member(tx.id) then
    -- it was confirmed: what spends the same in the pool gives way
    local rivals = {}
    for _, o in ipairs(split(tx.inputs, osize)) do
      local s = redis.call('HGET', key.spender, o)
      if s then
        rivals[#rivals + 1] = s
      end
    end
    withdraw(rivals)
    insert(tx)
  end
end
local orphaned = {}
for _, t in ipairs(split(ARGV[9], tsize)) do
  for _, kid in ipairs(redis.call('SMEMBERS', key.kids .. t)) do
    orphaned[#orphaned + 1] = kid
  end
end
withdraw(orphaned)

-- the new blocks: what they confirm leaves alone, and what spends what they
-- spend leaves with all that spends it
for _, t in ipairs(split(ARGV[10], tsize)) do
  remove(t, true)
end
local clashing = {}
for _, o in ipairs(split(ARGV[11], osize)) do
  local s = redis.call('HGET', key.spender, o)
  if s then
    clashing[#clashing + 1] = s
  end
end
withdraw(clashing)

redis.call('SET', key.tip, target)
redis.call('INCR', key.moves)
-- the blocks that the tip moved through, oldest first, if the store that
-- moves it gave them, and the block that it is at now otherwise
local path = split(ARGV[at] or '', bsize)
if #path == 0 then
  path = {target}
end
for _, block in ipairs(path) do
  emit('b', block)
end
return {'moved'}
