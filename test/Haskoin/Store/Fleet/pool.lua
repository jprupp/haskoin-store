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
-- replaced by fee or '0', the number of its entries, and then six
-- arguments for each entry: its kind, an address or '', an output or '',
-- an amount or '', a script or '', and a category of tokens or ''. The
-- kinds are 't', the transaction touches the address; 's', it spends that
-- output of the address, of that amount; and 'o', it pays that output to
-- the address, with that amount and script.
--
-- The tokens of Bitcoin Cash (CashTokens) have kinds of their own, so that
-- this script never reads the token prefix of a script: 'm', the
-- transaction makes that output, which holds that amount of fungible
-- tokens of the category, and a script that is the value of the output, a
-- space and its script; 'u', it uses up that output, with that amount of
-- the category. 'M' and 'U' are the same for an output that also holds a
-- non-fungible token. The address is that of the output, or '' if it has
-- none. The amount is in decimal digits, as long as it needs, up to the
-- largest signed integer of 64 bits.
--
-- 'offer': the tip that the store saw or '', a string with '1' for each
-- output that the transaction spends and that the store finds unspent at
-- the tip and '0' for the others, the transactions that it replaces, and
-- the transaction. Returns {'taken'}, {'known'}, {'conflict', ...},
-- {'orphan', ...} or {'raced'}.
--
-- 'withdraw': the transactions. Returns those that left.
--
-- 'trim': how many of the transactions seen to keep. Returns how many are
-- kept.
--
-- 'advance': the tip that the store saw or '', the block to move to, the
-- time in milliseconds or '' for the clock of Redis, the size of a block in
-- bytes, '1' to empty the pool instead of changing it or '0', the height of
-- the block to move to or '', the transactions that are gone, those that
-- are confirmed, the outputs that are spent, each followed by the
-- transaction of the block that spends it and the height of that block in
-- four bytes, big-endian, the transactions of the abandoned blocks that the
-- store has as replaced, each followed in the same way, the number of
-- transactions that come back, those transactions, and the blocks that the
-- tip moves through, oldest first, or '', the token of the move of the
-- cache that the store began (xpub.lua) or ''. Returns {'moved'}, {'stays'} or {'raced'}, also when the token is
-- not that of the move of the cache under way.
--
-- The advance is the commit of a move of the tip: the pool, the tip and the
-- cache of extended public keys change in it together. The pieces of the
-- move (xpub.lua) wrote the chain's layer of the entries and marked them;
-- here the pool's layer changes with the pool, every marked entry gets a
-- new version, the marks go, and the cache is put at the new place of the
-- tip. The cache is emptied only when the pool is, when the store cannot
-- say what the abandoned blocks held, or when it was not at the tip.
--
-- What the new blocks replaced is kept: each transaction of the pool that
-- leaves because it, or an ancestor, spent an output that a transaction of
-- the blocks spends, with that transaction and the height of its block.
-- A transaction of the abandoned blocks that the store names as replaced
-- waits until a move confirms the transaction that replaced it, and is
-- kept then. What is kept is forgotten when the transaction that replaced
-- it comes back from an abandoned block; it, and what waits, are forgotten
-- 2016 blocks after their height, by the height of the block that the tip
-- moves to. Haskoin.Store.Fleet.Pool.advanceReplacing says the same.
--
-- The pool is emptied instead of changed in two cases. With '1|NAME' the
-- store of that name cannot say what the blocks between the two tips did,
-- because it does not have the old tip: only when it is the one store that
-- is live, and {'raced'} otherwise, since another store may know. With '2' the store has both
-- tips and the way between them is longer than a store goes through: that
-- is so for every store, so any of them may, or the tip would never move
-- again. Each time the pool is emptied, what the fleet has seen is
-- forgotten, since what it decided on is gone, and a counter goes up,
-- next to the tip, which tells the stores to fill the pool again.
--
-- What the pool holds for each address, kept in step with its transactions:
-- its transactions and the outputs paid to it, both ordered newest first,
-- the outputs of it that are spent, and the sums of all that, which are the
-- summary of Haskoin.Store.Database.Types.MemSummary. It also holds every
-- transaction by the time it entered a mempool, which is the list of the
-- mempool, and by its id alone, to find those whose id starts with some
-- bytes.
--
-- What the pool holds for the tokens of each category: its transactions
-- and the outputs that hold it, both newest first, with the value and
-- script of each output, and three counts: its transactions, the outputs
-- that are free and those that are spent ('tokens'). For each address and
-- category, which the store reads as the address's token balance: the
-- amount, non-fungible tokens and outputs that are free, the same that are
-- spent, the amount that the pool pays to the address, and the number of
-- transactions; the categories of each address and the addresses of each
-- category, to find them. A transaction moves the tokens of a category,
-- and those of an address, when it makes or uses up an output that holds
-- some, as for a block (Haskoin.Store.Logic).
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
  tsum = prefix .. 'pool:tsum:',
  acat = prefix .. 'pool:acat:',
  chold = prefix .. 'pool:chold:',
  ctx = prefix .. 'pool:ctx:',
  cout = prefix .. 'pool:cout:',
  cval = prefix .. 'pool:cval:',
  csum = prefix .. 'pool:csum:',
  rep = prefix .. 'pool:rep',
  repby = prefix .. 'pool:repby:',
  repat = prefix .. 'pool:repat',
  lostval = prefix .. 'pool:lost',
  lostby = prefix .. 'pool:lostby:',
  lostat = prefix .. 'pool:lostat',
  seen = prefix .. 'fleet:seen',
  restarts = prefix .. 'fleet:restarts',
  of = prefix .. 'fx:of:',
  entry = prefix .. 'fx:e:',
  xtip = prefix .. 'fx:tip',
  moving = prefix .. 'fx:moving',
  marked = prefix .. 'fx:marked'
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

-- The transactions that the fleet has seen, by the time in milliseconds at
-- which they were first seen: each that entered the pool, that an offer
-- found in the pool or in conflict with it, and that left the pool
-- unconfirmed. No store fetches one of them when a peer announces it. Only
-- what the pool has decided on is seen: a transaction that a store
-- received and has not offered yet is not, so that if the store stops
-- before the offer, another fetches it. Not under 'pool:', so that it
-- outlives a pool that is started again, as the transactions do: peers do
-- not announce them again. 'trim' keeps the newest of them.
local seenAt
local function seen(t)
  if not seenAt then
    local time = redis.call('TIME')
    seenAt = time[1] * 1000 + math.floor(time[2] / 1000)
  end
  redis.call('ZADD', key.seen, 'NX', seenAt, t)
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
    entries[i] = {ARGV[at], ARGV[at + 1], ARGV[at + 2], ARGV[at + 3], ARGV[at + 4], ARGV[at + 5]}
    at = at + 6
  end
  tx.entries = entries
  return tx, at
end

-- whether a member of a list of an entry is in the pool's layer, with a
-- score below zero: what the pool puts in or takes out of an entry is never
-- what the chain's layer has, which a move of the tip writes ahead of the
-- pool (xpub.lua)
local function pooled(z, m)
  local s = redis.call('ZSCORE', z, m)
  return s and tonumber(s) < 0
end

-- add to a field of the summary of an address; the + 0 turns -0, which taking
-- out an output of value 0 gives, into 0: Redis reads '-0' as no integer, and
-- the error would stop the whole script half way
local function addTo(k, field, by)
  redis.call('HINCRBY', k, field, string.format('%.0f', by + 0))
end

local function add(a, field, by)
  addTo(key.asum .. a, field, by)
end

-- an amount of tokens in two parts, the digits above the last nine and the
-- last nine, each of which a number of Lua holds exactly: an amount can be
-- as large as 2^63 - 1, and Lua's numbers are exact only to 2^53. Each
-- part is added up apart in Redis, where neither sum comes near what an
-- integer of Redis holds, and the store joins them again.
local function parts(s)
  local n = #s
  if n <= 9 then
    return 0, tonumber(s)
  end
  return tonumber(string.sub(s, 1, n - 9)), tonumber(string.sub(s, n - 8))
end

-- What the pool does to the tokens of an output, by category and by
-- address. An output is 'free' while the pool makes it and nothing in the
-- pool uses it up, and 'spent' while the pool uses it up and the chain
-- made it. An output that the pool both makes and uses up is neither. So
-- both sums are the tokens of a set of outputs that exist together, which
-- a sum that counted an output each time it was paid and spent would not
-- be: a supply of 2^63 - 1 that moves twice in the pool would go past what
-- Redis counts, and the error would stop the script half way.
local function tokens(c, a, which, sign, hi, lo, nft)
  addTo(key.csum .. c, which, sign)
  if a ~= '' then
    local k = key.tsum .. c .. a
    addTo(k, which .. 'Hi', sign * hi)
    addTo(k, which .. 'Lo', sign * lo)
    addTo(k, which .. 'Nfts', sign * nft)
    addTo(k, which .. 'Outputs', sign)
  end
end

-- put the entries of a transaction in, with sign 1, or take them out, with
-- sign -1
local function account(t, time, entries, sign)
  local touched = {}
  -- the categories of tokens that the transaction moves, and the
  -- addresses with each
  local cats, held = {}, {}
  for _, e in ipairs(entries) do
    local kind, a, o, v = e[1], e[2], e[3], tonumber(e[4])
    if kind == 'm' or kind == 'M' or kind == 'u' or kind == 'U' then
      local c = e[6]
      local hi, lo = parts(e[4])
      local nft = 0
      if kind == 'M' or kind == 'U' then
        nft = 1
      end
      cats[c] = true
      if a ~= '' then
        held[c .. a] = {c, a}
      end
      if kind == 'm' or kind == 'M' then
        if sign > 0 then
          redis.call('ZADD', key.cout .. c, -time, o)
          redis.call('HSET', key.cval .. c, o, e[5])
        else
          redis.call('ZREM', key.cout .. c, o)
          redis.call('HDEL', key.cval .. c, o)
        end
        -- an output that the pool uses up was spent from the chain until
        -- the pool made it, and is spent from the chain again once the
        -- pool no longer makes it
        if redis.call('HEXISTS', key.spender, o) == 1 then
          tokens(c, a, 'spent', -sign, hi, lo, nft)
        else
          tokens(c, a, 'free', sign, hi, lo, nft)
        end
        if a ~= '' then
          addTo(key.tsum .. c .. a, 'receivedHi', sign * hi)
          addTo(key.tsum .. c .. a, 'receivedLo', sign * lo)
        end
      elseif member(string.sub(o, 1, tsize)) then
        tokens(c, a, 'free', -sign, hi, lo, nft)
      else
        tokens(c, a, 'spent', sign, hi, lo, nft)
      end
    else
      touched[a] = true
    end
    -- the cached keys that have the address
    local watching = {}
    if kind == 't' or kind == 'o' then
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
          redis.call('ZADD', key.entry .. id .. ':txs', 'NX', -time, t)
        elseif pooled(key.entry .. id .. ':txs', t) then
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
    elseif kind == 'o' then
      if sign > 0 then
        redis.call('ZADD', key.aout .. a, -time, o)
        redis.call('HSET', key.aval .. a, o, e[4] .. ' ' .. e[5])
      else
        redis.call('ZREM', key.aout .. a, o)
        redis.call('HDEL', key.aval .. a, o)
      end
      for _, id in ipairs(watching) do
        if sign > 0 then
          if redis.call('ZADD', key.entry .. id .. ':outs', 'NX', -time, o) == 1 then
            redis.call('HSET', key.entry .. id .. ':oval', o, e[4] .. ' ' .. #a .. ' ' .. a .. e[5])
          end
        elseif pooled(key.entry .. id .. ':outs', o) then
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
  -- every entry with an address that the transaction touches has a new
  -- version, so that a request that read it before reads it again
  local ver = nil
  for a, _ in pairs(touched) do
    for _, id in ipairs(redis.call('SMEMBERS', key.of .. a)) do
      ver = ver or redis.call('INCR', prefix .. 'fleet:xver')
      redis.call('HSET', key.entry .. id .. ':meta', 'ver', ver)
    end
  end
  -- the transaction in the lists of the categories that it moves, and in
  -- the count of each address that it moves tokens of
  for c, _ in pairs(cats) do
    if sign > 0 then
      redis.call('ZADD', key.ctx .. c, -time, t)
    else
      redis.call('ZREM', key.ctx .. c, t)
    end
    addTo(key.csum .. c, 'txs', sign)
  end
  for _, h in pairs(held) do
    local c, a = h[1], h[2]
    addTo(key.tsum .. c .. a, 'txs', sign)
    if sign > 0 then
      redis.call('SADD', key.acat .. a, c)
      redis.call('SADD', key.chold .. c, a)
    end
  end
  -- an address that no transaction touches has no summary, nor has a
  -- category or an address of one that no transaction moves
  if sign < 0 then
    for a, _ in pairs(touched) do
      if tonumber(redis.call('HGET', key.asum .. a, 'txs')) == 0 then
        redis.call('DEL', key.asum .. a)
      end
    end
    for c, _ in pairs(cats) do
      if tonumber(redis.call('HGET', key.csum .. c, 'txs')) == 0 then
        redis.call('DEL', key.csum .. c)
      end
    end
    for _, h in pairs(held) do
      local c, a = h[1], h[2]
      if tonumber(redis.call('HGET', key.tsum .. c .. a, 'txs')) == 0 then
        redis.call('DEL', key.tsum .. c .. a)
        redis.call('SREM', key.acat .. a, c)
        redis.call('SREM', key.chold .. c, a)
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
    seen(t)
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
  seen(t)
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

-- expire: what has been in the pool since before a time, so many at most,
-- chosen and withdrawn in this one call. Chosen by the caller, in a call
-- before, a transaction could leave and come back in between with a new
-- time, and be withdrawn for the time that it had before.
if what == 'expire' then
  return withdraw(redis.call('ZRANGEBYSCORE', key.time, 0, ARGV[4], 'LIMIT', 0, tonumber(ARGV[5])))
end

-- trimmed only when the set has grown a hundredth past its bound, so that
-- most calls cost a ZCARD
if what == 'trim' then
  local most, n = tonumber(ARGV[4]), redis.call('ZCARD', key.seen)
  if n > most + math.max(1, math.floor(most / 100)) then
    redis.call('ZREMRANGEBYRANK', key.seen, 0, n - most - 1)
    n = most
  end
  return n
end

-- forget that these transactions were replaced
local function unreplaced(ts)
  for _, t in ipairs(ts) do
    local value = redis.call('HGET', key.rep, t)
    if value then
      redis.call('SREM', key.repby .. string.sub(value, 1, tsize), t)
      redis.call('HDEL', key.rep, t)
      redis.call('ZREM', key.repat, t)
    end
  end
end

-- keep that a transaction was replaced: by a transaction, which comes
-- first in the value, at a height, which follows in four bytes. What was
-- kept of it before goes, with its place among what its winner replaced.
local function replaced(t, value)
  local by, h = string.sub(value, 1, tsize), string.sub(value, tsize + 1, tsize + 4)
  local b1, b2, b3, b4 = string.byte(h, 1, 4)
  unreplaced({t})
  redis.call('HSET', key.rep, t, value)
  redis.call('SADD', key.repby .. by, t)
  redis.call('ZADD', key.repat, ((b1 * 256 + b2) * 256 + b3) * 256 + b4, t)
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
    seen(t)
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
    seen(t)
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

-- the cache: the place of the tip before this move and after it, as
-- xpub.lua has them, and the move of the cache that the store began
local moves = tonumber(redis.call('GET', key.moves) or '0')
local before, after = moves .. '@' .. tip, (moves + 1) .. '@' .. target
local function token(at)
  return ARGV[at + 1] or ''
end
local function settle(wipe)
  local at = redis.call('GET', key.xtip)
  if wipe or (at and at ~= before) then
    for _, k in ipairs(redis.call('KEYS', prefix .. 'fx:*')) do
      redis.call('DEL', k)
    end
  else
    local ids = redis.call('SMEMBERS', key.marked)
    if #ids > 0 then
      local ver = redis.call('INCR', prefix .. 'fleet:xver')
      for _, id in ipairs(ids) do
        if redis.call('EXISTS', key.entry .. id .. ':meta') == 1 then
          redis.call('HSET', key.entry .. id .. ':meta', 'ver', ver)
        end
      end
    end
    redis.call('DEL', key.marked, key.moving, prefix .. 'fx:beat')
  end
  redis.call('SET', key.xtip, after)
end

-- a store that does not know the way from the old tip starts the pool
-- again: if it is alone, or if the way is too long for any store
-- '1' comes with the name of the store, as '1|NAME': it may empty the pool
-- only if it is the one store that is live, not merely if one store is.
-- A store whose report was refused is not live, and another that is live
-- may know the way
local how, caller = string.match(ARGV[8], '^([012])|?(.*)$')
if how == '1' or how == '2' then
  if how == '1' and (#live ~= 1 or live[1] ~= caller) then
    return {'raced'}
  end
  for _, k in ipairs(redis.call('KEYS', prefix .. 'pool:*')) do
    redis.call('DEL', k)
  end
  -- what the pool decided on is gone: the stores fetch it again when it
  -- is announced, and fill the pool again when the counter goes up. The
  -- first tip of a fleet, or the first after Redis lost its data, empties
  -- nothing, and the loss is a refill of its own (noteEpoch).
  redis.call('DEL', key.seen)
  if tip ~= '' then
    redis.call('INCR', key.restarts)
  end
  -- the entries have the pool's layer of the pool that is gone
  settle(true)
  redis.call('SET', key.tip, target)
  redis.call('INCR', key.moves)
  emit('b', target)
  return {'moved'}
end

-- the move of the cache must be the one that the store began, and nobody
-- aborted it; with none begun, none may be under way
local at = 15
for _ = 1, tonumber(ARGV[14]) do
  local _, next = read(at)
  at = next
end
local mine = token(at)
local current = redis.call('GET', key.moving)
if (current and current ~= mine) or (not current and mine ~= '') then
  return {'raced'}
end

-- the abandoned blocks: what can come back does, and what spends the rest
-- leaves. What a transaction that comes back replaced is not replaced any
-- more.
at = 15
for _ = 1, tonumber(ARGV[14]) do
  local tx
  tx, at = read(at)
  unreplaced(redis.call('SMEMBERS', key.repby .. tx.id))
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
-- what the store has as replaced of the abandoned blocks waits for the
-- transaction that replaced it
for _, item in ipairs(split(ARGV[13], 2 * tsize + 4)) do
  local t, value = string.sub(item, 1, tsize), string.sub(item, tsize + 1)
  local old = redis.call('HGET', key.lostval, t)
  if old then
    redis.call('SREM', key.lostby .. string.sub(old, 1, tsize), t)
  end
  local b1, b2, b3, b4 = string.byte(string.sub(value, tsize + 1, tsize + 4), 1, 4)
  redis.call('HSET', key.lostval, t, value)
  redis.call('SADD', key.lostby .. string.sub(value, 1, tsize), t)
  redis.call('ZADD', key.lostat, ((b1 * 256 + b2) * 256 + b3) * 256 + b4, t)
end
local orphaned = {}
for _, t in ipairs(split(ARGV[10], tsize)) do
  for _, kid in ipairs(redis.call('SMEMBERS', key.kids .. t)) do
    orphaned[#orphaned + 1] = kid
  end
end
withdraw(orphaned)

-- the new blocks: what they confirm leaves alone, and what spends what they
-- spend leaves with all that spends it, and is kept as replaced
for _, t in ipairs(split(ARGV[11], tsize)) do
  remove(t, true)
end
-- what waited for a transaction that they confirm was replaced
for _, t in ipairs(split(ARGV[11], tsize)) do
  for _, waiting in ipairs(redis.call('SMEMBERS', key.lostby .. t)) do
    replaced(waiting, redis.call('HGET', key.lostval, waiting))
    redis.call('HDEL', key.lostval, waiting)
    redis.call('ZREM', key.lostat, waiting)
  end
  redis.call('DEL', key.lostby .. t)
end
for _, item in ipairs(split(ARGV[12], osize + tsize + 4)) do
  local s = redis.call('HGET', key.spender, string.sub(item, 1, osize))
  if s then
    for _, t in ipairs(withdraw({s})) do
      replaced(t, string.sub(item, osize + 1))
    end
  end
end
-- forget what was replaced, or waits, 2016 blocks before the block of the
-- tip
if ARGV[9] ~= '' and tonumber(ARGV[9]) >= 2016 then
  local limit = tonumber(ARGV[9]) - 2016
  unreplaced(redis.call('ZRANGEBYSCORE', key.repat, '-inf', limit))
  for _, t in ipairs(redis.call('ZRANGEBYSCORE', key.lostat, '-inf', limit)) do
    redis.call('SREM', key.lostby .. string.sub(redis.call('HGET', key.lostval, t), 1, tsize), t)
    redis.call('HDEL', key.lostval, t)
    redis.call('ZREM', key.lostat, t)
  end
end

settle(false)
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
