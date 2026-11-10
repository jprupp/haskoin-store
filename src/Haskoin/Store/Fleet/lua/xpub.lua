-- The cache of extended public keys of a fleet: xpubScript in
-- Haskoin.Store.Fleet.XPub, which says what an entry is and why. pool.lua
-- keeps the part of each entry that comes from the shared mempool.
--
-- ARGV[1] is what to do, ARGV[2] the prefix of the keys, and ARGV[3] the
-- size of a transaction id in bytes. An output is the id of the transaction
-- that made it and four more bytes. A position in a list is a score, as
-- Haskoin.Store.Fleet.XPub.score gives it: lists are ordered by score and,
-- within one score, by the bytes of the member. What an entry keeps for an
-- output is its amount, a space, the length of its address, a space, the
-- address, and the script.
--
-- The reads take the fleet tip that the store answers from, the id of a
-- key, and the number of its addresses in each of its two chains as the
-- caller has them. They answer {'raced'} if the fleet tip is another block,
-- {'miss'} if the cache is not where the tip is, if it does not have the key,
-- or if it has it with other addresses, and {'ok', ...} otherwise.
--
-- 'bals': the tip, the id, and the time in milliseconds, or '' not to note
-- that the key was used. The numbers of addresses are not given, and not
-- checked. For each address of the entry: the address, its path followed
-- by its balance in the chain if it has one, and its seven sums in the
-- pool separated by spaces or ''. A path is five bytes: the chain of the
-- key that the address is in, and its number there.
--
-- 'txs', 'outs': the tip, the id, the two numbers, where to start, an
-- offset and how many, 0 for all. Where to start is 'top' and two '';
-- 'score' with a score and ''; or 'at' with a score and the bytes that the
-- members of that score are to be no smaller than. 'txs' gives a
-- transaction and a score for each; 'outs' an output, a score and what is
-- kept for it, leaving out the outputs that the pool spends.
--
-- 'count': the tip, the id and the two numbers. The number of transactions.
--
-- 'have': the id. The two numbers of the entry, whatever block the cache is
-- at: {n0, n1}.
--
-- 'build': the tip, the id, the two numbers that the entry is to have
-- before, the time in milliseconds, the number of addresses to add, and for
-- each: the address, its path, its balance in the chain or '', the number
-- of its transactions in the chain, a transaction and a score for each, the
-- number of its unspent outputs in the chain, and an output, a score, an
-- amount and a script for each. What the pool has for the address is added
-- from the pool. Returns {'ok'}; {'raced'} if the fleet tip is another
-- block; {'behind'} if the cache is at another block; {'changed'} if the
-- entry does not have those two numbers of addresses.
--
-- 'drop': the id. Deletes the entry.
--
-- 'tracked': addresses. A string with '1' for each address that an entry
-- has and '0' for the others.
--
-- 'where': nothing. The block that the cache is at, if it is at the fleet
-- tip as it stands, and '' otherwise.
--
-- 'advance': the block that the cache is taken to be at or '', the block to
-- bring it to, '1' to empty it or '0', the number of addresses that the
-- blocks in between changed, and for each: the address, its balance in
-- the chain at the new block or '', the number of transactions that the
-- blocks added to it, a transaction and a score for each, the number of
-- outputs that they paid to it, an output, a score, an amount and a script
-- for each, the number of its outputs that they spent, and those outputs.
-- Returns {'moved'}; {'stays'} if the cache is at the new block already;
-- {'raced'} if the fleet tip is not the new block; and {'emptied'} if it
-- had entries and was asked to empty them, or was not where the tip was
-- one move ago, in which case what happened since cannot be told.
--
-- 'size': nothing. The number of entries, and the number of addresses that
-- they have between them: {entries, addresses}.
--
-- 'evict': how many entries to keep. Deletes the least recently used ones
-- past that many, and says how many it deleted.
local what, prefix, tsize = ARGV[1], ARGV[2], tonumber(ARGV[3])
local key = {
  fleet = prefix .. 'fleet:tip',
  tip = prefix .. 'fx:tip',
  lru = prefix .. 'fx:lru',
  addrs = prefix .. 'fx:addrs',
  of = prefix .. 'fx:of:',
  entry = prefix .. 'fx:e:',
  spender = prefix .. 'pool:spender',
  atx = prefix .. 'pool:atx:',
  aout = prefix .. 'pool:aout:',
  aval = prefix .. 'pool:aval:',
  asum = prefix .. 'pool:asum:'
}

-- whether one string of bytes sorts before another, as Redis sorts the
-- members of a sorted set; the comparison of Lua goes by the locale
local function before(a, b)
  local n = math.min(#a, #b)
  for i = 1, n do
    local x, y = string.byte(a, i), string.byte(b, i)
    if x ~= y then
      return x < y
    end
  end
  return #a < #b
end

local function kept(a, amount, script)
  return amount .. ' ' .. #a .. ' ' .. a .. script
end

local function drop(id)
  local e = key.entry .. id .. ':'
  local had = redis.call('HKEYS', e .. 'addrs')
  for _, a in ipairs(had) do
    redis.call('SREM', key.of .. a, id)
  end
  if #had > 0 then
    redis.call('DECRBY', key.addrs, #had)
  end
  redis.call('DEL', e .. 'addrs', e .. 'meta', e .. 'txs', e .. 'outs', e .. 'oval')
  redis.call('ZREM', key.lru, id)
end

local function empty()
  for _, id in ipairs(redis.call('ZRANGE', key.lru, 0, -1)) do
    drop(id)
  end
  -- whatever a script that failed half way left behind
  for _, k in ipairs(redis.call('KEYS', prefix .. 'fx:*')) do
    redis.call('DEL', k)
  end
end

local function numbers(e)
  local n = redis.call('HMGET', e .. 'meta', 'n0', 'n1')
  return tonumber(n[1] or '0'), tonumber(n[2] or '0')
end

if what == 'have' then
  local n0, n1 = numbers(key.entry .. ARGV[4] .. ':')
  return {n0, n1}
elseif what == 'drop' then
  drop(ARGV[4])
  return {'ok'}
elseif what == 'tracked' then
  local flags = {}
  for i = 4, #ARGV do
    flags[#flags + 1] = tostring(redis.call('EXISTS', key.of .. ARGV[i]))
  end
  return table.concat(flags)
elseif what == 'size' then
  return {redis.call('ZCARD', key.lru), tonumber(redis.call('GET', key.addrs) or '0')}
elseif what == 'evict' then
  local over = redis.call('ZCARD', key.lru) - tonumber(ARGV[4])
  if over <= 0 then
    return 0
  end
  for _, id in ipairs(redis.call('ZRANGE', key.lru, 0, over - 1)) do
    drop(id)
  end
  return over
end

local fleet = redis.call('GET', key.fleet) or ''

-- Where the cache is, is a move of the fleet tip and the block that it went
-- to. A block alone would not do: the tip can leave a block and come back
-- to it, as when a block is reverted and mined again, and a cache that was
-- not brought along would find itself at the tip, with entries that the
-- pool has taken the transactions of that block out of on the way.
local moves = tonumber(redis.call('GET', prefix .. 'fleet:moves') or '0')
local function place(n, block)
  return n .. '@' .. block
end
local here = place(moves, fleet)

if what == 'where' then
  if (redis.call('GET', key.tip) or '') == here then
    return fleet
  end
  return ''
end

if what == 'advance' then
  local from, to = ARGV[4], ARGV[5]
  if fleet ~= to then
    return {'raced'}
  end
  local at = redis.call('GET', key.tip) or ''
  if at == here then
    return {'stays'}
  end
  -- it can be brought along only from the move before this one
  local held = redis.call('ZCARD', key.lru) > 0
  if held and (ARGV[6] == '1' or at ~= place(moves - 1, from)) then
    empty()
    redis.call('SET', key.tip, here)
    return {'emptied'}
  end
  local changed = tonumber(ARGV[7])
  local i = 8
  for _ = 1, changed do
    local a, bal = ARGV[i], ARGV[i + 1]
    local ids = redis.call('SMEMBERS', key.of .. a)
    local txs, outs, spent = {}, {}, {}
    local n = tonumber(ARGV[i + 2])
    i = i + 3
    for _ = 1, n do
      txs[#txs + 1] = {ARGV[i], ARGV[i + 1]}
      i = i + 2
    end
    n = tonumber(ARGV[i])
    i = i + 1
    for _ = 1, n do
      outs[#outs + 1] = {ARGV[i], ARGV[i + 1], kept(a, ARGV[i + 2], ARGV[i + 3])}
      i = i + 4
    end
    n = tonumber(ARGV[i])
    i = i + 1
    for _ = 1, n do
      spent[#spent + 1] = ARGV[i]
      i = i + 1
    end
    for _, id in ipairs(ids) do
      local e = key.entry .. id .. ':'
      local row = redis.call('HGET', e .. 'addrs', a)
      if row then
        redis.call('HSET', e .. 'addrs', a, string.sub(row, 1, 5) .. bal)
      end
      for _, t in ipairs(txs) do
        redis.call('ZADD', e .. 'txs', t[2], t[1])
      end
      for _, o in ipairs(outs) do
        redis.call('ZADD', e .. 'outs', o[2], o[1])
        redis.call('HSET', e .. 'oval', o[1], o[3])
      end
      for _, o in ipairs(spent) do
        redis.call('ZREM', e .. 'outs', o)
        redis.call('HDEL', e .. 'oval', o)
      end
    end
  end
  redis.call('SET', key.tip, here)
  return {'moved'}
end

-- everything else is for a store that answers from the fleet tip
local tip, id = ARGV[4], ARGV[5]
local e = key.entry .. id .. ':'
if fleet ~= tip then
  return {'raced'}
end

if what == 'build' then
  local at = redis.call('GET', key.tip)
  if not at then
    -- nothing is cached yet: the cache starts here
    redis.call('SET', key.tip, here)
  elseif at ~= here then
    return {'behind'}
  end
  local n0, n1 = numbers(e)
  if n0 ~= tonumber(ARGV[6]) or n1 ~= tonumber(ARGV[7]) then
    return {'changed'}
  end
  redis.call('ZADD', key.lru, ARGV[8], id)
  local i = 10
  for _ = 1, tonumber(ARGV[9]) do
    local a, path, bal = ARGV[i], ARGV[i + 1], ARGV[i + 2]
    redis.call('HSET', e .. 'addrs', a, path .. bal)
    redis.call('SADD', key.of .. a, id)
    if string.byte(path, 1) == 0 then
      n0 = n0 + 1
    else
      n1 = n1 + 1
    end
    local n = tonumber(ARGV[i + 3])
    i = i + 4
    for _ = 1, n do
      redis.call('ZADD', e .. 'txs', ARGV[i + 1], ARGV[i])
      i = i + 2
    end
    n = tonumber(ARGV[i])
    i = i + 1
    for _ = 1, n do
      redis.call('ZADD', e .. 'outs', ARGV[i + 1], ARGV[i])
      redis.call('HSET', e .. 'oval', ARGV[i], kept(a, ARGV[i + 2], ARGV[i + 3]))
      i = i + 4
    end
    -- what the pool has for the address, which pool.lua keeps up from now
    local rows = redis.call('ZRANGE', key.atx .. a, 0, -1, 'WITHSCORES')
    for j = 1, #rows, 2 do
      redis.call('ZADD', e .. 'txs', rows[j + 1], rows[j])
    end
    rows = redis.call('ZRANGE', key.aout .. a, 0, -1, 'WITHSCORES')
    for j = 1, #rows, 2 do
      local o = rows[j]
      local value = redis.call('HGET', key.aval .. a, o)
      local space = string.find(value, ' ', 1, true)
      redis.call('ZADD', e .. 'outs', rows[j + 1], o)
      redis.call('HSET', e .. 'oval', o, kept(a, string.sub(value, 1, space - 1), string.sub(value, space + 1)))
    end
  end
  redis.call('HSET', e .. 'meta', 'n0', n0, 'n1', n1)
  redis.call('INCRBY', key.addrs, tonumber(ARGV[9]))
  return {'ok'}
end

if (redis.call('GET', key.tip) or '') ~= here then
  return {'miss'}
end
local answer = {'ok'}

if what == 'bals' then
  local addrs = redis.call('HGETALL', e .. 'addrs')
  if #addrs == 0 then
    return {'miss'}
  end
  if ARGV[6] ~= '' then
    redis.call('ZADD', key.lru, ARGV[6], id)
  end
  -- the pool has a summary for an address when one of its transactions
  -- touches it, and those transactions are in the entry, with scores
  -- below zero: most keys have none, and are spared a read for each
  -- address
  local touched = redis.call('ZCOUNT', e .. 'txs', '-inf', '(0') > 0
  for i = 1, #addrs, 2 do
    local a = addrs[i]
    answer[#answer + 1] = a
    answer[#answer + 1] = addrs[i + 1]
    local sums = ''
    if touched then
      local s = redis.call('HMGET', key.asum .. a,
        'txs', 'paid', 'paidCount', 'spent', 'spentCount', 'both', 'bothCount')
      if s[1] then
        for j = 1, 7 do
          s[j] = s[j] or '0'
        end
        sums = table.concat(s, ' ')
      end
    end
    answer[#answer + 1] = sums
  end
  return answer
end

local n0, n1 = numbers(e)
if n0 + n1 == 0 or n0 ~= tonumber(ARGV[6]) or n1 ~= tonumber(ARGV[7]) then
  return {'miss'}
end

if what == 'count' then
  return {'ok', tostring(redis.call('ZCARD', e .. 'txs'))}
end

-- 'txs' and 'outs': the place in the list where the page starts
local zkey = e .. what
local mode, score, bytes = ARGV[8], ARGV[9], ARGV[10]
local offset, many = tonumber(ARGV[11]), tonumber(ARGV[12])
local first = 0
if mode ~= 'top' then
  first = redis.call('ZCOUNT', zkey, '-inf', '(' .. score)
  if mode == 'at' then
    for _, m in ipairs(redis.call('ZRANGEBYSCORE', zkey, score, score)) do
      if before(m, bytes) then
        first = first + 1
      end
    end
  end
end

if what == 'txs' then
  local last = -1
  if many > 0 then
    last = first + offset + many - 1
  end
  local rows = redis.call('ZRANGE', zkey, first + offset, last, 'WITHSCORES')
  for _, row in ipairs(rows) do
    answer[#answer + 1] = row
  end
  return answer
end

-- 'outs': those that the pool spends are left out, before the offset
local found, skipped, size = 0, 0, 64
while many == 0 or found < many do
  local rows = redis.call('ZRANGE', zkey, first, first + size - 1, 'WITHSCORES')
  if #rows == 0 then
    break
  end
  for j = 1, #rows, 2 do
    local o = rows[j]
    if redis.call('HEXISTS', key.spender, o) == 0 then
      if skipped < offset then
        skipped = skipped + 1
      elseif many == 0 or found < many then
        found = found + 1
        answer[#answer + 1] = o
        answer[#answer + 1] = rows[j + 1]
        answer[#answer + 1] = redis.call('HGET', e .. 'oval', o)
      end
    end
  end
  first = first + size
  size = math.min(4096, size * 4)
end
return answer
