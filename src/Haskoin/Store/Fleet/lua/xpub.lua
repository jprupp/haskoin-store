-- The reads of the cache of extended public keys of a fleet: xpubScript
-- in Haskoin.Store.Fleet.XPub, which says what an entry is and why, after
-- greedy.lua. Nothing here writes an entry: building, moving with the tip,
-- dropping and evicting are decisions made in Haskell
-- (Haskoin.Store.Fleet.XPub.Decide), and the pool keeps the part of each
-- entry that comes from the shared mempool
-- (Haskoin.Store.Fleet.Pool.Decide). A script that failed among the
-- thousands of rows of a build left an entry half written. The one write
-- here is the note that a key was used, a single command.
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
-- or if it has it with other addresses, {'marked'} while a move of the tip
-- is writing the entry in pieces, and {'ok', version, ...}
-- otherwise. A reader that gets 'marked' waits a moment and asks again: it
-- never sees an entry half moved.
--
-- 'bals': the tip, the id, and '1' to note that the key was used, or ''
-- not to. The numbers of addresses are not given, and not
-- checked. For each address of the entry: the address, its path followed
-- by its balance in the chain if it has one, and its seven sums in the
-- pool separated by spaces or ''. A path is five bytes: the chain of the
-- key that the address is in, and its number there. The entry may have
-- more addresses than a scan of the key reads: those after them are its
-- lookahead, which Haskoin.Store.Fleet.XPub.window accepts when unused.
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
-- 'tracked': addresses. A string with '1' for each address that an entry
-- has and '0' for the others.
--
-- An entry that a move of the tip is writing in pieces is marked, and a
-- read of it answers {'marked'} until the move is committed. A move whose
-- store wrote nothing for ten seconds is no move any more: a read of an
-- entry that it marked is a miss, and the build that follows a miss ends
-- the move and drops what it wrote.
--
-- 'size': nothing. The number of entries, and the number of addresses that
-- they have between them: {entries, addresses}.
local what, prefix, tsize = ARGV[1], ARGV[2], tonumber(ARGV[3])
local key = {
  fleet = prefix .. 'fleet:tip',
  tip = prefix .. 'fx:tip',
  prio = prefix .. 'fx:prio',
  level = prefix .. 'fx:level',
  addrs = prefix .. 'fx:addrs',
  of = prefix .. 'fx:of:',
  entry = prefix .. 'fx:e:',
  moving = prefix .. 'fx:moving',
  beat = prefix .. 'fx:beat',
  marked = prefix .. 'fx:marked',
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

-- the number of entries
local function entries()
  return redis.call('ZCARD', key.prio)
end

local function now()
  local t = redis.call('TIME')
  return t[1] * 1000 + math.floor(t[2] / 1000)
end

-- the token of the move of the tip under way, if there is one; a move
-- whose store said nothing for ten seconds is none
local function moving()
  local token = redis.call('GET', key.moving)
  if not token then
    return nil
  end
  if now() - tonumber(redis.call('GET', key.beat) or '0') > 10000 then
    return nil
  end
  return token
end

local function numbers(e)
  local n = redis.call('HMGET', e .. 'meta', 'n0', 'n1')
  return tonumber(n[1] or '0'), tonumber(n[2] or '0')
end

if what == 'have' then
  local n0, n1 = numbers(key.entry .. ARGV[4] .. ':')
  return {n0, n1}
elseif what == 'tracked' then
  local flags = {}
  for i = 4, #ARGV do
    flags[#flags + 1] = tostring(redis.call('EXISTS', key.of .. ARGV[i]))
  end
  return table.concat(flags)
elseif what == 'size' then
  return {entries(), tonumber(redis.call('GET', key.addrs) or '0')}
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

-- everything else is for a store that answers from the fleet tip
local tip, id = ARGV[4], ARGV[5]
local e = key.entry .. id .. ':'
if fleet ~= tip then
  return {'raced'}
end

if (redis.call('GET', key.tip) or '') ~= here then
  return {'miss'}
end
-- an entry that a move of the tip is writing is read when it is done
if redis.call('SISMEMBER', key.marked, id) == 1 then
  if moving() then
    return {'marked'}
  end
  return {'miss'}
end
-- every read gives the version of the entry, which each change stamps
local answer = {'ok', redis.call('HGET', e .. 'meta', 'ver') or '0'}

if what == 'bals' then
  local addrs = redis.call('HGETALL', e .. 'addrs')
  if #addrs == 0 then
    return {'miss'}
  end
  if ARGV[6] ~= '' then
    greedy_use(key.prio, key.level, id, redis.call('HGET', e .. 'meta', 'cost'), false)
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
  answer[#answer + 1] = tostring(redis.call('ZCARD', e .. 'txs'))
  return answer
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
