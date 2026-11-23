-- What a store reads from the shared mempool of its fleet: readScript in
-- Haskoin.Store.Fleet.Redis. Haskoin.Store.Fleet.Pool.Decide writes what
-- this reads.
--
-- ARGV[1] is what to read, ARGV[2] the prefix of the keys, ARGV[3] the size
-- of a transaction id in bytes, and ARGV[4] the fleet tip that the store
-- answers from, or ''. Every answer is {'raced'} if the tip is another one:
-- the pool goes with the tip, and laid over the chain of another block it
-- would give a transaction twice or not at all. Otherwise it is {'ok', ...}.
--
-- A move of the tip marks what its blocks took out of the pool, and the
-- transactions leave after it, a few at a time (pool:leaving, and takeOut
-- in Haskoin.Store.Fleet.Pool.Decide). The chain has them already, at the
-- tip that the store answers from, and the pool still does. So a read that
-- would go by one of them answers {'leaving', ...} with those
-- transactions, and reads nothing: the store takes them out and reads
-- again. Which those are, for each read: what the pool keeps for an
-- address is written by the transactions at that address and by no
-- other, and the same for a category of tokens; a transaction and an
-- output are read with what spends them. A read that goes by none of
-- them reads the pool as it will be when they are all out. The list of
-- the pool alone is read without them.
--
-- 'sums': addresses. For each, its seven sums separated by spaces, in the
-- order of the summary, or '' if the pool has nothing for it.
--
-- 'txs': an address, the time and id of the transaction to start at or two
-- '', and how many. Transactions of the address from there, newest first:
-- an id and a time for each.
--
-- 'outs': an address, the time and output to start at or two '', and how
-- many. Outputs paid to the address from there, newest first: for each the
-- output, its time, its amount and script separated by a space, and '1' if
-- the pool spends it or '0'.
--
-- 'spent': an address and outputs. A string with '1' for each output of the
-- address that the pool spends and '0' for the others.
--
-- 'bodies': transactions, of the pool or not. For each: its body or '',
-- the number of its outputs that the pool spends, and for each of those
-- the output, the transaction that spends it and the number of its input
-- that does.
--
-- 'spenders': outputs. For each, the transaction that spends it and the
-- number of its input that does, or two ''.
--
-- 'page': an offset and how many, 0 for all. Transactions of the pool from
-- the newest, an id and a time for each.
--
-- 'ids': some bytes. The transactions whose id starts with them.
--
-- 'tokens': an address. For each category of tokens that the pool moves at
-- the address: the category, and its eleven sums separated by spaces: the
-- transactions; the amount in two parts, the digits above the last nine
-- and the last nine, the non-fungible tokens and the outputs, that are
-- free; the same that are spent; and the amount paid in two parts.
--
-- 'holders': a category. For each address at which the pool moves tokens
-- of it: the address and its sums, as 'tokens' gives them.
--
-- 'csum': a category. Its transactions, free outputs and spent outputs
-- separated by spaces, or '' if the pool does not move it.
--
-- 'ctxs': as 'txs', for a category of tokens.
--
-- 'couts': as 'outs', for the outputs that hold a category of tokens; the
-- flag says whether the pool uses the output up.
--
-- 'replaced': transactions. For each, the transaction of a block that
-- replaced it and the height of the block in four bytes, or ''.
local what, prefix, tsize = ARGV[1], ARGV[2], tonumber(ARGV[3])
local osize = tsize + 4
if (redis.call('GET', prefix .. 'fleet:tip') or '') ~= ARGV[4] then
  return {'raced'}
end
local answer = {'ok'}

local function split(s, n)
  local items = {}
  for i = 1, #s, n do
    items[#items + 1] = string.sub(s, i, i + n - 1)
  end
  return items
end

local leaving = prefix .. 'pool:leaving'
local some = redis.call('EXISTS', leaving) == 1
if some then
  local found, seen = {'leaving'}, {}
  local function note(t)
    if not seen[t] and redis.call('HEXISTS', leaving, t) == 1 then
      seen[t] = true
      found[#found + 1] = t
    end
  end
  -- the marked among the members of a sorted set, going through the
  -- smaller of the two
  local function among(zkey)
    if redis.call('ZCARD', zkey) <= redis.call('HLEN', leaving) then
      for _, t in ipairs(redis.call('ZRANGE', zkey, 0, -1)) do
        note(t)
      end
    else
      for _, t in ipairs(redis.call('HKEYS', leaving)) do
        if not seen[t] and redis.call('ZSCORE', zkey, t) then
          seen[t] = true
          found[#found + 1] = t
        end
      end
    end
  end
  if what == 'sums' then
    for i = 5, #ARGV do
      among(prefix .. 'pool:atx:' .. ARGV[i])
    end
  elseif what == 'txs' or what == 'outs' or what == 'spent' or what == 'tokens' then
    among(prefix .. 'pool:atx:' .. ARGV[5])
  elseif what == 'holders' or what == 'csum' or what == 'ctxs' or what == 'couts' then
    among(prefix .. 'pool:ctx:' .. ARGV[5])
  elseif what == 'bodies' then
    for _, t in ipairs(split(ARGV[5], tsize)) do
      note(t)
      for _, kid in ipairs(redis.call('SMEMBERS', prefix .. 'pool:kids:' .. t)) do
        note(kid)
      end
    end
  elseif what == 'spenders' then
    for _, o in ipairs(split(ARGV[5], osize)) do
      local s = redis.call('HGET', prefix .. 'pool:spender', o)
      if s then
        note(s)
      end
    end
  elseif what == 'ids' then
    for _, t in ipairs(redis.call('ZRANGEBYLEX', prefix .. 'pool:ids',
      '[' .. ARGV[5], '(' .. ARGV[5] .. string.rep(string.char(255), tsize))) do
      note(t)
    end
  elseif what == 'replaced' then
    for i = 5, #ARGV do
      note(ARGV[i])
    end
  end
  if #found > 1 then
    return found
  end
end

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

-- the position in a sorted set, ordered newest first, of the member with a
-- time and a name, or where it would be
local function position(zkey, time, member)
  if time == '' then
    return 0
  end
  local score = -tonumber(time)
  local at = redis.call('ZCOUNT', zkey, '-inf', '(' .. string.format('%.0f', score))
  for _, m in ipairs(redis.call('ZRANGEBYSCORE', zkey, score, score)) do
    if before(m, member) then
      at = at + 1
    end
  end
  return at
end

if what == 'sums' then
  for i = 5, #ARGV do
    local s = redis.call('HMGET', prefix .. 'pool:asum:' .. ARGV[i],
      'txs', 'paid', 'paidCount', 'spent', 'spentCount', 'both', 'bothCount')
    if s[1] then
      for j = 1, 7 do
        s[j] = s[j] or '0'
      end
      answer[#answer + 1] = table.concat(s, ' ')
    else
      answer[#answer + 1] = ''
    end
  end
elseif what == 'txs' then
  local zkey = prefix .. 'pool:atx:' .. ARGV[5]
  local at = position(zkey, ARGV[6], ARGV[7])
  local rows = redis.call('ZRANGE', zkey, at, at + tonumber(ARGV[8]) - 1, 'WITHSCORES')
  for i = 1, #rows, 2 do
    answer[#answer + 1] = rows[i]
    answer[#answer + 1] = string.format('%.0f', -tonumber(rows[i + 1]))
  end
elseif what == 'outs' then
  local a = ARGV[5]
  local zkey = prefix .. 'pool:aout:' .. a
  local at = position(zkey, ARGV[6], ARGV[7])
  local rows = redis.call('ZRANGE', zkey, at, at + tonumber(ARGV[8]) - 1, 'WITHSCORES')
  for i = 1, #rows, 2 do
    answer[#answer + 1] = rows[i]
    answer[#answer + 1] = string.format('%.0f', -tonumber(rows[i + 1]))
    answer[#answer + 1] = redis.call('HGET', prefix .. 'pool:aval:' .. a, rows[i])
    answer[#answer + 1] = tostring(redis.call('HEXISTS', prefix .. 'pool:aspent:' .. a, rows[i]))
  end
elseif what == 'spent' then
  local flags = {}
  for i, o in ipairs(split(ARGV[6], osize)) do
    flags[i] = tostring(redis.call('HEXISTS', prefix .. 'pool:aspent:' .. ARGV[5], o))
  end
  answer[2] = table.concat(flags)
elseif what == 'bodies' then
  for _, t in ipairs(split(ARGV[5], tsize)) do
    answer[#answer + 1] = redis.call('HGET', prefix .. 'pool:body', t) or ''
    -- what spends its outputs: the inputs of the transactions of the pool
    -- that spend from it
    local spent = {}
    for _, kid in ipairs(redis.call('SMEMBERS', prefix .. 'pool:kids:' .. t)) do
      for i, input in ipairs(split(redis.call('HGET', prefix .. 'pool:spends', kid), osize)) do
        if string.sub(input, 1, tsize) == t then
          spent[#spent + 1] = input
          spent[#spent + 1] = kid
          spent[#spent + 1] = tostring(i - 1)
        end
      end
    end
    answer[#answer + 1] = tostring(#spent / 3)
    for _, item in ipairs(spent) do
      answer[#answer + 1] = item
    end
  end
elseif what == 'spenders' then
  for _, o in ipairs(split(ARGV[5], osize)) do
    local s = redis.call('HGET', prefix .. 'pool:spender', o)
    local index = ''
    if s then
      for i, input in ipairs(split(redis.call('HGET', prefix .. 'pool:spends', s), osize)) do
        if input == o then
          index = tostring(i - 1)
        end
      end
    end
    answer[#answer + 1] = s or ''
    answer[#answer + 1] = index
  end
elseif what == 'page' then
  local offset, many = tonumber(ARGV[5]), tonumber(ARGV[6])
  local last = -1
  if many > 0 then
    last = offset + many - 1
  end
  if some then
    -- without what is leaving: the place of a transaction in the list
    -- is its place among those that stay
    local rows, at = redis.call('ZREVRANGE', prefix .. 'pool:time', 0, -1, 'WITHSCORES'), 0
    for i = 1, #rows, 2 do
      if redis.call('HEXISTS', leaving, rows[i]) == 0 then
        if at >= offset and (many == 0 or at < offset + many) then
          answer[#answer + 1] = rows[i]
          answer[#answer + 1] = string.format('%.0f', tonumber(rows[i + 1]))
        end
        at = at + 1
      end
    end
    return answer
  end
  local rows = redis.call('ZREVRANGE', prefix .. 'pool:time', offset, last, 'WITHSCORES')
  for i = 1, #rows, 2 do
    answer[#answer + 1] = rows[i]
    answer[#answer + 1] = string.format('%.0f', tonumber(rows[i + 1]))
  end
elseif what == 'ids' then
  local found = redis.call('ZRANGEBYLEX', prefix .. 'pool:ids',
    '[' .. ARGV[5], '(' .. ARGV[5] .. string.rep(string.char(255), tsize))
  for _, t in ipairs(found) do
    answer[#answer + 1] = t
  end
elseif what == 'tokens' or what == 'holders' then
  local sets, lead = prefix .. 'pool:acat:', true
  if what == 'holders' then
    sets, lead = prefix .. 'pool:chold:', false
  end
  for _, m in ipairs(redis.call('SMEMBERS', sets .. ARGV[5])) do
    local c, a = m, ARGV[5]
    if not lead then
      c, a = ARGV[5], m
    end
    local s = redis.call('HMGET', prefix .. 'pool:tsum:' .. c .. a, 'txs', 'freeHi', 'freeLo', 'freeNfts',
      'freeOutputs', 'spentHi', 'spentLo', 'spentNfts', 'spentOutputs', 'receivedHi', 'receivedLo')
    for j = 1, 11 do
      s[j] = s[j] or '0'
    end
    answer[#answer + 1] = m
    answer[#answer + 1] = table.concat(s, ' ')
  end
elseif what == 'csum' then
  local s = redis.call('HMGET', prefix .. 'pool:csum:' .. ARGV[5], 'txs', 'free', 'spent')
  if s[1] then
    answer[2] = (s[1] or '0') .. ' ' .. (s[2] or '0') .. ' ' .. (s[3] or '0')
  else
    answer[2] = ''
  end
elseif what == 'ctxs' then
  local zkey = prefix .. 'pool:ctx:' .. ARGV[5]
  local at = position(zkey, ARGV[6], ARGV[7])
  local rows = redis.call('ZRANGE', zkey, at, at + tonumber(ARGV[8]) - 1, 'WITHSCORES')
  for i = 1, #rows, 2 do
    answer[#answer + 1] = rows[i]
    answer[#answer + 1] = string.format('%.0f', -tonumber(rows[i + 1]))
  end
elseif what == 'couts' then
  local c = ARGV[5]
  local zkey = prefix .. 'pool:cout:' .. c
  local at = position(zkey, ARGV[6], ARGV[7])
  local rows = redis.call('ZRANGE', zkey, at, at + tonumber(ARGV[8]) - 1, 'WITHSCORES')
  for i = 1, #rows, 2 do
    answer[#answer + 1] = rows[i]
    answer[#answer + 1] = string.format('%.0f', -tonumber(rows[i + 1]))
    answer[#answer + 1] = redis.call('HGET', prefix .. 'pool:cval:' .. c, rows[i])
    answer[#answer + 1] = tostring(redis.call('HEXISTS', prefix .. 'pool:spender', rows[i]))
  end
elseif what == 'replaced' then
  for i = 5, #ARGV do
    answer[#answer + 1] = redis.call('HGET', prefix .. 'pool:rep', ARGV[i]) or ''
  end
end
return answer
