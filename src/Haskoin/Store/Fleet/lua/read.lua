-- What a store reads from the shared mempool of its fleet: readScript in
-- Haskoin.Store.Fleet.Redis. pool.lua writes what this reads.
--
-- ARGV[1] is what to read, ARGV[2] the prefix of the keys, ARGV[3] the size
-- of a transaction id in bytes, and ARGV[4] the fleet tip that the store
-- answers from, or ''. Every answer is {'raced'} if the tip is another one:
-- the pool goes with the tip, and laid over the chain of another block it
-- would give a transaction twice or not at all. Otherwise it is {'ok', ...}.
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
-- '1' if it can be replaced by fee or '0', the number of its outputs that
-- the pool spends, and for each of those the output, the transaction that
-- spends it and the number of its input that does.
--
-- 'spenders': outputs. For each, the transaction that spends it and the
-- number of its input that does, or two ''.
--
-- 'page': an offset and how many, 0 for all. Transactions of the pool from
-- the newest, an id and a time for each.
--
-- 'ids': some bytes. The transactions whose id starts with them.
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
    answer[#answer + 1] = tostring(redis.call('SISMEMBER', prefix .. 'pool:rbf', t))
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
elseif what == 'replaced' then
  for i = 5, #ARGV do
    answer[#answer + 1] = redis.call('HGET', prefix .. 'pool:rep', ARGV[i]) or ''
  end
end
return answer
