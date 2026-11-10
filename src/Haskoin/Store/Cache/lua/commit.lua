-- Apply the differences of an entry of an xpub: commitScript in Haskoin.Store.Cache.
local p = ARGV[1]
local id = ARGV[2]
local h = tonumber(ARGV[3])
local xk = p .. 'x:' .. id .. ':'
local meta = xk .. 'meta'
local oh = redis.call('HGET', meta, 'height')
if oh and h < tonumber(oh) then return {'stale'} end
if (redis.call('HGET', meta, 'gen') or '') ~= ARGV[7] then return {'changed'} end
local full = ARGV[8]
local i = 9
local function list(width)
  local n = tonumber(ARGV[i])
  local s = i + 1
  i = s + width * n
  return s, n
end
local aa, naa = list(1)
local ad, nad = list(1)
local ta, nta = list(2)
local td, ntd = list(1)
local ua, nua = list(3)
local ud, nud = list(2)
local bi, nb = list(2)
local si, ns = list(3)
local given = {}
for j = 0, nb - 1 do given[ARGV[bi + 2 * j]] = true end
local missing = {'missing'}
for j = 0, nta - 1 do
  local t = ARGV[ta + 2 * j]
  if not given[t] and redis.call('EXISTS', p .. 'tx:' .. t) == 0 then
    table.insert(missing, t)
  end
end
if #missing > 1 then return missing end
for j = 0, nad - 1 do
  local a = ARGV[ad + j]
  redis.call('SREM', p .. 'a:' .. a, id)
  redis.call('SREM', xk .. 'addrs', a)
end
for j = 0, naa - 1 do
  local a = ARGV[aa + j]
  redis.call('SADD', p .. 'a:' .. a, id)
  redis.call('SADD', xk .. 'addrs', a)
end
for j = 0, ntd - 1 do
  local t = ARGV[td + j]
  local rk = p .. 'txref:' .. t
  redis.call('ZREM', xk .. 'txs', t)
  redis.call('SREM', rk, id)
  if redis.call('SCARD', rk) == 0 then
    redis.call('DEL', rk, p .. 'tx:' .. t, p .. 'sp:' .. t)
  end
end
for j = 0, nta - 1 do
  local t = ARGV[ta + 2 * j]
  redis.call('ZADD', xk .. 'txs', ARGV[ta + 2 * j + 1], t)
  redis.call('SADD', p .. 'txref:' .. t, id)
end
-- unspent outputs by outpoint: uop maps each to its member, which changes
-- with the block of the output, so that one can go by its outpoint alone
local uop = xk .. 'uop'
for j = 0, nud - 1 do
  local op, m = ARGV[ud + 2 * j], ARGV[ud + 2 * j + 1]
  if m == '' then m = redis.call('HGET', uop, op) end
  if m then redis.call('ZREM', xk .. 'utxo', m) end
  redis.call('HDEL', uop, op)
end
for j = 0, nua - 1 do
  local op, m = ARGV[ua + 3 * j], ARGV[ua + 3 * j + 1]
  local old = redis.call('HGET', uop, op)
  if old and old ~= m then redis.call('ZREM', xk .. 'utxo', old) end
  redis.call('ZADD', xk .. 'utxo', ARGV[ua + 3 * j + 2], m)
  redis.call('HSET', uop, op, m)
end
for j = 0, nb - 1 do
  local t = ARGV[bi + 2 * j]
  redis.call('SET', p .. 'tx:' .. t, ARGV[bi + 2 * j + 1])
  redis.call('DEL', p .. 'sp:' .. t)
end
for j = 0, ns - 1 do
  local k = si + 3 * j
  if given[ARGV[k]] then
    redis.call('HSET', p .. 'sp:' .. ARGV[k], ARGV[k + 1], ARGV[k + 2])
  end
end
redis.call('SET', xk .. 'bal', ARGV[6])
redis.call('HSET', meta, 'spec', ARGV[5], 'height', ARGV[3], 'ver', '3')
if full ~= '' then redis.call('HSET', meta, 'full', full) end
redis.call('HINCRBY', meta, 'gen', 1)
redis.call('ZADD', p .. 'lru', ARGV[4], id)
return {'ok'}
