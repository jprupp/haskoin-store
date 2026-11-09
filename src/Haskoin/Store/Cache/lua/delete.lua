-- Delete an xpub: deleteScript in Haskoin.Store.Cache. evict.lua removes
-- xpubs the same way.
local p = ARGV[1]
local id = ARGV[2]
local xk = p .. 'x:' .. id .. ':'
for _, a in ipairs(redis.call('SMEMBERS', xk .. 'addrs')) do
  redis.call('SREM', p .. 'a:' .. a, id)
end
for _, t in ipairs(redis.call('ZRANGE', xk .. 'txs', 0, -1)) do
  local rk = p .. 'txref:' .. t
  redis.call('SREM', rk, id)
  if redis.call('SCARD', rk) == 0 then
    redis.call('DEL', rk, p .. 'tx:' .. t, p .. 'sp:' .. t)
  end
end
local existed = redis.call('DEL', xk .. 'meta', xk .. 'bal', xk .. 'txs', xk .. 'utxo', xk .. 'addrs', xk .. 'uop')
redis.call('ZREM', p .. 'lru', id)
if existed > 0 then return 1 else return 0 end
