-- Evict the least recently used xpubs past a number: evictScript in
-- Haskoin.Store.Cache. Each xpub goes as delete.lua removes one.
local p = ARGV[1]
local max = tonumber(ARGV[2])
local n = 0
while redis.call('ZCARD', p .. 'lru') > max do
  local id = redis.call('ZRANGE', p .. 'lru', 0, 0)[1]
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
  redis.call('DEL', xk .. 'meta', xk .. 'bal', xk .. 'txs', xk .. 'utxo', xk .. 'addrs', xk .. 'uop')
  redis.call('ZREM', p .. 'lru', id)
  n = n + 1
end
return n
