-- Record or forget spenders of outputs of cached transactions: spendsScript in Haskoin.Store.Cache.
local p = ARGV[1]
local n = tonumber(ARGV[2])
local i = 3
for j = 1, n do
  local t = ARGV[i]
  if redis.call('EXISTS', p .. 'tx:' .. t) == 1 then
    local sk = p .. 'sp:' .. t
    if ARGV[i + 2] ~= '' then
      redis.call('HSET', sk, ARGV[i + 1], ARGV[i + 2])
    else
      local cur = redis.call('HGET', sk, ARGV[i + 1])
      if cur and string.sub(cur, 1, 32) == ARGV[i + 3] then
        redis.call('HDEL', sk, ARGV[i + 1])
      end
    end
  end
  i = i + 4
end
return 1
