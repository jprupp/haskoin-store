-- Take the lock of an xpub, to index it: takeScript in Haskoin.Store.XPub.Lock.
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local done = string.format('%.0f', now + tonumber(ARGV[2]))
if redis.call('SET', KEYS[1], ARGV[1] .. ' ' .. done .. ' ' .. ARGV[3], 'NX', 'PX', ARGV[4]) then
  return {'ok'}
end
local v = redis.call('GET', KEYS[1])
local _, _, at, who = string.find(v or '', '^%S+ (%S+) (.*)$')
if not at then return {'held', '0', ''} end
return {'held', string.format('%.0f', tonumber(at) - now), who}
