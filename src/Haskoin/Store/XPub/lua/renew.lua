-- Renew the locks that a store holds: renewScript in Haskoin.Store.XPub.Lock.
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local lost = 0
for i, k in ipairs(KEYS) do
  local token, est = ARGV[2 * i], tonumber(ARGV[2 * i + 1])
  local v = redis.call('GET', k)
  if v and string.sub(v, 1, #token + 1) == token .. ' ' then
    local _, _, who = string.find(v, '^%S+ %S+ (.*)$')
    local done = string.format('%.0f', now + est)
    redis.call('SET', k, token .. ' ' .. done .. ' ' .. (who or ''), 'PX', ARGV[1])
  else
    lost = lost + 1
  end
end
return lost
