-- Who holds a lock, and until when: peekScript in Haskoin.Store.XPub.Lock.
local v = redis.call('GET', KEYS[1])
if not v then return {} end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local _, _, at, who = string.find(v, '^%S+ (%S+) (.*)$')
if not at then return {'0', ''} end
return {string.format('%.0f', tonumber(at) - now), who}
