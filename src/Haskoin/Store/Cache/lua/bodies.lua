-- Rewrite bodies that Redis has, ahead of a commit: bodiesScript in Haskoin.Store.Cache.
local p = ARGV[1]
local nb = tonumber(ARGV[2])
local bi = 3
local si = bi + 2 * nb + 1
local ns = tonumber(ARGV[si - 1])
local written = {}
local absent = {}
for j = 0, nb - 1 do
  local t = ARGV[bi + 2 * j]
  if redis.call('EXISTS', p .. 'tx:' .. t) == 1 then
    redis.call('SET', p .. 'tx:' .. t, ARGV[bi + 2 * j + 1])
    redis.call('DEL', p .. 'sp:' .. t)
    written[t] = true
  else
    table.insert(absent, t)
  end
end
for j = 0, ns - 1 do
  local k = si + 3 * j
  if written[ARGV[k]] then
    redis.call('HSET', p .. 'sp:' .. ARGV[k], ARGV[k + 1], ARGV[k + 2])
  end
end
return absent
