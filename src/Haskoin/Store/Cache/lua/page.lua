-- A page of a sorted set of a cached xpub: pageScript in Haskoin.Store.Cache.
local set, txs = ARGV[2], ARGV[3]
if redis.call('EXISTS', ARGV[1]) == 0 then return {'none'} end
if ARGV[5] ~= '' then redis.call('ZADD', ARGV[4], ARGV[5], ARGV[6]) end
local off = tonumber(ARGV[7])
local lim = tonumber(ARGV[8])
local mode = ARGV[9]
local function lt(a, b)
  for k = 1, math.min(#a, #b) do
    local x, y = string.byte(a, k), string.byte(b, k)
    if x ~= y then return x < y end
  end
  return #a < #b
end
local function out(r)
  local o = {'ok'}
  for _, v in ipairs(r) do table.insert(o, v) end
  return o
end
if mode == 'top' then
  local stop = -1
  if lim > 0 then stop = off + lim - 1 end
  return out(redis.call('ZRANGE', set, off, stop, 'WITHSCORES'))
end
local score = ARGV[10]
local skip = 0
if mode == 'tx' then
  score = redis.call('ZSCORE', txs, ARGV[10])
  if not score then return {'nostart'} end
end
if mode ~= 'score' then
  local cut, bytes = tonumber(ARGV[11]), ARGV[12]
  for _, m in ipairs(redis.call('ZRANGEBYSCORE', set, score, score)) do
    if lt(string.sub(m, cut + 1), bytes) then skip = skip + 1 end
  end
end
local count = -1
if lim > 0 then count = lim end
return out(redis.call('ZRANGEBYSCORE', set, score, '+inf', 'WITHSCORES', 'LIMIT', off + skip, count))
