-- Let go of a lock if this store still holds it: releaseScript in Haskoin.Store.XPub.Lock.
local v = redis.call('GET', KEYS[1])
if v and string.sub(v, 1, #ARGV[1] + 1) == ARGV[1] .. ' ' then
  return redis.call('DEL', KEYS[1])
end
return 0
