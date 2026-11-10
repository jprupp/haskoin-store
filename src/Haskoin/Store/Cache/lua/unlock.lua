-- Release a lock if it still holds the given token: unlockScript in Haskoin.Store.Cache.
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
