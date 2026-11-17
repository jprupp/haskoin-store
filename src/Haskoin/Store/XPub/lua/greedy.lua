-- GreedyDual for the caches of xpubs in Redis: greedyLua in Haskoin.Store.XPub,
-- put before the scripts that use it. A use: priority = level + cost.
local function greedy_use(prio, level, id, cost, only_new)
  local p = tonumber(redis.call('GET', level) or '0') + (tonumber(cost) or 0)
  if only_new then
    redis.call('ZADD', prio, 'NX', p, id)
  else
    redis.call('ZADD', prio, p, id)
  end
end
-- evict the lowest priority past max, raising the level; drop deletes an entry
local function greedy_evict(prio, level, max, drop)
  local n = 0
  while redis.call('ZCARD', prio) > max do
    local low = redis.call('ZRANGE', prio, 0, 0, 'WITHSCORES')
    local id, score = low[1], tonumber(low[2])
    if score > tonumber(redis.call('GET', level) or '0') then
      redis.call('SET', level, string.format('%.0f', score))
    end
    drop(id)
    redis.call('ZREM', prio, id)
    n = n + 1
  end
  return n
end
-- entries scored by the time of their last use: the level, so they go first
local function greedy_migrate(old, prio, level)
  if redis.call('EXISTS', old) == 1 then
    local lv = tonumber(redis.call('GET', level) or '0')
    for _, id in ipairs(redis.call('ZRANGE', old, 0, -1)) do
      redis.call('ZADD', prio, 'NX', lv, id)
    end
    redis.call('DEL', old)
  end
end
