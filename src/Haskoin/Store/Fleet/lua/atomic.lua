-- Decisions made in Haskell, carried out in one step: atomicScript in
-- Haskoin.Store.Fleet.Atomic.Redis, for Haskoin.Store.Fleet.Atomic, which
-- says why. This script decides nothing. It asks Redis questions, and it
-- carries out a list of writes if the answers to questions are what the
-- caller was told before.
--
-- ARGV[1] is what to do.
--
-- 'read': the number of questions, and the questions. Returns the clock of
-- Redis in milliseconds, as a string, and then the answer to each question.
--
-- 'apply': the number of questions, each question followed by the answer
-- that the caller has for it, the number of writes, and the writes.
-- Returns {'raced', n} if the answer to question n, from 1, is another
-- now, and has changed nothing then; {'done'} once it has carried out
-- every write.
--
-- A question is a letter for the shape of its answer, the number of words
-- of its command, and the words: 'p' for an answer to take as it is, 'u'
-- for a list in no order, as the members of a set, 'h' for the fields of
-- a hash with their values, and 'k' for the fields of a hash whose value
-- is a number above a bound, which is then the first of the words, before
-- the command.
--
-- An answer is given as a string that says all of it, so that two answers
-- are the same when their strings are: 'n' for none, 'i' a number and ';',
-- 's' a length, ':' and so many bytes, 'a' a count, ';' and so many
-- answers. A list in no order is given in the order of the SHA-1 of its
-- members, and a hash as pairs in the order of the SHA-1 of its fields:
-- that order is the same every time, which the order of the bytes is not
-- where Redis compares strings as some language does.
--
-- A write is the number of its words and the words of one command, or two
-- words, '!WIPE' and the start of the keys to delete.
--
-- No write comes before the last answer is compared, and nothing is
-- decided between two writes: each is a plain command to a key of the kind
-- that the command is for. So a call changes everything it was given or
-- nothing, which a script that reads and writes in turn cannot promise:
-- Redis keeps what a script wrote before it failed.
local mode = ARGV[1]

local function enc(r)
  local t = type(r)
  if t == 'string' then
    return 's' .. #r .. ':' .. r
  elseif t == 'number' then
    return 'i' .. string.format('%.0f', r) .. ';'
  elseif t == 'table' then
    if r.ok then
      return 's' .. #r.ok .. ':' .. r.ok
    end
    local parts = {'a' .. #r .. ';'}
    for i = 1, #r do
      parts[#parts + 1] = enc(r[i])
    end
    return table.concat(parts)
  end
  return 'n'
end

-- pairs of a key to order by and an item, as the items in the order of
-- the SHA-1 of their keys
local function ordered(keyed)
  for _, pair in ipairs(keyed) do
    pair[1] = redis.sha1hex(pair[1])
  end
  table.sort(keyed, function(a, b) return a[1] < b[1] end)
  local out = {}
  for i, pair in ipairs(keyed) do
    out[i] = pair[2]
  end
  return out
end

-- the answer to the question at a place of the arguments, and the place
-- after it
local function ask(at)
  local shape, n = ARGV[at], tonumber(ARGV[at + 1])
  local words = {}
  for i = 1, n do
    words[i] = ARGV[at + 1 + i]
  end
  local above
  if shape == 'k' then
    above = tonumber(table.remove(words, 1))
  end
  local r = redis.call(unpack(words))
  if shape == 'k' then
    -- of a hash, the fields whose value is a number above the bound
    local keyed = {}
    for i = 1, #r, 2 do
      local n = tonumber(r[i + 1])
      if n and n > above then
        keyed[#keyed + 1] = {r[i], r[i]}
      end
    end
    r = ordered(keyed)
  elseif shape == 'u' then
    local keyed = {}
    for i, member in ipairs(r) do
      keyed[i] = {member, member}
    end
    r = ordered(keyed)
  elseif shape == 'h' then
    local keyed = {}
    for i = 1, #r, 2 do
      keyed[#keyed + 1] = {r[i], {r[i], r[i + 1]}}
    end
    r = ordered(keyed)
  end
  return enc(r), at + 2 + n
end

if mode == 'read' then
  local time = redis.call('TIME')
  local answers = {string.format('%.0f', time[1] * 1000 + math.floor(time[2] / 1000))}
  local at = 3
  for _ = 1, tonumber(ARGV[2]) do
    local a
    a, at = ask(at)
    answers[#answers + 1] = a
  end
  return answers
end

-- apply: compare every answer, and only then write
local at = 3
for i = 1, tonumber(ARGV[2]) do
  local a
  a, at = ask(at)
  if a ~= ARGV[at] then
    return {'raced', tostring(i)}
  end
  at = at + 1
end
local writes = tonumber(ARGV[at])
at = at + 1
for _ = 1, writes do
  local n = tonumber(ARGV[at])
  if ARGV[at + 1] == '!WIPE' then
    for _, k in ipairs(redis.call('KEYS', ARGV[at + 2] .. '*')) do
      redis.call('DEL', k)
    end
  else
    local words = {}
    for i = 1, n do
      words[i] = ARGV[at + i]
    end
    redis.call(unpack(words))
  end
  at = at + 1 + n
end
return {'done'}
