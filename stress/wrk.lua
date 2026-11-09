-- The script of wrk for the stress test (stress/Main.hs). It asks for the
-- paths given after "--" on the command line in turn, and when wrk is done it
-- prints one line of JSON with the results, latencies in microseconds:
--
--   wrk -s wrk.lua http://server -- /xpub/a /xpub/b
local paths = {}
local i = 0

init = function(args)
  paths = args
end

request = function()
  i = i % #paths + 1
  return wrk.format("GET", paths[i])
end

done = function(summary, latency, requests)
  local e = summary.errors
  local fields = {
    '"requests":' .. summary.requests,
    '"duration_us":' .. summary.duration,
    '"errors":' .. (e.connect + e.read + e.write + e.timeout),
    '"non2xx":' .. e.status,
    '"p50_us":' .. latency:percentile(50),
    '"p90_us":' .. latency:percentile(90),
    '"p99_us":' .. latency:percentile(99),
    '"max_us":' .. latency.max,
    '"mean_us":' .. string.format("%.1f", latency.mean),
  }
  io.write("{" .. table.concat(fields, ",") .. "}\n")
end
