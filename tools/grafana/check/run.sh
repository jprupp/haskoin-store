#!/bin/sh
# Check the Grafana dashboard against a real Telegraf and a real InfluxDB,
# with a known load:
#
#   tools/grafana/check/run.sh
#
# It generates the dashboard, starts InfluxDB 1.8, Telegraf with its statsd
# input as it comes, and Grafana with the dashboard, in a pod of Podman.
# Then it sends every metric that the dashboard reads, for two minutes and
# a half, through the store's own StatsD library, as a store started with
# --statsd-aggregates --statsd-no-events does (Sender.hs). After a hundred
# seconds it runs every query of the dashboard through Grafana, and holds
# some to what was sent (check.py). It ends with a failure if a query fails
# or a figure is wrong, and removes the pod either way.
#
# With `keep` as its argument it leaves the pod running afterwards, with the
# sender, so that the dashboard can be looked at in a browser, at the address
# that it prints: what a panel does after its queries, naming the series and
# adding them up, is only seen there.
#
# It needs podman, python3, and stack with the store's dependencies built:
# the sender is run with the statsd-rupp of the store. Telegraf listens on
# 127.0.0.1:CHECK_STATSD_PORT (18125) and Grafana on
# 127.0.0.1:CHECK_GRAFANA_PORT (13000).
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
pod=haskoin-grafana-check
statsd=${CHECK_STATSD_PORT:-18125}
grafana=${CHECK_GRAFANA_PORT:-13000}
keep=${1:-}
work=$(mktemp -d)
sender=

cleanup() {
  status=$?
  if [ "$keep" = keep ] && [ "$status" -eq 0 ]; then
    echo "Left running: http://127.0.0.1:$grafana/d/haskoin-store"
    echo "The sender goes on for an hour. Stop both with:"
    echo "  kill -- -$sender; podman pod rm -f $pod"
  else
    # the sender and the programs that it started: a group of their own
    [ -n "$sender" ] && kill -- "-$sender" 2> /dev/null || true
    podman pod rm -f "$pod" > /dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# The settings are copied, so that the labels that Podman gives what a
# container reads are not put on files of the repository.
mkdir -p "$work/provisioning/datasources" "$work/provisioning/dashboards" "$work/dashboards"
cp "$here/telegraf.conf" "$work/telegraf.conf"
cp "$here/grafana/datasources.yaml" "$work/provisioning/datasources/"
cp "$here/grafana/dashboards.yaml" "$work/provisioning/dashboards/"
python3 "$root/tools/grafana/haskoin_grafana.py" "$work/dashboards/haskoin-store.json" > /dev/null
python3 "$here/sample.py" > "$work/metrics.txt"

podman pod rm -f "$pod" > /dev/null 2>&1 || true
podman pod create --name "$pod" \
  -p "127.0.0.1:$statsd:8125/udp" \
  -p "127.0.0.1:$grafana:3000" > /dev/null
podman run -d --pod "$pod" --memory 1g docker.io/library/influxdb:1.8 > /dev/null
# Telegraf's image starts it through a program that Podman without root
# does not let it run: start Telegraf itself.
podman run -d --pod "$pod" --memory 512m --entrypoint telegraf \
  -v "$work/telegraf.conf:/etc/telegraf/telegraf.conf:ro,Z" \
  docker.io/library/telegraf:1.33 --config /etc/telegraf/telegraf.conf > /dev/null
podman run -d --pod "$pod" --memory 1g \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
  -v "$work/provisioning:/etc/grafana/provisioning:ro,Z" \
  -v "$work/dashboards:/var/lib/grafana/dashboards:ro,Z" \
  docker.io/grafana/grafana:11.4.0 > /dev/null

echo "Waiting for Grafana"
tries=0
until python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:$grafana/api/health', timeout=2)" \
  2> /dev/null; do
  tries=$((tries + 1))
  [ "$tries" -lt 60 ] || { echo "Grafana did not start"; exit 1; }
  sleep 2
done

echo "Sending every metric for 150 seconds"
seconds=150
[ "$keep" = keep ] && seconds=3600
cd "$root"
setsid stack runghc --package statsd-rupp -- \
  "$here/Sender.hs" "$work/metrics.txt" "$statsd" "$seconds" > "$work/sender.log" 2>&1 &
sender=$!
sleep 100
kill -0 "$sender" 2> /dev/null || { cat "$work/sender.log"; echo "The sender stopped"; exit 1; }

python3 "$here/check.py" "$work/dashboards/haskoin-store.json" "http://127.0.0.1:$grafana"
