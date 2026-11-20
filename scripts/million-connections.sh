#!/usr/bin/env bash
set -euo pipefail
umask 077
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
if [[ ${1:-} != isolated ]]; then
    # Build and run in a private network namespace. No host sysctls or listeners change.
    mkdir -p runtime
    work=$(mktemp -d "$PWD/runtime/million-connections.XXXXXX")
    export TMPDIR="$work" GOTMPDIR="$work"
    printf 'Connection validation artifacts: %s\n' "$work"
    go build -o "$work/api" ./cmd/api
    go build -o "$work/connection-bench" ./cmd/connection-bench
    exec unshare --user --map-root-user --net -- "$BASH" "$0" isolated "$work"
fi
[[ $(readlink /proc/self/ns/net) != "$(readlink /proc/1/ns/net)" ]]
work=$2
ip link set lo up
base=31320
servers=8
clients=${CONNECTION_TEST_COUNT:-1000000}
hold=${CONNECTION_TEST_HOLD:-120s}
(( $(ulimit -n) >= clients + 1024 ))
export HTTP_IDLE_TIMEOUT=30m KAFKA_BROKERS= OBSERVABILITY_ADDR= GIN_MODE=release
export MYSQL_ADDR=127.0.0.1:31340 REDIS_ADDR=127.0.0.1:31341 REDIS_SENTINEL_ADDRS=
export GOMAXPROCS=${CONNECTION_TEST_PROCS:-3}
pids=()
client_pid=
cleanup() {
    for pid in ${client_pid:+"$client_pid"} "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
    for pid in ${client_pid:+"$client_pid"} "${pids[@]}"; do wait "$pid" || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for (( offset=0; offset<servers; offset++ )); do
    HTTP_ADDR="127.0.0.1:$((base+offset))" "$work/api" > "$work/api-$offset.log" 2>&1 &
    pids+=("$!")
done
for (( offset=0; offset<servers; offset++ )); do
    for attempt in $(seq 1 60); do
        if curl -fsS --max-time 2 "http://127.0.0.1:$((base+offset))/health/live" >/dev/null 2>&1; then break; fi
        sleep .1
    done
    curl -fsS --max-time 2 "http://127.0.0.1:$((base+offset))/health/live" >/dev/null
done
"$work/connection-bench" -port "$base" -servers "$servers" -connections "$clients" -hold "$hold" \
    > "$work/connections.jsonl" 2> "$work/client.log" &
client_pid=$!
printf 'unix_time\testablished_server_connections\tavailable_host_kib\tapi_rss_kib\n' > "$work/socket-samples.tsv"
max_established=0
while kill -0 "$client_pid" 2>/dev/null; do
    awk '/^TcpExt:/ { print }' /proc/net/netstat > "$work/tcp-ext-latest.txt"
    available=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
    if (( available < 32*1024*1024 )); then
        echo 'Host available memory dropped below 32 GiB' >&2
        exit 1
    fi
    established=$(ss -Htn state established "( sport >= :$base and sport < :$((base+servers)) )" | wc -l)
    rss=$(ps -o rss= -p "$(IFS=,; echo "${pids[*]}")" | awk '{n+=$1} END {print n+0}')
    printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$established" "$available" "$rss" | tee -a "$work/socket-samples.tsv"
    if (( established > max_established )); then max_established=$established; fi
    sleep 15
done
wait "$client_pid" || { cat "$work/client.log" >&2; exit 1; }
client_pid=
cat "$work/connections.jsonl"
[[ "$max_established" == "$clients" ]]
printf 'PASS %s simultaneous TCP connections measured on API sockets\n' "$max_established"
