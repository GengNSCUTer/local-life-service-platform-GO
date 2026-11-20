#!/usr/bin/env bash
# Run against disposable, loopback-only middleware using installed binaries.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
: "${MYSQL_BIN_DIR:?set MYSQL_BIN_DIR to the MySQL bin directory}"
: "${KAFKA_BIN_DIR:?set KAFKA_BIN_DIR to the Kafka bin directory}"
redis_bin=${REDIS_BIN_DIR:-/usr/bin}
mode=${1:-restore}
case "$mode" in restore|soak) ;; *) echo 'usage: scripts/stage7-local.sh [restore|soak]' >&2; exit 2 ;; esac
port=${STAGE7_BASE_PORT:-29840}
mkdir -p runtime
work=$(mktemp -d "$PWD/runtime/stage7-$mode.XXXXXX")
export TMPDIR="$work"
export LOG_DIR="$work/kafka-logs"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -XX:ActiveProcessorCount=4 -XX:-UsePerfData -Djava.io.tmpdir=$work"
export KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS:-'-Xms128m -Xmx384m'}
export HTTP_ADDR="127.0.0.1:$port" MYSQL_ADDR="127.0.0.1:$((port+1))" REDIS_ADDR="127.0.0.1:$((port+2))"
export KAFKA_BROKERS="127.0.0.1:$((port+3))" WORKER_HEALTH_ADDR="127.0.0.1:$((port+5))"
export MYSQL_DATABASE=hmdp_stage7 MYSQL_USER=hmdp_stage7 MYSQL_PASSWORD=stage7_local_mysql
export REDIS_PASSWORD=stage7_local_redis REDISCLI_AUTH=stage7_local_redis REDIS_SENTINEL_ADDRS= GIN_MODE=release
export SECKILL_OUTBOX_LIMIT=0 SECKILL_PENDING_LIMIT=0 SECKILL_PENDING_TIMEOUT=15m KAFKA_CONSUMER_WORKERS=${KAFKA_CONSUMER_WORKERS:-3}
mysql=("$MYSQL_BIN_DIR/mysql" --no-defaults -u root --socket="$work/mysql.sock")
redis=("$redis_bin/redis-cli" --raw -p "$((port+2))")
mysql_pid= redis_pid= kafka_pid= api_pid= worker_pid= second_pid=
stop() {
    local pid=$1 result=0
    if [[ -n "$pid" ]]; then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" || result=$?
        [[ "$result" == 0 || "$result" == 143 ]]
    fi
}
cleanup() {
    stop "$second_pid" || true
    stop "$api_pid" || true
    stop "$worker_pid" || true
    stop "$kafka_pid" || true
    stop "$redis_pid" || true
    stop "$mysql_pid" || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'Stage 7 logs and data: %s\n' "$work"

go build -o "$work/api" ./cmd/api
go build -o "$work/worker" ./cmd/worker
# Java .properties files decode Unicode escapes in otherwise ASCII text.
kafka_data=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1])[1:-1])' "$work/source/kafka")
cat > "$work/kafka.properties" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$((port+4))
listeners=PLAINTEXT://$KAFKA_BROKERS,CONTROLLER://127.0.0.1:$((port+4))
advertised.listeners=PLAINTEXT://$KAFKA_BROKERS
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$kafka_data
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
PROPS

start_mysql() {
    local data=$1
    mkdir -p "$data"
    local basedir
    basedir=$(dirname -- "$MYSQL_BIN_DIR")
    "$MYSQL_BIN_DIR/mysqld" --no-defaults --basedir="$basedir" --datadir="$data" --initialize-insecure >>"$work/mysql.log" 2>&1
    "$MYSQL_BIN_DIR/mysqld" --no-defaults --basedir="$basedir" --datadir="$data" --bind-address=127.0.0.1 --port="$((port+1))" --mysqlx=OFF --socket="$work/mysql.sock" --pid-file="$work/mysql.pid" >>"$work/mysql.log" 2>&1 & mysql_pid=$!
    for attempt in $(seq 1 120); do
        if "${mysql[@]}" -e 'SELECT 1' >/dev/null 2>&1; then break; fi
        sleep .25
    done
    "${mysql[@]}" -e "CREATE DATABASE $MYSQL_DATABASE; CREATE USER '$MYSQL_USER'@'127.0.0.1' IDENTIFIED BY '$MYSQL_PASSWORD'; GRANT ALL ON $MYSQL_DATABASE.* TO '$MYSQL_USER'@'127.0.0.1';"
}
start_redis() {
    mkdir -p "$1"
    "$redis_bin/redis-server" --bind 127.0.0.1 --port "$((port+2))" --requirepass "$REDIS_PASSWORD" --save '' \
        --appendonly yes --appendfsync always --dir "$1" >>"$work/redis.log" 2>&1 & redis_pid=$!
    for attempt in $(seq 1 120); do
        if "${redis[@]}" ping 2>/dev/null | grep -qx PONG; then return; fi
        sleep .25
    done
    return 1
}
start_kafka() {
    "$KAFKA_BIN_DIR/kafka-server-start.sh" "$1" >>"$work/kafka.log" 2>&1 & kafka_pid=$!
    for attempt in $(seq 1 120); do
        if (true >/dev/tcp/127.0.0.1/"$((port+3))") 2>/dev/null; then return; fi
        sleep .5
    done
    return 1
}
wait_ready() {
    local addr=$1
    for attempt in $(seq 1 120); do
        if curl -fsS --max-time 3 "http://$addr/health/ready" >/dev/null 2>&1; then return; fi
        sleep .25
    done
    curl -fsS --max-time 3 "http://$addr/health/ready"
}
start_api() {
    KAFKA_BROKERS= OBSERVABILITY_ADDR="127.0.0.1:$((port+6))" "$work/api" >>"$work/api.log" 2>&1 & api_pid=$!
    wait_ready "$HTTP_ADDR"
}
start_worker() {
    OBSERVABILITY_ADDR="127.0.0.1:$((port+7))" "$work/worker" >>"$work/worker.log" 2>&1 & worker_pid=$!
    wait_ready "$WORKER_HEALTH_ADDR"
}
seed_sessions() {
    python3 - "$1" <<'PY' | "${redis[@]}" --pipe
import sys
for i in range(1, int(sys.argv[1]) + 1):
    fields = ['HSET', f'login:token:stage7-{i}', 'id', str(i)]
    sys.stdout.write(f'*{len(fields)}\r\n' + ''.join(f'${len(value)}\r\n{value}\r\n' for value in fields))
PY
}
wait_confirmed() {
    local expected=$1 found
    for attempt in $(seq 1 180); do
        found=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id=9700 AND status='confirmed'")
        if [[ "$found" == "$expected" ]]; then return; fi
        sleep .5
    done
    printf 'Expected %s confirmations, saw %s\n' "$expected" "$found" >&2
    return 1
}
wait_empty() {
    for attempt in $(seq 1 120); do
        if curl -fsS "http://127.0.0.1:$((port+7))/metrics" | awk '
            /^hmdp_seckill_kafka_lag\{/ {lag += $2; n++}
            /^hmdp_seckill_queue_size\{/ {size += $2; queues++}
            END {exit !(n==4 && queues==2 && lag==0 && size==0)}'; then return; fi
        sleep .5
    done
    return 1
}
submit() {
    local addr=$1 user=$2
    curl -fsS --max-time 5 -X POST -H "Authorization: Bearer stage7-$user" "http://$addr/voucher-order/seckill/9700" |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["orderId"])'
}
status() {
    curl -fsS --max-time 5 -H "Authorization: Bearer stage7-$1" "http://$HTTP_ADDR/voucher-order/$2" |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["status"])'
}
assert_inventory() {
    "$work/api" audit-seckill -voucher-id 9700 | tee "$work/audit.json" |
        python3 -c 'import json,sys; row=json.load(sys.stdin); assert row["status"]=="consistent" and row["confirmedOrders"]==int(sys.argv[1]) and row["mysqlRemaining"]==row["redisRemaining"]==0, row; print(row)' "$1"
}

mkdir -p "$work/source"
start_mysql "$work/source/mysql"
for sql in migrations/*.sql; do "${mysql[@]}" "$MYSQL_DATABASE" < "$sql"; done
start_redis "$work/source/redis"
"$KAFKA_BIN_DIR/kafka-storage.sh" format -t "$("$KAFKA_BIN_DIR/kafka-storage.sh" random-uuid)" -c "$work/kafka.properties" >/dev/null
start_kafka "$work/kafka.properties"
if [[ "$mode" == restore ]]; then export SECKILL_PENDING_LIMIT=2; fi
start_api
start_worker
stock=4
duration=${SOAK_SECONDS:-3660}
rate=${SOAK_RATE:-10}
if [[ "$mode" == soak ]]; then stock=$((duration * rate)); fi
seed_sessions "$stock"
starts=$(date -u -d '+6 seconds' +%FT%TZ)
"$work/api" prepare-seckill -voucher-id 9700 -stock "$stock" -starts-at "$starts" -ends-at "$(date -u -d "+$((duration + 1800)) seconds" +%FT%TZ)"
while (( $(date +%s) < $(date -d "$starts" +%s) )); do sleep .2; done

if [[ "$mode" == soak ]]; then
    wait_empty
    curl -fsS "http://127.0.0.1:$((port+6))/metrics" > "$work/api-before.prom"
    curl -fsS "http://127.0.0.1:$((port+7))/metrics" > "$work/worker-before.prom"
    python3 scripts/seckill-soak.py --port "$port" --voucher 9700 --seconds "$duration" --rate "$rate" --prefix stage7- | tee "$work/soak.jsonl"
    wait_confirmed "$stock"
    wait_empty
    assert_inventory "$stock"
    curl -fsS "http://127.0.0.1:$((port+6))/metrics" > "$work/api-after.prom"
    curl -fsS "http://127.0.0.1:$((port+7))/metrics" > "$work/worker-after.prom"
    exit 0
fi

# Keep a committed baseline, then create a pending order in each queue.
first=$(submit "$HTTP_ADDR" 1)
wait_confirmed 1
wait_empty
stop "$worker_pid"; worker_pid=
second_addr="127.0.0.1:$((port+8))"
HTTP_ADDR="$second_addr" KAFKA_BROKERS= OBSERVABILITY_ADDR= "$work/api" >"$work/api-second.log" 2>&1 & second_pid=$!
wait_ready "$second_addr"
stop "$api_pid"; api_pid=
second=$(submit "$second_addr" 2)
start_api
[[ $(status 2 "$second") == pending ]]
third=$(submit "$HTTP_ADDR" 3)
key='seckill:{orders}:'
payload=$("${redis[@]}" GET "${key}order:$third")
[[ -n "$payload" ]]
printf '%s\t%s\n' "$third" "$payload" | "$KAFKA_BIN_DIR/kafka-console-producer.sh" \
    --bootstrap-server "$KAFKA_BROKERS" --topic hmdp.seckill.order.v1 --property parse.key=true --producer-property acks=all
[[ $("${redis[@]}" ZREM "${key}outbox" "$third") == 1 ]]
[[ $("${redis[@]}" ZCARD "${key}outbox") == 1 ]]
[[ $(status 1 "$first") == confirmed && $(status 3 "$third") == pending ]]
code=$(curl -sS --max-time 5 -D "$work/backpressure-headers.txt" -o "$work/backpressure.json" -w '%{http_code}' \
    -X POST -H 'Authorization: Bearer stage7-4' "http://$HTTP_ADDR/voucher-order/seckill/9700")
[[ "$code" == 503 ]]
grep -iq '^Retry-After: 1' "$work/backpressure-headers.txt"
[[ $("${redis[@]}" HGET "${key}activity:9700" stock) == 1 ]]
[[ $("${redis[@]}" HLEN "${key}activity:9700:buyers") == 3 ]]
[[ $("${redis[@]}" ZCARD "${key}outbox") == 1 ]]
[[ $("${redis[@]}" ZCARD "${key}timeouts") == 2 ]]
stop "$second_pid"; second_pid=
stop "$api_pid"; api_pid=

# Snapshot with application writes stopped. Kafka and Redis are copied only after graceful stop.
mkdir -p "$work/backup" "$work/restored/redis"
"$MYSQL_BIN_DIR/mysqldump" --no-defaults --socket="$work/mysql.sock" -u root --single-transaction --no-tablespaces "$MYSQL_DATABASE" > "$work/backup/mysql.sql"
stop "$kafka_pid"; kafka_pid=
stop "$redis_pid"; redis_pid=
stop "$mysql_pid"; mysql_pid=
cp -a "$work/source/kafka" "$work/backup/kafka"
cp -a "$work/source/redis/appendonlydir" "$work/backup/appendonlydir"
cp -a "$work/backup/kafka" "$work/restored/kafka"
cp -a "$work/backup/appendonlydir" "$work/restored/redis/appendonlydir"
sed 's|/source/kafka$|/restored/kafka|' "$work/kafka.properties" > "$work/kafka-restored.properties"
start_mysql "$work/restored/mysql"
"${mysql[@]}" "$MYSQL_DATABASE" < "$work/backup/mysql.sql"
start_redis "$work/restored/redis"
start_kafka "$work/kafka-restored.properties"
start_api
[[ $(status 1 "$first") == confirmed && $(status 2 "$second") == pending && $(status 3 "$third") == pending ]]
[[ $("${redis[@]}" ZCARD "${key}outbox") == 1 ]]
"$KAFKA_BIN_DIR/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" --describe --group hmdp-seckill-orders-v1 > "$work/kafka-restored-offsets.txt"
awk '$1=="hmdp-seckill-orders-v1" {committed += $4} END {exit !(committed==1)}' "$work/kafka-restored-offsets.txt"
"$KAFKA_BIN_DIR/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BROKERS" --topic hmdp.seckill.order.v1 > "$work/kafka-restored-end-offsets.txt"
awk -F: 'NF==3 {n++; end += $3} END {exit !(n==3 && end==2)}' "$work/kafka-restored-end-offsets.txt"
start_worker
wait_confirmed 3
wait_empty
[[ $(status 1 "$first") == confirmed && $(status 2 "$second") == confirmed && $(status 3 "$third") == confirmed ]]
fourth=$(submit "$HTTP_ADDR" 4)
wait_confirmed 4
wait_empty
[[ $(status 4 "$fourth") == confirmed ]]
assert_inventory 4
printf 'PASS: API restart, pending backpressure, MySQL dump, Redis AOF and Kafka log/offset recovery\n'
