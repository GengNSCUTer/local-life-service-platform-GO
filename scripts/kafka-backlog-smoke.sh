#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

project=hmdp-go-kafka-backlog-smoke
message_count=${BACKLOG_MESSAGES:-10000}
activity_count=${BACKLOG_ACTIVITIES:-1}
partition_count=${BACKLOG_PARTITIONS:-3}
drain_timeout=${BACKLOG_TIMEOUT_SECONDS:-300}
for value in "$message_count" "$activity_count" "$partition_count" "$drain_timeout"; do
    [[ "$value" =~ ^[1-9][0-9]{0,6}$ ]] || { echo 'Backlog settings must be positive decimal integers.' >&2; exit 2; }
done
if (( message_count > 1000000 || activity_count > 100 || partition_count > 48 || message_count % activity_count != 0 )); then
    echo 'Use up to 1,000,000 messages, 100 activities and 48 partitions. Divide messages evenly among activities.' >&2
    exit 2
fi
mysql_port=${MYSQL_PORT:-33316}
redis_port=${REDIS_PORT:-36389}
kafka_port=${KAFKA_PORT:-39192}
sentinel_1_port=${REDIS_SENTINEL_1_PORT:-36390}
sentinel_2_port=${REDIS_SENTINEL_2_PORT:-36391}
sentinel_3_port=${REDIS_SENTINEL_3_PORT:-36392}
http_port=${HTTP_PORT:-18090}
password=${REDIS_PASSWORD:-hmdp_dev_redis}

export MYSQL_PORT="$mysql_port" REDIS_PORT="$redis_port" KAFKA_PORT="$kafka_port"
export REDIS_SENTINEL_1_PORT="$sentinel_1_port" REDIS_SENTINEL_2_PORT="$sentinel_2_port" REDIS_SENTINEL_3_PORT="$sentinel_3_port"
export MYSQL_ADDR="127.0.0.1:$mysql_port" MYSQL_USER="${MYSQL_USER:-hmdp}" MYSQL_DATABASE="${MYSQL_DATABASE:-hmdp}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-hmdp_dev_mysql}" REDIS_PASSWORD="$password"
export REDIS_SENTINEL_PASSWORD= REDIS_MASTER_NAME="hmdp-master" REDIS_SENTINEL_ADDRS="127.0.0.1:$sentinel_1_port,127.0.0.1:$sentinel_2_port,127.0.0.1:$sentinel_3_port"
export KAFKA_BROKERS="127.0.0.1:$kafka_port" HTTP_ADDR="127.0.0.1:$http_port"
export KAFKA_CONSUMER_WORKERS="${KAFKA_CONSUMER_WORKERS:-$partition_count}"
# Send legacy events directly to Kafka without Redis reservations.
export SECKILL_REDIS_SHARDED=false SECKILL_REDIS_CLUSTER_ADDRS= SECKILL_STOCK_BUCKETS=1

source scripts/smoke-compose.sh
mysql_exec() {
    local query=$1
    compose exec -T -e MYSQL_QUERY="$query" mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -u "$MYSQL_USER" "$MYSQL_DATABASE" -e "$MYSQL_QUERY"'
}
kafka_tool() {
    local tool=$1
    shift
    compose exec -T kafka "/opt/kafka/bin/$tool.sh" --bootstrap-server kafka:9092 "$@"
}

smoke_start

first_voucher=9201
last_voucher=$((first_voucher + activity_count - 1))
topic=hmdp.seckill.order.v1
kafka_tool kafka-topics --create --topic "$topic" --partitions "$partition_count" --replication-factor 1
starts_at=$(date -u -d "+$((8+activity_count)) seconds" +%FT%TZ)
ends_at=$(date -u -d '+30 minutes' +%FT%TZ)
for (( voucher_id=first_voucher; voucher_id<=last_voucher; voucher_id++ )); do
    "$work_dir/api" prepare-seckill -voucher-id "$voucher_id" -stock "$((message_count/activity_count))" \
        -starts-at "$starts_at" -ends-at "$ends_at"
done

python3 - "$message_count" "$first_voucher" "$activity_count" <<'PY' | \
    kafka_tool kafka-console-producer --topic "$topic" --property parse.key=true --producer-property acks=all
import base64
import json
import sys
from datetime import datetime, timezone

count, first_voucher, activity_count = map(int, sys.argv[1:])
occurred_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
for index in range(1, count + 1):
    order_id = base64.b32encode(index.to_bytes(16, "big")).decode().rstrip("=")
    event = {
        "schema_version": 1,
        "order_id": order_id,
        "voucher_id": first_voucher + (index - 1) % activity_count,
        "user_id": 700000 + index,
        "occurred_at": occurred_at,
    }
    print(order_id + "\t" + json.dumps(event, separators=(",", ":")))
PY

queued=$(kafka_tool kafka-get-offsets --topic "$topic" | awk -F: 'NF==3 { sum += $3 } END { print sum+0 }')
[[ "$queued" == "$message_count" ]]
echo "Seeded $queued messages across $activity_count activities and $partition_count partitions"

started_at=$SECONDS
"$work_dir/all" >"$work_dir/api.log" 2>&1 &
api_pid=$!
for attempt in $(seq 1 60); do
    if curl -fsS --max-time 2 "http://$HTTP_ADDR/health/ready" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
curl -fsS --max-time 2 "http://$HTTP_ADDR/health/ready" >/dev/null

lag=-1
while (( SECONDS-started_at < drain_timeout )); do
    lag=$(kafka_tool kafka-consumer-groups --describe --group hmdp-seckill-orders-v1 2>/dev/null |
        awk -v partitions="$partition_count" -v topic="$topic" '$1 == "hmdp-seckill-orders-v1" && $2 == topic {
            if ($6 ~ /^[0-9]+$/) { total+=$6; seen++ }
            else if ($4=="-" && $5=="0") seen++
        } END { if (seen==partitions) print total+0; else print -1 }' || true)
    printf 'elapsed=%ss lag=%s\n' "$((SECONDS-started_at))" "$lag"
    if [[ "$lag" == 0 ]]; then
        break
    fi
    sleep 5
done

processed=$(mysql_exec "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id BETWEEN $first_voucher AND $last_voucher AND status = 'confirmed';" | tr -d '\r')
db_stock=$(mysql_exec "SELECT SUM(remaining_stock) FROM seckill_activities WHERE id BETWEEN $first_voucher AND $last_voucher;" | tr -d '\r')
dead_letters=$(mysql_exec 'SELECT COUNT(*) FROM seckill_dead_letters;' | tr -d '\r')
if [[ "$lag" != 0 || "$processed" != "$message_count" || "$db_stock" != 0 || "$dead_letters" != 0 ]]; then
    echo "FAIL: lag=$lag confirmed=$processed stock=$db_stock dead_letters=$dead_letters" >&2
    exit 1
fi

echo "PASS: Kafka backlog of $message_count messages drained in $((SECONDS-started_at))s, lag=$lag, confirmed=$processed, MySQL stock=$db_stock, dead letters=$dead_letters"
