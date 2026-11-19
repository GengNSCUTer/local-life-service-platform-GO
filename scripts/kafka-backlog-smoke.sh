#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

project=${COMPOSE_PROJECT_NAME:-hmdp-go-kafka-backlog-smoke}
message_count=${BACKLOG_MESSAGES:-10000}
mysql_port=${MYSQL_PORT:-33316}
redis_port=${REDIS_PORT:-36389}
kafka_port=${KAFKA_PORT:-39192}
sentinel_1_port=${REDIS_SENTINEL_1_PORT:-36390}
sentinel_2_port=${REDIS_SENTINEL_2_PORT:-36391}
sentinel_3_port=${REDIS_SENTINEL_3_PORT:-36392}
http_port=${HTTP_PORT:-18090}
password=${REDIS_PASSWORD:-hmdp_dev_redis}
api_pid=''
work_dir=$(mktemp -d)

export MYSQL_PORT="$mysql_port" REDIS_PORT="$redis_port" KAFKA_PORT="$kafka_port"
export REDIS_SENTINEL_1_PORT="$sentinel_1_port" REDIS_SENTINEL_2_PORT="$sentinel_2_port" REDIS_SENTINEL_3_PORT="$sentinel_3_port"
export MYSQL_ADDR="127.0.0.1:$mysql_port" MYSQL_USER="${MYSQL_USER:-hmdp}" MYSQL_DATABASE="${MYSQL_DATABASE:-hmdp}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-hmdp_dev_mysql}" REDIS_PASSWORD="$password"
export REDIS_MASTER_NAME="hmdp-master" REDIS_SENTINEL_ADDRS="127.0.0.1:$sentinel_1_port,127.0.0.1:$sentinel_2_port,127.0.0.1:$sentinel_3_port"
export KAFKA_BROKERS="127.0.0.1:$kafka_port" HTTP_ADDR="127.0.0.1:$http_port"

compose() { docker compose -p "$project" "$@"; }
mysql_exec() {
    local query=$1
    compose exec -T -e MYSQL_QUERY="$query" mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -u "$MYSQL_USER" "$MYSQL_DATABASE" -e "$MYSQL_QUERY"'
}
cleanup() {
    if [[ -n "$api_pid" ]]; then
        kill -TERM "$api_pid" 2>/dev/null || true
        wait "$api_pid" 2>/dev/null || true
    fi
    compose down --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$work_dir"
}
trap cleanup EXIT

if ! docker info >/dev/null 2>&1; then
    echo 'Docker daemon is unavailable. Start Docker and rerun this script.' >&2
    exit 1
fi

compose up -d --wait mysql redis redis-replica redis-sentinel-1 redis-sentinel-2 redis-sentinel-3 kafka
compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/001_seckill.sql
compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/002_seckill_dead_letters.sql
compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/006_order_lifecycle.sql

voucher_id=9201
starts_at=$(date -u -d '+8 seconds' +%FT%TZ)
ends_at=$(date -u -d '+30 minutes' +%FT%TZ)
go run ./cmd/api prepare-seckill -voucher-id "$voucher_id" -stock "$message_count" -starts-at "$starts_at" -ends-at "$ends_at"

python3 - "$work_dir/events.txt" "$message_count" "$voucher_id" <<'PY'
import base64
import json
import pathlib
import sys
from datetime import datetime, timezone

path, count, voucher_id = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
occurred_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
with pathlib.Path(path).open("w") as output:
    for index in range(1, count + 1):
        order_id = base64.b32encode(index.to_bytes(16, "big")).decode().rstrip("=")
        event = {
            "schema_version": 1,
            "order_id": order_id,
            "voucher_id": voucher_id,
            "user_id": 700000 + index,
            "occurred_at": occurred_at,
        }
        output.write(order_id + "\t" + json.dumps(event, separators=(",", ":")) + "\n")
PY

compose exec -T kafka sh -c '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic hmdp.seckill.order.v1 --property parse.key=true' < "$work_dir/events.txt"

started_at=$(date +%s)
go run ./cmd/api >"$work_dir/api.log" 2>&1 &
api_pid=$!
for attempt in $(seq 1 60); do
    if curl -fsS --max-time 2 "http://$HTTP_ADDR/health/ready" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
curl -fsS --max-time 2 "http://$HTTP_ADDR/health/ready" >/dev/null

lag=1
for attempt in $(seq 1 180); do
    lag=$(compose exec -T kafka sh -c '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group hmdp-seckill-orders-v1 2>/dev/null' \
        | awk '$1 == "hmdp-seckill-orders-v1" && $2 == "hmdp.seckill.order.v1" { total += $6; partitions++ } END { if (partitions == 3) print total; else print -1 }' || true)
    if [[ "$lag" == 0 ]]; then
        break
    fi
    sleep 1
done

processed=$(mysql_exec "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id = $voucher_id AND status = 'confirmed';" | tr -d '\r')
db_stock=$(mysql_exec "SELECT remaining_stock FROM seckill_activities WHERE id = $voucher_id;" | tr -d '\r')
end_at=$(date +%s)
[[ "$lag" == 0 ]]
[[ "$processed" == "$message_count" ]]
[[ "$db_stock" == 0 ]]

echo "PASS: Kafka backlog of $message_count messages drained in $((end_at - started_at))s, lag=$lag, confirmed=$processed, MySQL stock=$db_stock"
