#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

project=${COMPOSE_PROJECT_NAME:-hmdp-go-compensation-smoke}
mysql_port=${MYSQL_PORT:-33326}
redis_port=${REDIS_PORT:-36399}
kafka_port=${KAFKA_PORT:-39292}
sentinel_1_port=${REDIS_SENTINEL_1_PORT:-36400}
sentinel_2_port=${REDIS_SENTINEL_2_PORT:-36401}
sentinel_3_port=${REDIS_SENTINEL_3_PORT:-36402}
http_port=${HTTP_PORT:-18100}
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
compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/007_reservation_anomalies.sql
compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/008_social.sql

voucher_id=9301
starts_at=$(date -u -d '+8 seconds' +%FT%TZ)
ends_at=$(date -u -d '+10 minutes' +%FT%TZ)
go run ./cmd/api prepare-seckill -voucher-id "$voucher_id" -stock 2 -starts-at "$starts_at" -ends-at "$ends_at"

mysql_exec "UPDATE seckill_activities SET remaining_stock = 1 WHERE id = $voucher_id;"
mysql_exec "INSERT INTO seckill_orders (id, voucher_id, user_id, status, created_at) VALUES ('conflict-existing', $voucher_id, 930001, 'confirmed', UTC_TIMESTAMP(3));"
mysql_exec "INSERT INTO seckill_order_claims (voucher_id, user_id, order_id, created_at) VALUES ($voucher_id, 930001, 'conflict-existing', UTC_TIMESTAMP(3));"
compose exec -T redis redis-cli HSET login:token:compensation-user id 930001 >/dev/null
compose exec -T redis redis-cli EXPIRE login:token:compensation-user 600 >/dev/null

go run ./cmd/api >"$work_dir/api.log" 2>&1 &
api_pid=$!
for attempt in $(seq 1 60); do
    if curl -fsS --max-time 2 "http://$HTTP_ADDR/health/ready" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
curl -fsS --max-time 2 "http://$HTTP_ADDR/health/ready" >/dev/null
while (( $(date -u +%s) < $(date -u -d "$starts_at" +%s) )); do
    sleep 0.2
done

body_file="$work_dir/submit.json"
code=$(curl -sS --max-time 5 -o "$body_file" -w '%{http_code}' -X POST \
    -H 'Authorization: Bearer compensation-user' "http://$HTTP_ADDR/voucher-order/seckill/$voucher_id")
[[ "$code" == 202 ]] || { cat "$body_file" >&2; exit 1; }
event_id=$(python3 - "$body_file" <<'PY'
import json
import sys
with open(sys.argv[1]) as source:
    print(json.load(source)["data"]["orderId"])
PY
)

outcome=''
for attempt in $(seq 1 60); do
    outcome=$(mysql_exec "SELECT outcome FROM seckill_dead_letters WHERE voucher_id = $voucher_id ORDER BY id DESC LIMIT 1;" | tr -d '\r' || true)
    if [[ "$outcome" == compensated ]]; then
        break
    fi
    sleep 1
done
[[ "$outcome" == compensated ]]
redis_stock=$(compose exec -T redis redis-cli HGET "seckill:{orders}:activity:$voucher_id" stock | tr -d '\r')
buyer_exists=$(compose exec -T redis redis-cli HEXISTS "seckill:{orders}:activity:$voucher_id:buyers" 930001 | tr -d '\r')
order_key_exists=$(compose exec -T redis redis-cli EXISTS "seckill:{orders}:order:$event_id" | tr -d '\r')
db_stock=$(mysql_exec "SELECT remaining_stock FROM seckill_activities WHERE id = $voucher_id;" | tr -d '\r')
[[ "$redis_stock" == 2 ]]
[[ "$buyer_exists" == 0 ]]
[[ "$order_key_exists" == 0 ]]
[[ "$db_stock" == 1 ]]

echo "PASS: order $event_id was automatically compensated, Redis stock=$redis_stock, buyer reservation=$buyer_exists, event key=$order_key_exists, MySQL stock=$db_stock"
