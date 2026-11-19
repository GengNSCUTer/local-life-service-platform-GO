#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

project=${COMPOSE_PROJECT_NAME:-hmdp-go-sentinel-smoke}
mysql_port=${MYSQL_PORT:-33306}
redis_port=${REDIS_PORT:-36379}
kafka_port=${KAFKA_PORT:-39092}
sentinel_1_port=${REDIS_SENTINEL_1_PORT:-36380}
sentinel_2_port=${REDIS_SENTINEL_2_PORT:-36381}
sentinel_3_port=${REDIS_SENTINEL_3_PORT:-36382}
http_port=${HTTP_PORT:-18080}
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
compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < migrations/009_inventory_audits.sql

master_address() {
    compose exec -T redis-sentinel-1 redis-cli --raw -p 26379 SENTINEL get-master-addr-by-name hmdp-master | tr -d '\r' | paste -sd ':' -
}

starts_at=$(date -u -d '+8 seconds' +%FT%TZ)
ends_at=$(date -u -d '+10 minutes' +%FT%TZ)
go run ./cmd/api prepare-seckill -voucher-id 9101 -stock 2 -starts-at "$starts_at" -ends-at "$ends_at"
compose exec -T redis redis-cli HSET login:token:sentinel-user-1 id 910001 >/dev/null
compose exec -T redis redis-cli EXPIRE login:token:sentinel-user-1 600 >/dev/null
compose exec -T redis redis-cli HSET login:token:sentinel-user-2 id 910002 >/dev/null
compose exec -T redis redis-cli EXPIRE login:token:sentinel-user-2 600 >/dev/null

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

submit() {
    local user=$1
    local token="sentinel-user-$user"
    local body_file="$work_dir/submit-$user.json"
    local code
    code=$(curl -sS --max-time 5 -o "$body_file" -w '%{http_code}' -X POST \
        -H "Authorization: Bearer $token" "http://$HTTP_ADDR/voucher-order/seckill/9101")
    [[ "$code" == 202 ]] || { cat "$body_file" >&2; return 1; }
}

submit 1
before=$(master_address)
compose stop redis >/dev/null
for attempt in $(seq 1 45); do
    after=$(master_address)
    if [[ "$after" != 'redis:6379' && "$after" != 'redis\r:6379' ]]; then
        break
    fi
    sleep 1
done
[[ "$after" != 'redis:6379' && "$after" != 'redis\r:6379' ]] || {
    echo "Sentinel did not promote the replica. Last master address: $after" >&2
    exit 1
}

submitted=0
for attempt in $(seq 1 30); do
    if submit 2; then
        submitted=1
        break
    fi
    sleep 1
done
[[ "$submitted" == 1 ]]

for attempt in $(seq 1 60); do
    confirmed=$(mysql_exec "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id = 9101 AND status = 'confirmed';" | tr -d '\r')
    if [[ "$confirmed" == 2 ]]; then
        break
    fi
    sleep 1
done
[[ "$confirmed" == 2 ]]

replica_stock=$(compose exec -T redis-replica redis-cli HGET seckill:{orders}:activity:9101 stock | tr -d '\r')
[[ "$replica_stock" == 0 ]]

echo "PASS: Sentinel master $before failed over to $after, API reconnected, and both orders confirmed"
