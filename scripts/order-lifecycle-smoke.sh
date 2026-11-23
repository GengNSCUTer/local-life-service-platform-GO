#!/usr/bin/env bash
set -Eeuo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

project=hmdp-go-order-lifecycle-smoke
mysql_port=${MYSQL_PORT:-33336}
redis_port=${REDIS_PORT:-36409}
kafka_port=${KAFKA_PORT:-39392}
sentinel_1_port=${REDIS_SENTINEL_1_PORT:-36410}
sentinel_2_port=${REDIS_SENTINEL_2_PORT:-36411}
sentinel_3_port=${REDIS_SENTINEL_3_PORT:-36412}
http_port=${HTTP_PORT:-18110}
password=${REDIS_PASSWORD:-hmdp_dev_redis}

export MYSQL_PORT="$mysql_port" REDIS_PORT="$redis_port" KAFKA_PORT="$kafka_port"
export REDIS_SENTINEL_1_PORT="$sentinel_1_port" REDIS_SENTINEL_2_PORT="$sentinel_2_port" REDIS_SENTINEL_3_PORT="$sentinel_3_port"
export MYSQL_ADDR="127.0.0.1:$mysql_port" MYSQL_USER="${MYSQL_USER:-hmdp}" MYSQL_DATABASE="${MYSQL_DATABASE:-hmdp}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-hmdp_dev_mysql}" REDIS_PASSWORD="$password"
export REDIS_SENTINEL_PASSWORD= REDIS_MASTER_NAME="hmdp-master" REDIS_SENTINEL_ADDRS="127.0.0.1:$sentinel_1_port,127.0.0.1:$sentinel_2_port,127.0.0.1:$sentinel_3_port"
export KAFKA_BROKERS="127.0.0.1:$kafka_port" HTTP_ADDR="127.0.0.1:$http_port"
export SECKILL_PENDING_TIMEOUT=120s

source scripts/smoke-compose.sh
mysql_exec() {
    local query=$1
    compose exec -T -e MYSQL_QUERY="$query" mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -N -u "$MYSQL_USER" "$MYSQL_DATABASE" -e "$MYSQL_QUERY"'
}
redis_exec() { compose exec -T redis redis-cli "$@"; }

submit() {
    local token=$1
    local output="$work_dir/submit-$token.json"
    local code
    code=$(curl -sS --max-time 5 -o "$output" -w '%{http_code}' -X POST \
        -H "Authorization: Bearer $token" "http://$HTTP_ADDR/voucher-order/seckill/$voucher_id")
    [[ "$code" == 202 ]] || { cat "$output" >&2; return 1; }
    python3 - "$output" <<'PY'
import json
import sys
with open(sys.argv[1]) as source:
    print(json.load(source)["data"]["orderId"])
PY
}

wait_status() {
    local token=$1 order_id=$2 wanted=$3
    local output="$work_dir/status-$order_id.json"
    local deadline=$((SECONDS + ${4:-20}))
    while (( SECONDS < deadline )); do
        local code
        code=$(curl -sS --max-time 3 -o "$output" -w '%{http_code}' \
            -H "Authorization: Bearer $token" "http://$HTTP_ADDR/voucher-order/$order_id" || true)
        if [[ "$code" == 200 ]]; then
            local status
            status=$(python3 - "$output" <<'PY'
import json
import sys
with open(sys.argv[1]) as source:
    print(json.load(source)["data"]["status"])
PY
)
            if [[ "$status" == "$wanted" ]]; then
                return 0
            fi
        fi
        sleep 0.25
    done
    cat "$output" >&2
    return 1
}

smoke_start

voucher_id=$(date +%s%N)
ends_at=$(date -u -d '+15 minutes' +%FT%TZ)
starts_at=$(date -u -d '+6 seconds' +%FT%TZ)
"$work_dir/api" prepare-seckill -voucher-id "$voucher_id" -stock 5 -starts-at "$starts_at" -ends-at "$ends_at"

for user_id in 940001 940002 940003 940004; do
    token="phase4-$user_id"
    redis_exec HSET "login:token:$token" id "$user_id" nickName "user-$user_id" >/dev/null
    redis_exec EXPIRE "login:token:$token" 600 >/dev/null
done

"$work_dir/all" >"$work_dir/api.log" 2>&1 &
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

first_order=$(submit phase4-940001)
wait_status phase4-940001 "$first_order" confirmed
cancel_file="$work_dir/cancel-first.json"
cancel_code=$(curl -sS --max-time 5 -o "$cancel_file" -w '%{http_code}' -X POST \
    -H 'Authorization: Bearer phase4-940001' "http://$HTTP_ADDR/voucher-order/$first_order/cancel")
[[ "$cancel_code" == 200 ]]
wait_status phase4-940001 "$first_order" cancelled
replacement_order=$(submit phase4-940001)
wait_status phase4-940001 "$replacement_order" confirmed

compose stop kafka >/dev/null
recovered_order=$(submit phase4-940002)
wait_status phase4-940002 "$recovered_order" pending
recovered_payload=$(redis_exec GET "seckill:{orders}:order:$recovered_order")
[[ -n "$recovered_payload" ]]
compose start kafka >/dev/null
wait_status phase4-940002 "$recovered_order" confirmed 100
printf '%s\t%s\n' "$recovered_order" "$recovered_payload" | compose exec -T kafka sh -c \
    '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic hmdp.seckill.order.v1 --property parse.key=true'
for attempt in $(seq 1 40); do
    lag=$(compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
        --describe --group hmdp-seckill-orders-v1 2>/dev/null | awk \
        '$1 == "hmdp-seckill-orders-v1" && $2 == "hmdp.seckill.order.v1" {
            if ($6 ~ /^[0-9]+$/) { sum += $6; seen++ }
            else if ($4 == "-" && $5 == "0") { seen++ }
        }
        END { if (seen == 3) print sum; else print -1 }' || true)
    [[ "$lag" != 0 ]] || break
    sleep 0.25
done
[[ "$lag" == 0 ]]

compose pause kafka >/dev/null
timed_out_order=$(submit phase4-940003)
redis_exec DEL "seckill:{orders}:order:$timed_out_order" >/dev/null
redis_exec ZADD 'seckill:{orders}:timeouts' 0 "$timed_out_order" >/dev/null
wait_status phase4-940003 "$timed_out_order" cancelled
compose unpause kafka >/dev/null

retry_order=$(submit phase4-940004)
wait_status phase4-940004 "$retry_order" confirmed
activity_key="seckill:{orders}:activity:$voucher_id"
redis_exec RENAME "$activity_key" "$activity_key:hidden" >/dev/null
retry_file="$work_dir/cancel-retry.json"
retry_code=$(curl -sS --max-time 8 -o "$retry_file" -w '%{http_code}' -X POST \
    -H 'Authorization: Bearer phase4-940004' "http://$HTTP_ADDR/voucher-order/$retry_order/cancel")
[[ "$retry_code" == 202 ]]
redis_exec RENAME "$activity_key:hidden" "$activity_key" >/dev/null

redis_synced=0
for attempt in $(seq 1 40); do
    redis_synced=$(mysql_exec "SELECT redis_synced FROM seckill_order_resolutions WHERE order_id = '$retry_order';" | tr -d '\r')
    if [[ "$redis_synced" == 1 ]]; then
        break
    fi
    sleep 0.25
done
[[ "$redis_synced" == 1 ]]

db_stock=$(mysql_exec "SELECT remaining_stock FROM seckill_activities WHERE id = $voucher_id;" | tr -d '\r')
redis_stock=$(redis_exec HGET "$activity_key" stock | tr -d '\r')
active_claims=$(mysql_exec "SELECT COUNT(*) FROM seckill_order_claims WHERE voucher_id = $voucher_id;" | tr -d '\r')
timed_out_claims=$(mysql_exec "SELECT COUNT(*) FROM seckill_order_claims WHERE order_id = '$timed_out_order';" | tr -d '\r')
orders=$(mysql_exec "SELECT SUM(status = 'confirmed'), SUM(status = 'cancelled') FROM seckill_orders WHERE voucher_id = $voucher_id;" | tr -d '\r')

[[ "$db_stock" == 3 ]]
[[ "$redis_stock" == 3 ]]
[[ "$active_claims" == 2 ]]
[[ "$timed_out_claims" == 0 ]]
[[ "$orders" == $'2\t3' ]]

echo "PASS: replacement=$replacement_order, Kafka recovery=$recovered_order, timeout=$timed_out_order, compensation retry=$retry_order, stock=$db_stock"
