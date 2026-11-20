# Sourced by stage7-local.sh after the isolated stack is ready.
run_worker_scale() {
    local rate=${WORKER_SCALE_RATE:-600} duration=${WORKER_SCALE_SECONDS:-24}
    local activities=24 partitions=12
    [[ "$rate" =~ ^[1-9][0-9]*$ && "$duration" =~ ^[1-9][0-9]*$ ]]
    (( rate <= 2000 && duration >= 10 && duration <= 120 && (rate * duration) % activities == 0 ))
    local total=$((rate * duration)) stock=$((rate * duration / activities))
    # Four consumers per replica. Keep aggregate relay, SQL pool and Go scheduler budgets fixed.
    export KAFKA_RELAY_WORKERS=24 MYSQL_MAX_OPEN_CONNS=48 SECKILL_PENDING_TIMEOUT=15m SECKILL_AUDIT_INTERVAL=30m
    "$KAFKA_BIN_DIR/kafka-topics.sh" --bootstrap-server "$KAFKA_BROKERS" --create \
        --topic hmdp.seckill.order.v1 --partitions "$partitions" --replication-factor 1
    go build -o "$work/seckill-bench" ./cmd/seckill-bench
    python3 - "$stock" <<'PY' | "${mysql[@]}" "$MYSQL_DATABASE"
import sys
count = int(sys.argv[1])
for start in range(1, count + 1, 2000):
    rows = ','.join(f"({i},'199{i:08d}')" for i in range(start, min(start + 2000, count + 1)))
    print('INSERT INTO tb_user (id, phone) VALUES ' + rows + ';')
PY
    seed_sessions "$stock" > "$work/sessions.log"
    grep -q "errors: 0, replies: $stock" "$work/sessions.log"
    start_api
    local round=0 replicas first last starts ends activity
    local start_time
    starts=$(date -u -d '+75 seconds' +%FT%TZ)
    start_time=$(date -d "$starts" +%s)
    ends=$(date -u -d '+2 hours' +%FT%TZ)
    for (( activity=10000; activity<10000+6*activities; activity++ )); do
        "$work/api" prepare-seckill -voucher-id "$activity" -stock "$stock" -starts-at "$starts" -ends-at "$ends"
    done > "$work/prepare.log" 2>&1
    while (( $(date +%s) < start_time )); do sleep .5; done
    for replicas in 1 2 3 3 2 1; do
        round=$((round + 1))
        first=$((10000 + (round - 1) * activities))
        last=$((first + activities - 1))
        printf 'Round %s, %s Worker processes, %s requests at %s/s\n' "$round" "$replicas" "$total" "$rate"
        run_worker_scale_round "$round" "$replicas" "$first" "$last" "$total" "$rate" "$stock" "$partitions"
    done
}

run_worker_scale_round() {
    local round=$1 replicas=$2 first=$3 last=$4 total=$5 rate=$6 stock=$7 partitions=$8
    local folder="$work/round-$round-$replicas" worker health metrics pid
    mkdir -p "$folder"
    local -a health_ports=(5 8 9)
    for (( worker=0; worker<replicas; worker++ )); do
        health=$((port + health_ports[worker]))
        metrics=
        # All replicas read the same group lag, so scrape the first only.
        if (( worker == 0 )); then metrics="127.0.0.1:$((port + 7))"; fi
        OBSERVABILITY_ADDR="$metrics" WORKER_HEALTH_ADDR="127.0.0.1:$health" GOMAXPROCS=$((12 / replicas)) \
            KAFKA_CONSUMER_WORKERS=4 KAFKA_RELAY_WORKERS=$((24 / replicas)) MYSQL_MAX_OPEN_CONNS=$((48 / replicas)) \
            "$work/worker" > "$folder/worker-$worker.log" 2>&1 &
        scale_worker_pids+=("$!")
    done
    for (( worker=0; worker<replicas; worker++ )); do
        wait_ready "127.0.0.1:$((port + health_ports[worker]))"
    done
    local group_ready=0
    for (( worker=0; worker<6; worker++ )); do
        "$KAFKA_BIN_DIR/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
            --describe --state --group hmdp-seckill-orders-v1 > "$folder/group.txt"
        if awk -v n="$((replicas * 4))" '$1=="hmdp-seckill-orders-v1" && $(NF-1)=="Stable" && $NF==n {ok=1} END {exit !ok}' "$folder/group.txt"; then
            group_ready=1
            break
        fi
        sleep 2
    done
    [[ "$group_ready" == 1 ]]
    local began phases=0 confirmed cancelled waits lock_ms current outbox pending lag elapsed deadline drained_at=0 kafka_sample
    printf 'phase\telapsed_ms\tconfirmed\tcancelled\tkafka_lag\tredis_outbox\tredis_pending\tlock_waits\tlock_ms\tcurrent_waits\n' > "$folder/samples.tsv"
    began=$(date +%s%3N)
    worker_scale_sample baseline
    began=$(date +%s%3N)
    deadline=$((began + 300000))
    "$work/seckill-bench" -target "http://$HTTP_ADDR" -voucher-id "$first" -vouchers "$((last - first + 1))" \
        -requests "$total" -workers 128 -rate "$rate" -users "$stock" -token-prefix stage7- > "$folder/http.json" 2> "$folder/client.log" &
    benchmark_pid=$!
    while true; do
        if (( phases == 0 )) && ! kill -0 "$benchmark_pid" 2>/dev/null; then
            wait "$benchmark_pid"
            benchmark_pid=
            python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["accepted"]==int(sys.argv[2]), r' "$folder/http.json" "$total"
            phases=1
            worker_scale_sample http_end
        elif (( phases == 0 )); then
            worker_scale_sample submit
        else
            worker_scale_sample drain
        fi
        if (( phases == 1 && confirmed == total && pending == 0 && outbox == 0 )); then
            if (( drained_at == 0 )); then drained_at=$(date +%s); fi
            if (( lag == 0 && kafka_sample > drained_at )); then break; fi
        fi
        (( $(date +%s%3N) < deadline )) || { echo 'Worker scale round exceeded five minutes' >&2; return 1; }
        sleep 1
    done
    "$KAFKA_BIN_DIR/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
        --describe --group hmdp-seckill-orders-v1 > "$folder/offsets-after.txt"
    awk -v n="$partitions" '$2=="hmdp.seckill.order.v1" {partitions++; if ($6!="0") failed=1} END {exit (partitions!=n || failed)}' "$folder/offsets-after.txt"
    for pid in "${scale_worker_pids[@]}"; do stop "$pid"; done
    scale_worker_pids=()
    local voucher_id
    for (( voucher_id=first; voucher_id<=last; voucher_id++ )); do
        assert_inventory "$stock" "$folder/audit-$voucher_id.json"
    done > "$folder/audits.log"
    [[ "$cancelled" == 0 ]]
    [[ $("${mysql[@]}" -BN "$MYSQL_DATABASE" -e 'SELECT (SELECT COUNT(*) FROM seckill_dead_letters) + (SELECT COUNT(*) FROM seckill_reservation_anomalies)') == 0 ]]
    printf 'PASS round %s, %s confirmed, queues empty\n' "$round" "$total"
}

worker_scale_sample() {
    local phase=$1 sql
    elapsed=$(( $(date +%s%3N) - began ))
    sql=$("${mysql[@]}" -BN "$MYSQL_DATABASE" -e "
        SELECT (SELECT COUNT(*) FROM seckill_orders WHERE voucher_id BETWEEN $first AND $last AND status='confirmed'),
               (SELECT COUNT(*) FROM seckill_orders WHERE voucher_id BETWEEN $first AND $last AND status='cancelled'),
               MAX(IF(VARIABLE_NAME='Innodb_row_lock_waits', VARIABLE_VALUE+0, NULL)),
               MAX(IF(VARIABLE_NAME='Innodb_row_lock_time', VARIABLE_VALUE+0, NULL)),
               MAX(IF(VARIABLE_NAME='Innodb_row_lock_current_waits', VARIABLE_VALUE+0, NULL))
        FROM performance_schema.global_status
        WHERE VARIABLE_NAME IN ('Innodb_row_lock_waits', 'Innodb_row_lock_time', 'Innodb_row_lock_current_waits')")
    IFS=$'\t' read -r confirmed cancelled waits lock_ms current <<< "$sql"
    [[ "$waits" =~ ^[0-9]+$ && "$lock_ms" =~ ^[0-9]+$ && "$current" =~ ^[0-9]+$ ]]
    outbox=$("${redis[@]}" ZCARD 'seckill:{orders}:outbox')
    pending=$("${redis[@]}" ZCARD 'seckill:{orders}:timeouts')
    curl -fsS --max-time 5 "http://127.0.0.1:$((port + 7))/metrics" > "$folder/worker.prom"
    lag=$(awk -v expected="$partitions" '/^hmdp_seckill_kafka_lag\{.*topic="hmdp.seckill.order.v1"/ {n++; sum+=$2}
        END {if (n==expected) print sum; else print -1}' "$folder/worker.prom")
    kafka_sample=$(awk '/^hmdp_seckill_monitor_last_success_timestamp_seconds\{source="kafka"/ {printf "%.0f", $2}' "$folder/worker.prom")
    kafka_sample=${kafka_sample:-0}
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$phase" "$elapsed" "$confirmed" "$cancelled" "$lag" "$outbox" "$pending" "$waits" "$lock_ms" "$current" >> "$folder/samples.tsv"
}
