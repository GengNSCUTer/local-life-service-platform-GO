# Sourced by stage7-local.sh after it provisions isolated middleware.
run_million_orders() {
    local users=${ORDER_TEST_USERS:-10000} activities=${ORDER_TEST_ACTIVITIES:-100}
    local rate=${ORDER_TEST_RATE:-800} sample_seconds=${ORDER_TEST_SAMPLE_SECONDS:-20}
    local partitions=24 first=9900
    [[ "$users" =~ ^[1-9][0-9]*$ && "$activities" =~ ^[1-9][0-9]*$ && "$rate" =~ ^[1-9][0-9]*$ ]]
    [[ "$sample_seconds" =~ ^[1-9][0-9]*$ && ${ORDER_TEST_DURABLE:-0} =~ ^[01]$ ]]
    local total=$((users * activities))
    (( total <= 1000000 && users <= 100000 && activities <= 100 ))
    "${mysql[@]}" -BN -e 'SELECT @@GLOBAL.innodb_flush_log_at_trx_commit, @@GLOBAL.sync_binlog, @@GLOBAL.log_bin' > "$work/orders-mysql-settings.tsv"
    export KAFKA_CONSUMER_WORKERS=$partitions KAFKA_RELAY_WORKERS=56 MYSQL_MAX_OPEN_CONNS=56
    export SECKILL_PENDING_TIMEOUT=2h SECKILL_AUDIT_INTERVAL=10m
    "$KAFKA_BIN_DIR/kafka-topics.sh" --bootstrap-server "$KAFKA_BROKERS" --create \
        --topic hmdp.seckill.order.v1 --partitions "$partitions" --replication-factor 1
    go build -o "$work/seckill-bench" ./cmd/seckill-bench
    python3 - "$users" <<'PY' | "${mysql[@]}" "$MYSQL_DATABASE"
import sys
count = int(sys.argv[1])
for start in range(1, count+1, 2000):
    rows = ','.join(f"({i}, '199{i:08d}', 'Test User {i}')" for i in range(start, min(start+2000,count+1)))
    print('INSERT INTO tb_user (id, phone, nick_name) VALUES ' + rows + ';')
PY
    seed_sessions "$users" > "$work/sessions.log"
    grep -q "errors: 0, replies: $users" "$work/sessions.log"
    start_api
    start_worker
    local starts ends index
    starts=$(date -u -d "+$((20 + activities)) seconds" +%FT%TZ)
    ends=$(date -u -d '+2 hours' +%FT%TZ)
    for (( index=0; index<activities; index++ )); do
        "$work/api" prepare-seckill -voucher-id "$((first+index))" -stock "$users" -starts-at "$starts" -ends-at "$ends"
    done > "$work/prepare.log" 2>&1
    while (( $(date +%s) < $(date -d "$starts" +%s) )); do sleep .5; done

    local lock_baseline
    lock_baseline=$(order_test_lock_stats)
    local began=$SECONDS
    "$work/seckill-bench" -target "http://$HTTP_ADDR" -voucher-id "$first" -vouchers "$activities" \
        -requests "$total" -workers "$((total < 128 ? total : 128))" -rate "$rate" -users "$users" -token-prefix stage7- \
        > "$work/orders-http.json" 2> "$work/orders-client.log" &
    benchmark_pid=$!
    printf 'elapsed\torders\tconfirmed\tcancelled\tremaining\toutbox\tpending\tkafka_lag\tlock_waits\tlock_wait_ms\tphase\n' > "$work/orders-progress.tsv"
    local progress outbox pending confirmed kafka_lag lock_sample phase=submit
    local baseline_waits baseline_lock_ms waits lock_ms http_reported=0
    IFS=$'\t' read -r baseline_waits baseline_lock_ms <<< "$lock_baseline"
    while true; do
        if [[ "$phase" == http_end ]]; then phase=drain; fi
        if [[ "$phase" == submit ]] && ! kill -0 "$benchmark_pid" 2>/dev/null; then
            phase=http_end
        fi
        local elapsed=$((SECONDS-began))
        progress=$("${mysql[@]}" -BN "$MYSQL_DATABASE" -e "
            SELECT (SELECT COUNT(*) FROM seckill_orders),
                   (SELECT COUNT(*) FROM seckill_orders WHERE status='confirmed'),
                   (SELECT COUNT(*) FROM seckill_orders WHERE status='cancelled'),
                   (SELECT SUM(remaining_stock) FROM seckill_activities)")
        outbox=$("${redis[@]}" ZCARD 'seckill:{orders}:outbox')
        pending=$("${redis[@]}" ZCARD 'seckill:{orders}:timeouts')
        curl -fsS --max-time 5 "http://127.0.0.1:$((port+7))/metrics" > "$work/orders-worker.prom"
        kafka_lag=$(awk -v expected="$partitions" '/^hmdp_seckill_kafka_lag\{.*topic="hmdp.seckill.order.v1"/ {n++; sum+=$2}
            END {if (n==expected) print sum; else print "NA"}' "$work/orders-worker.prom")
        lock_sample=$(order_test_lock_stats)
        IFS=$'\t' read -r waits lock_ms <<< "$lock_sample"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$elapsed" "$progress" "$outbox" "$pending" "$kafka_lag" \
            "$((waits - baseline_waits))" "$((lock_ms - baseline_lock_ms))" "$phase" | tee -a "$work/orders-progress.tsv"
        IFS=$'\t' read -r _ confirmed _ _ <<< "$progress"
        if [[ "$phase" != submit ]]; then
            if (( http_reported == 0 )); then
                wait "$benchmark_pid"
                benchmark_pid=
                cat "$work/orders-http.json"
                python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["accepted"]==int(sys.argv[2]),r' "$work/orders-http.json" "$total"
                http_reported=1
            fi
            if [[ "$confirmed" == "$total" && "$pending" == 0 && "$outbox" == 0 ]]; then break; fi
        fi
        (( SECONDS-began < 5400 )) || { echo 'Orders did not drain within 90 minutes' >&2; return 1; }
        sleep "$sample_seconds"
    done
    local summary
    summary=$("${mysql[@]}" -BN "$MYSQL_DATABASE" -e "
        SELECT (SELECT COUNT(*) FROM seckill_orders), (SELECT COUNT(*) FROM seckill_order_claims),
        (SELECT COUNT(*) FROM seckill_dead_letters), (SELECT COUNT(*) FROM seckill_reservation_anomalies),
        (SELECT COUNT(*) FROM seckill_activities WHERE remaining_stock=0),
        (SELECT COUNT(*) FROM seckill_orders o LEFT JOIN tb_user u ON u.id=o.user_id WHERE u.id IS NULL)")
    printf 'orders\tclaims\tdead_letters\tanomalies\tempty_activities\tunknown_users\n%s\n' "$summary" | tee "$work/orders-summary.tsv"
    [[ "$summary" == "$total"$'\t'"$total"$'\t0\t0\t'"$activities"$'\t0' ]]
    local cursor=0 sample
    for (( sample=0; sample<18; sample++ )); do
        curl -fsS --max-time 5 "http://127.0.0.1:$((port+7))/metrics" > "$work/orders-worker.prom"
        if awk -v expected="$((partitions+1))" '/^hmdp_seckill_kafka_lag\{/ {n++; lag+=$2} END {exit !(n==expected && lag==0)}' "$work/orders-worker.prom"; then
            cursor=1
            break
        fi
        sleep 5
    done
    [[ "$cursor" == 1 ]]
    "$KAFKA_BIN_DIR/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" --describe \
        --group hmdp-seckill-orders-v1 > "$work/orders-offsets-after.txt"
    awk -v expected="$partitions" '$2=="hmdp.seckill.order.v1" {n++; if ($6!="0") failed=1} END {exit (n!=expected || failed)}' "$work/orders-offsets-after.txt"
    for (( index=0; index<activities; index++ )); do
        voucher_id=$((first+index))
        assert_inventory "$users" "$work/audit-$voucher_id.json"
    done > "$work/orders-audits.log"
    printf 'PASS %s confirmed orders and %s consistent activity audits\n' "$total" "$activities"
}

order_test_lock_stats() {
    "${mysql[@]}" -BN -e "SELECT MAX(IF(VARIABLE_NAME='Innodb_row_lock_waits', VARIABLE_VALUE+0, NULL)),
        MAX(IF(VARIABLE_NAME='Innodb_row_lock_time', VARIABLE_VALUE+0, NULL))
        FROM performance_schema.global_status
        WHERE VARIABLE_NAME IN ('Innodb_row_lock_waits', 'Innodb_row_lock_time')"
}
