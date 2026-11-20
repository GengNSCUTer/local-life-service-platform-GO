# Sourced by stage7-local.sh after it provisions isolated middleware.
run_capacity() {
    local duration=${CAPACITY_SECONDS:-20}
    local rates=${CAPACITY_RATES:-'5 15 30'}
    local second_api="127.0.0.1:$((port+8))"
    local second_worker="127.0.0.1:$((port+9))"
    local -a rate_steps
    read -r -a rate_steps <<< "$rates"
    [[ "$duration" =~ ^[0-9]+$ && "$duration" -ge 10 && ${#rate_steps[@]} -gt 0 ]]
    local rate stock=0
    for rate in "${rate_steps[@]}"; do
        [[ "$rate" =~ ^[1-9][0-9]*$ ]]
        stock=$((stock > duration * rate ? stock : duration * rate))
    done
    seed_sessions "$stock"
    HTTP_ADDR="$second_api" KAFKA_BROKERS= OBSERVABILITY_ADDR= "$work/api" >"$work/capacity-api-2.log" 2>&1 & second_pid=$!
    WORKER_HEALTH_ADDR="$second_worker" KAFKA_CONSUMER_WORKERS=1 OBSERVABILITY_ADDR="127.0.0.1:$((port+10))" \
        "$work/worker" >"$work/capacity-worker-2.log" 2>&1 & second_worker_pid=$!
    wait_ready "$second_api"
    wait_ready "$second_worker"
    wait_empty

    local step=0 result after_http confirmed stock_file profile_pid
    printf 'offered_per_second\tseconds\taccepted\thttp_p99_ms\tconfirmed_at_http_end\tdrain_seconds\tconfirmed_final\n' > "$work/capacity.tsv"
    for rate in "${rate_steps[@]}"; do
        step=$((step+1))
        voucher_id=$((9700+step))
        stock=$((duration*rate))
        prepare_capacity_activity "$stock"
        if [[ "$step" == "${#rate_steps[@]}" ]]; then
            collect_capacity_profiles & profile_pid=$!
        fi
        result="$work/capacity-$step.jsonl"
        python3 scripts/seckill-soak.py --port "$port" --port "$((port+8))" --voucher "$voucher_id" \
            --seconds "$duration" --rate "$rate" --prefix stage7- | tee "$result"
        after_http=$SECONDS
        confirmed=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM seckill_orders WHERE voucher_id=$voucher_id AND status='confirmed'")
        wait_confirmed "$stock"
        stock_file="$work/audit-$step.json"
        wait_empty
        assert_inventory "$stock" "$stock_file"
        python3 - "$result" "$rate" "$stock" "$confirmed" "$((SECONDS-after_http))" >> "$work/capacity.tsv" <<'PY'
import json
import sys
from pathlib import Path

result, rate, stock, confirmed, drain = sys.argv[1:]
row = json.loads(Path(result).read_text().splitlines()[-1])
per_api = row['acceptedByPort']
assert len(per_api) == 2 and max(per_api.values()) - min(per_api.values()) <= 1, row
print(rate, row['seconds'], row['accepted'], row['p99Ms'], confirmed, drain, stock, sep='\t')
PY
    done
    wait "$profile_pid"
    cat "$work/capacity.tsv"
    run_mysql_recovery
}

prepare_capacity_activity() {
    local starts
    starts=$(date -u -d '+4 seconds' +%FT%TZ)
    "$work/api" prepare-seckill -voucher-id "$voucher_id" -stock "$1" -starts-at "$starts" -ends-at "$(date -u -d '+30 minutes' +%FT%TZ)"
    while (( $(date +%s) < $(date -d "$starts" +%s) )); do sleep .2; done
}

collect_capacity_profiles() {
    local api_metrics="http://127.0.0.1:$((port+6))" worker_metrics="http://127.0.0.1:$((port+7))" api_profile worker_profile
    curl -fsS --max-time 15 "$api_metrics/debug/pprof/profile?seconds=6" -o "$work/api.cpu.pprof" & api_profile=$!
    curl -fsS --max-time 15 "$worker_metrics/debug/pprof/profile?seconds=6" -o "$work/worker.cpu.pprof" & worker_profile=$!
    wait "$api_profile" "$worker_profile"
    curl -fsS --max-time 5 "$api_metrics/debug/pprof/heap" -o "$work/api.heap.pprof"
    curl -fsS --max-time 5 "$worker_metrics/debug/pprof/heap" -o "$work/worker.heap.pprof"
}

run_mysql_recovery() {
    voucher_id=9800
    prepare_capacity_activity 1
    stop "$mysql_pid"; mysql_pid=
    local code order_id
    code=$(curl -sS --max-time 5 -o "$work/mysql-down-ready.json" -w '%{http_code}' "http://$HTTP_ADDR/health/ready")
    [[ "$code" == 503 ]]
    curl -fsS --max-time 5 "http://$HTTP_ADDR/health/live" >/dev/null
    order_id=$(submit "$HTTP_ADDR" 1)
    [[ -n "$order_id" ]]
    [[ $("${redis[@]}" ZSCORE 'seckill:{orders}:timeouts' "$order_id") != '' ]]
    run_mysql "$work/source/mysql"
    wait_ready "$HTTP_ADDR"
    wait_confirmed 1
    wait_empty
    assert_inventory 1 "$work/mysql-restored-audit.json"
    [[ $(status 1 "$order_id") == confirmed ]]
    printf 'PASS: MySQL stopped, reservation accepted, original database restarted and order confirmed\n'
}
