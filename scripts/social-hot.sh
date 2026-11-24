#!/usr/bin/env bash
# Sourced by the isolated stage7 harness.
run_social_hot() {
    local duration=${SOCIAL_HOT_SECONDS:-30}
    local rate=${SOCIAL_HOT_RATE:-400}
    local workload=${SOCIAL_HOT_WORKLOAD:-mixed}
    local api_count=${SOCIAL_HOT_APIS:-2}
    local order_rate=${SOCIAL_HOT_ORDER_RATE:-0}
    local stock=$((duration * order_rate))
    local second_addr="127.0.0.1:$((port+8))"
    local -a targets=("$port")
    if [[ "$api_count" == 2 ]]; then
        HTTP_ADDR="$second_addr" OBSERVABILITY_ADDR="127.0.0.1:$((port+9))" "$work/api" >"$work/api-second.log" 2>&1 & second_pid=$!
        wait_ready "$second_addr"
        targets+=("$((port+8))")
    fi
    python3 scripts/social_hot.py seed "${SOCIAL_HOT_BLOGS:-1000000}" "${SOCIAL_HOT_COMMENTS:-100000}" | "${mysql[@]}" "$MYSQL_DATABASE"
    seed_sessions "$((stock+4096))" > "$work/sessions.log"
    python3 scripts/social_hot.py verify "$port" "$((port+8))" "$api_count" | tee "$work/social-smoke.log"
    if [[ "${SOCIAL_HOT_FAULTS:-0}" == 1 ]]; then
        social_hot_failures
    fi
    if [[ "$workload" == smoke ]]; then return; fi
    if ((order_rate > 0)); then
        local starts
        starts=$(date -u -d '+5 seconds' +%FT%TZ)
        "$work/api" prepare-seckill -voucher-id "$voucher_id" -stock "$stock" -starts-at "$starts" \
            -ends-at "$(date -u -d "+$((duration+600)) seconds" +%FT%TZ)"
        while (( $(date +%s) < $(date -d "$starts" +%s) )); do sleep .2; done
        python3 scripts/seckill-soak.py --port "$port" --voucher "$voucher_id" --seconds "$duration" --rate "$order_rate" --prefix stage7- > "$work/seckill-soak.jsonl" & benchmark_pid=$!
    fi
    social_hot_snapshot before
    python3 scripts/social_hot.py load "$rate" "$duration" "$workload" "$((stock+1))" "${targets[@]}" | tee "$work/social-hot.json"
    if ((order_rate > 0)); then
        wait "$benchmark_pid"
        benchmark_pid=
        wait_confirmed "$stock"
        wait_empty
        assert_inventory "$stock"
        tail -1 "$work/seckill-soak.jsonl"
    fi
    for attempt in {1..60}; do
        if [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM social_blog_counter_buckets WHERE dirty_since IS NOT NULL') == 0 ]]; then break; fi
        sleep .25
    done
    local audit
    audit=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM tb_blog AS b WHERE b.id <= 33 AND
        (b.liked != (SELECT COUNT(*) FROM social_blog_likes l WHERE l.blog_id = b.id) OR
         b.comments != (SELECT COUNT(*) FROM social_blog_comments c WHERE c.blog_id = b.id AND c.deleted_at IS NULL))')
    printf 'counter mismatches: %s\n' "$audit" | tee "$work/social-counter-audit.txt"
    [[ "$audit" == 0 ]]
    social_hot_snapshot after
}

social_hot_failures() {
    local blog=0
    local token='stage7-1'
    "${mysql[@]}" "$MYSQL_DATABASE" -e 'INSERT IGNORE INTO social_follows (user_id, follow_user_id) SELECT id, 1 FROM tb_user WHERE id BETWEEN 2 AND 4096'
    stop "$worker_pid"; worker_pid=
    blog=$(curl -fsS -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
        --data '{"shopId":1,"title":"Outage test","content":"fixture"}' "http://$HTTP_ADDR/blog" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_blog_outbox WHERE blog_id=$blog") == 1 ]]
    "${redis[@]}" CLIENT PAUSE 3000 WRITE >/dev/null
    start_worker
    for attempt in {1..100}; do
        if [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_fanout_tasks WHERE blog_id=$blog AND due_at IS NULL") == 1 ]]; then break; fi
        sleep .2
    done
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_fanout_tasks WHERE blog_id=$blog AND due_at IS NULL") == 1 ]]
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_blog_outbox WHERE blog_id=$blog") == 0 ]]
    for id in 2 2048 4096; do
        [[ -n $("${redis[@]}" ZSCORE "social:notifications:$id" "$blog") ]]
    done
    printf 'Worker stopped during publish, outbox persisted, Redis write pause, task completion and notification samples passed\n' | tee "$work/social-faults.log"
}

social_hot_snapshot() {
    "${mysql[@]}" -N -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_row_lock_time','Innodb_row_lock_waits','Innodb_rows_read','Com_select')" > "$work/social-mysql-$1.tsv"
    curl -fsS "http://127.0.0.1:$((port+6))/metrics" > "$work/social-api-$1.prom"
    curl -fsS "http://127.0.0.1:$((port+7))/metrics" > "$work/social-worker-$1.prom"
}
