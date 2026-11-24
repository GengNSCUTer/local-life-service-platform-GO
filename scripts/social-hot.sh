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
    python3 scripts/social_hot.py verify "$port" "$((port+8))" "$api_count" "$workload" "$(( ${SOCIAL_HOT_BLOGS:-1000000} + 1 ))" | tee "$work/social-smoke.log"
    if [[ "$workload" == matrix || "$workload" == sweep ]]; then
        social_hot_matrix
        return
    fi
    if [[ "$workload" == feed ]]; then
        social_hot_feed_plans
    fi
    if [[ "${SOCIAL_HOT_FAULTS:-0}" == 1 ]]; then
        social_hot_failures
        social_hot_legacy_base
        social_hot_cache_failure
    fi
    if [[ "$workload" == smoke ]]; then return; fi
    if ((order_rate > 0)); then
        local starts
        starts=$(date -u -d '+5 seconds' +%FT%TZ)
        "$work/api" prepare-seckill -voucher-id "$voucher_id" -stock "$stock" -starts-at "$starts" \
            -ends-at "$(date -u -d "+$((duration+600)) seconds" +%FT%TZ)"
        while (( $(date +%s) < $(date -d "$starts" +%s) )); do sleep .2; done
        python3 scripts/seckill-soak.py --port "$port" --voucher "$voucher_id" --seconds "$duration" --rate "$order_rate" --prefix stage7- --measure-confirmation > "$work/seckill-soak.jsonl" & benchmark_pid=$!
    fi
    social_hot_snapshot before
    python3 scripts/social_hot.py load "$rate" "$duration" "$workload" "$((stock+1))" "${targets[@]}" | tee "$work/social-hot.json"
    if ((order_rate > 0)); then
        python3 - "$work/social-hot.json" <<'PY'
import json, sys
with open(sys.argv[1]) as report:
    data = json.load(report)
assert data['dropped'] == 0, data
for result in data['requestGroups'].values():
    assert result['success'] == sum(result['statuses'].values()), data
PY
        wait "$benchmark_pid"
        benchmark_pid=
        wait_confirmed "$stock"
        wait_empty
        assert_inventory "$stock"
        tail -1 "$work/seckill-soak.jsonl"
    fi
    social_hot_assert_counters
    printf 'counter mismatches: 0\n' | tee "$work/social-counter-audit.txt"
    social_hot_snapshot after
}

social_hot_legacy_base() {
    "${mysql[@]}" "$MYSQL_DATABASE" -e 'UPDATE tb_blog SET liked=47 WHERE id=34;'
    curl -fsS -X PUT -H 'Authorization: Bearer stage7-2' "http://$HTTP_ADDR/blog/like/34?liked=true" >/dev/null
    local expected=48 verb
    for verb in PUT DELETE; do
        for attempt in {1..30}; do
            if [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT liked FROM tb_blog WHERE id=34') == "$expected" ]]; then break; fi
            sleep .2
        done
        [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT liked FROM tb_blog WHERE id=34') == "$expected" ]]
        if [[ "$verb" == PUT ]]; then
            curl -fsS -X DELETE -H 'Authorization: Bearer stage7-2' "http://$HTTP_ADDR/blog/like/34" >/dev/null
            expected=47
        fi
    done
    printf 'Legacy display count survived new like and unlike\n' | tee -a "$work/social-faults.log"
}

social_hot_feed_plans() {
    for viewer in 1 2 3; do
        "${mysql[@]}" "$MYSQL_DATABASE" -e "EXPLAIN ANALYZE SELECT b.id FROM tb_blog AS b
            JOIN social_follows f ON f.follow_user_id=b.user_id AND f.user_id=$viewer
            WHERE b.id < 900000 ORDER BY b.id DESC LIMIT 11;"
        "${mysql[@]}" "$MYSQL_DATABASE" -e "EXPLAIN ANALYZE SELECT recent.id FROM social_follows AS f
            JOIN LATERAL (SELECT candidate.id FROM tb_blog AS candidate FORCE INDEX (idx_blog_author)
            WHERE candidate.user_id=f.follow_user_id AND candidate.id < 900000 ORDER BY candidate.id DESC LIMIT 11) AS recent ON TRUE
            WHERE f.user_id=$viewer ORDER BY recent.id DESC LIMIT 11;"
    done > "$work/social-feed-plans.txt"
}

social_hot_matrix() {
    local round variant count kind offered buckets cache
    local second_addr="127.0.0.1:$((port+8))"
    local -a targets
    local -a variants=(legacy buckets cache combined)
    local -a steps=(read:200 read:800 write:200 write:800 mixed:400)
    local -a rounds=(1 2 3)
    if [[ ${SOCIAL_HOT_WORKLOAD:-} == sweep ]]; then
        variants=(combined)
        steps=(write:800 write:1600 write:2400 write:3200 write:4800)
        rounds=(1 2)
    fi
    for round in "${rounds[@]}"; do
        count=2
        if [[ "$round" == 1 ]]; then count=1; fi
        for variant in "${variants[@]}"; do
            buckets=false cache=false
            case "$variant" in buckets) buckets=true ;; cache) cache=true ;; combined) buckets=true; cache=true ;; esac
            stop "$second_pid"; second_pid=
            stop "$api_pid"; api_pid=
            export SOCIAL_COUNTER_BUCKETS=$buckets SOCIAL_DETAIL_CACHE=$cache
            start_api
            targets=("$port")
            if [[ "$count" == 2 ]]; then
                HTTP_ADDR="$second_addr" OBSERVABILITY_ADDR="127.0.0.1:$((port+9))" "$work/api" >"$work/api-second.log" 2>&1 & second_pid=$!
                wait_ready "$second_addr"
                targets+=("$((port+8))")
            fi
            local spec
            for spec in "${steps[@]}"; do
                kind=${spec%:*}; offered=${spec#*:}
                local label="r${round}-${variant}-${kind}-${offered}"
                social_hot_snapshot "$label-before"
                python3 scripts/social_hot.py load "$offered" "${SOCIAL_HOT_SECONDS:-15}" "$kind" 1 "${targets[@]}" | tee "$work/$label.json"
                social_hot_assert_counters
                social_hot_snapshot "$label-after"
                printf 'Completed %s with %s API\n' "$label" "$count"
            done
        done
    done
}

social_hot_assert_counters() {
    for attempt in {1..60}; do
        if [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM social_blog_counter_buckets WHERE dirty_since IS NOT NULL') == 0 ]]; then break; fi
        sleep .25
    done
    local failures
    failures=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM tb_blog AS b WHERE b.id <= 33 AND
        (b.liked != (SELECT COUNT(*) FROM social_blog_likes l WHERE l.blog_id = b.id) OR
         b.comments != (SELECT COUNT(*) FROM social_blog_comments c WHERE c.blog_id = b.id AND c.deleted_at IS NULL))')
    [[ "$failures" == 0 ]]
}

social_hot_failures() {
    local blog=0 attempt
    local token='stage7-1'
    "${mysql[@]}" "$MYSQL_DATABASE" -e 'INSERT IGNORE INTO social_follows (user_id, follow_user_id) SELECT id, 1 FROM tb_user WHERE id BETWEEN 2 AND 4096'
    stop "$worker_pid"; worker_pid=
    stop "$kafka_pid"; kafka_pid=
    blog=$(curl -fsS -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
        --data '{"shopId":1,"title":"Outage test","content":"fixture"}' "http://$HTTP_ADDR/blog" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_blog_outbox WHERE blog_id=$blog") == 1 ]]
    start_kafka "$work/kafka.properties"
    "${redis[@]}" CLIENT PAUSE 9000 WRITE >/dev/null
    start_worker
    for attempt in {1..100}; do
        if [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_fanout_tasks WHERE blog_id=$blog AND lease_token IS NOT NULL") == 1 ]]; then break; fi
        sleep .05
    done
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_fanout_tasks WHERE blog_id=$blog AND lease_token IS NOT NULL") == 1 ]]
    kill -KILL "$worker_pid"; wait "$worker_pid" 2>/dev/null || true; worker_pid=
    start_worker
    for attempt in {1..150}; do
        if [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_fanout_tasks WHERE blog_id=$blog AND due_at IS NULL") == 1 ]]; then break; fi
        sleep .2
    done
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_fanout_tasks WHERE blog_id=$blog AND due_at IS NULL") == 1 ]]
    [[ $("${mysql[@]}" -N "$MYSQL_DATABASE" -e "SELECT COUNT(*) FROM social_blog_outbox WHERE blog_id=$blog") == 0 ]]
    for id in 2 2048 4096; do
        [[ -n $("${redis[@]}" ZSCORE "social:notifications:$id" "$blog") ]]
    done
    printf 'Worker and Kafka stopped during publish, worker killed after claiming a fanout task, recovered with no missing samples\n' | tee "$work/social-faults.log"
}

social_hot_cache_failure() {
    "${redis[@]}" DEL 'social:blog:{1}:detail' >/dev/null
    "${redis[@]}" CLIENT PAUSE 1500 ALL >/dev/null
    curl -fsS --max-time 3 "http://$HTTP_ADDR/blog/1" | python3 -c 'import json,sys; assert json.load(sys.stdin)["data"]["id"] == 1'
    printf 'Anonymous detail served from MySQL with Redis unavailable\n' | tee -a "$work/social-faults.log"
}

social_hot_snapshot() {
    "${mysql[@]}" -N -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_row_lock_time','Innodb_row_lock_waits','Innodb_rows_read','Com_select')" > "$work/social-mysql-$1.tsv"
    curl -fsS "http://127.0.0.1:$((port+6))/metrics" > "$work/social-api-$1.prom"
    curl -fsS "http://127.0.0.1:$((port+7))/metrics" > "$work/social-worker-$1.prom"
}
