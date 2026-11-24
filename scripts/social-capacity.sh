#!/usr/bin/env bash
# Sourced by stage7-local.sh after the isolated API and Worker are ready.
run_social_capacity() {
    local followers=${SOCIAL_TEST_FOLLOWERS:-100000} results="$work/social-results.json"
    [[ "$followers" =~ ^[1-9][0-9]{0,6}$ && "$followers" -le 1000000 ]]
    python3 scripts/social_capacity.py seed "$followers" | "${mysql[@]}" "$MYSQL_DATABASE" > "$work/social-seed.log"
    local rows
    rows=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM social_follows WHERE follow_user_id = 2')
    [[ "$rows" == "$followers" ]]
    for id in 1 2 7 101 "$((followers+100))"; do
        "${redis[@]}" HSET "login:token:social-$id" id "$id" >/dev/null
    done
    local before after
    before=$(social_zadds)
    python3 scripts/social_capacity.py measure "$followers" "$port" | tee "$results" & benchmark_pid=$!
    while kill -0 "$benchmark_pid" 2>/dev/null; do
        "$KAFKA_BIN_DIR/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
            --describe --group hmdp-social-notifications-v1 >> "$work/social-lag.log" 2>&1 || true
        for attempt in {1..20}; do
            if ! kill -0 "$benchmark_pid" 2>/dev/null; then break; fi
            sleep .25
        done
    done
    wait "$benchmark_pid"
    benchmark_pid=
    after=$(social_zadds)
    printf 'Redis ZADD delta: %s\n' "$((after-before))" | tee "$work/social-redis.txt"
    "$KAFKA_BIN_DIR/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
        --describe --group hmdp-social-notifications-v1 | tee "$work/social-lag-final.log"
    awk '$2 == "hmdp.social.blog-published.v1" && $5 ~ /^[0-9]+$/ && $5 > 0 {
        seen++; if ($6 != "0") failed=1
    } END {exit !(seen && !failed)}' "$work/social-lag-final.log"
    local outbox
    outbox=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM social_blog_outbox')
    [[ "$outbox" == 0 ]]
    local pending
    for attempt in {1..60}; do
        pending=$("${mysql[@]}" -N "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM social_fanout_tasks WHERE due_at IS NOT NULL')
        if [[ "$pending" == 0 ]]; then break; fi
        sleep .25
    done
    printf 'Pending durable fanout tasks: %s\n' "$pending" | tee "$work/social-task-pending.txt"
    [[ "$pending" == 0 ]]
}

social_zadds() {
    "${redis[@]}" INFO commandstats | awk -F'[,=]' '/^cmdstat_zadd:/ {calls=$2} END {print calls+0}'
}
