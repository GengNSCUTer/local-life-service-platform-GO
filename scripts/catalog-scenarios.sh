# Sourced only by stage7-local.sh after the isolated stack has started.
run_catalog() {
    export PYTHONDONTWRITEBYTECODE=1
    local clients=${CATALOG_CLIENTS:-16} seconds=${CATALOG_SECONDS:-60}
    local dataset code api_count traffic cache name result=0
    local -a ports
    python3 scripts/catalog_seed.py sql | "${mysql[@]}" "$MYSQL_DATABASE" > "$work/catalog-seed.log"
    python3 scripts/catalog_seed.py sessions | "${redis[@]}" --pipe > "$work/catalog-sessions.log"
    grep -q 'errors: 0, replies: 20000' "$work/catalog-sessions.log"
    [[ $("${redis[@]}" TTL login:token:catalog-10000) -gt 0 ]]
    dataset=$("${mysql[@]}" -BN "$MYSQL_DATABASE" <<'SQL'
SELECT (SELECT COUNT(*) FROM tb_user), (SELECT COUNT(*) FROM tb_shop),
       (SELECT COUNT(*) FROM tb_shop_type), (SELECT COUNT(*) FROM tb_blog),
       (SELECT COUNT(*) FROM social_follows);
SELECT (SELECT COUNT(*) FROM tb_shop s LEFT JOIN tb_shop_type t ON t.id=s.type_id WHERE t.id IS NULL),
       (SELECT COUNT(*) FROM tb_blog b LEFT JOIN tb_shop s ON s.id=b.shop_id LEFT JOIN tb_user u ON u.id=b.user_id WHERE s.id IS NULL OR u.id IS NULL),
       (SELECT COUNT(*) FROM social_follows f LEFT JOIN tb_user u ON u.id=f.user_id LEFT JOIN tb_user a ON a.id=f.follow_user_id WHERE u.id IS NULL OR a.id IS NULL OR f.user_id=f.follow_user_id);
SQL
    )
    printf 'Counts (users, shops, types, blogs, follows), then invalid links:\n%s\n' "$dataset" | tee "$work/catalog-counts.txt"
    [[ "$dataset" == $'100000\t10000\t10\t1000000\t1000000\n0\t0\t0' ]]
    curl -fsS --max-time 5 -X POST "http://$HTTP_ADDR/user/code?phone=19900000002" > /dev/null
    code=$("${redis[@]}" HGET 'login:code:{19900000002}' code)
    python3 scripts/catalog-load.py smoke --ports "$port" --code "$code" | tee "$work/catalog-smoke.log"
    [[ $("${mysql[@]}" -BN "$MYSQL_DATABASE" -e 'SELECT COUNT(*) FROM tb_user') == 100000 ]]
    "${mysql[@]}" --batch --raw --skip-column-names "$MYSQL_DATABASE" < scripts/catalog-plans.sql > "$work/catalog-explain.txt"

    for api_count in 1 2; do
        ports=("$port")
        if [[ "$api_count" == 2 ]]; then
            ports+=("$((port+8))")
            HTTP_ADDR="127.0.0.1:$((port+8))" KAFKA_BROKERS= OBSERVABILITY_ADDR="127.0.0.1:$((port+9))" \
                "$work/api" > "$work/api-second.log" 2>&1 & second_pid=$!
            wait_ready "127.0.0.1:$((port+8))"
        fi
        for traffic in uniform hot; do
            for cache in cold warm; do
                name="api${api_count}-${traffic}-${cache}"
                # Do not touch sessions, notification keys, or Redis data from any other instance.
                local keys="$work/catalog-cache-keys.txt"
                "${redis[@]}" --scan --pattern 'cache:shop:*' > "$keys"
                "${redis[@]}" --scan --pattern 'shop:geo:*' >> "$keys"
                xargs -r -n 500 "${redis[@]}" UNLINK < "$keys" > /dev/null
                if [[ "$cache" == warm ]]; then
                    python3 scripts/catalog-load.py warm --ports "${ports[@]}" --clients "$clients"
                fi
                catalog_snapshot "$name-before" "$api_count"
                printf 'Running %s with %s clients for %ss\n' "$name" "$clients" "$seconds"
                if ! python3 scripts/catalog-load.py measure --ports "${ports[@]}" --clients "$clients" \
                    --seconds "$seconds" --traffic "$traffic" --scenario "$name" | tee -a "$work/catalog-results.jsonl"; then
                    result=1
                fi
                catalog_snapshot "$name-after" "$api_count"
            done
        done
    done
    rm -f -- "$keys"
    return "$result"
}

catalog_snapshot() {
    local name=$1 api_count=$2
    "${redis[@]}" INFO stats > "$work/$name.redis"
    "${redis[@]}" INFO commandstats >> "$work/$name.redis"
    "${redis[@]}" INFO memory >> "$work/$name.redis"
    "${redis[@]}" INFO keyspace >> "$work/$name.redis"
    "${mysql[@]}" -BN -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Questions','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_reads','Innodb_rows_read','Select_scan');" > "$work/$name.mysql"
    curl -fsS --max-time 5 "http://127.0.0.1:$((port+6))/metrics" > "$work/$name.api.prom"
    if [[ "$api_count" == 2 ]]; then
        curl -fsS --max-time 5 "http://127.0.0.1:$((port+9))/metrics" > "$work/$name.second.prom"
    fi
}
