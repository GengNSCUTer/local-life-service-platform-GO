# Shared setup for disposable Docker Compose smoke tests.
project=${COMPOSE_PROJECT_NAME:-$project-$(date +%s%N)-$$}
work_dir=$(mktemp -d)
api_pid=
compose_started=0
compose_files=(-f compose.yaml)
if [[ -n ${SMOKE_COMPOSE_OVERRIDE:-} ]]; then
    compose_files+=(-f "$SMOKE_COMPOSE_OVERRIDE")
fi

compose() { docker compose "${compose_files[@]}" -p "$project" "$@"; }

smoke_cleanup() {
    local status=$?
    trap '' INT TERM
    if [[ -n "$api_pid" ]]; then
        kill -TERM "$api_pid" 2>/dev/null || true
        wait "$api_pid" 2>/dev/null || true
    fi
    if (( compose_started )); then
        if (( status != 0 )); then
            # MySQL's startup log can contain its generated root password.
            compose logs --tail 20 redis redis-replica redis-sentinel-1 kafka >&2 || true
        fi
        compose unpause kafka >/dev/null 2>&1 || true
        if ! compose down --volumes --remove-orphans; then
            printf 'Compose cleanup failed for %s. Logs are in %s\n' "$project" "$work_dir" >&2
            exit 1
        fi
    fi
    if (( status != 0 )) && [[ -f "$work_dir/api.log" ]]; then
        tail -n 40 "$work_dir/api.log" >&2
    fi
    rm -rf -- "$work_dir"
    return "$status"
}
trap smoke_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

smoke_start() {
    if ! docker info >/dev/null 2>&1; then
        echo 'Docker daemon is unavailable. Start Docker and rerun this script.' >&2
        return 1
    fi
    local filter="label=com.docker.compose.project=$project" resources volumes
    resources=$(docker ps -aq --filter "$filter")
    volumes=$(docker volume ls -q --filter "$filter")
    if [[ -n "$resources" || -n "$volumes" ]]; then
        printf 'Compose project %s already has containers or volumes. Use a fresh project name.\n' "$project" >&2
        return 1
    fi
    go build -o "$work_dir/api" ./cmd/api
    go build -o "$work_dir/all" ./cmd/all
    compose_started=1
    compose up -d --wait mysql redis redis-replica redis-sentinel-1 redis-sentinel-2 redis-sentinel-3 kafka
    local sql
    for sql in migrations/*.sql; do
        compose exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u "$MYSQL_USER" "$MYSQL_DATABASE"' < "$sql"
    done
}
