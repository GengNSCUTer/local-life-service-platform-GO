"""Exercise real HTTP endpoints on the disposable catalog stack."""
import concurrent.futures
import argparse
from collections import Counter, defaultdict
import http.client
import json
import math
import random
import time

from catalog_seed import CENTERS, SESSIONS, SHOPS, TYPES


def request(connection, path, token=None, body=None, method="GET"):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    if body is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(body)
    connection.request(method, path, body, headers)
    response = connection.getresponse()
    payload = json.loads(response.read())
    if response.status != 200 or not payload.get("success"):
        raise ValueError(f"{path}: HTTP {response.status} {payload.get('errorMsg', '')}")
    return payload.get("data")


def nearby(kind, center, page):
    x, y = CENTERS[center]
    return f"/shop/of/type?typeId={kind}&x={x}&y={y}&current={page}"


def addresses(args):
    return [("127.0.0.1", port) for port in args.ports]


def smoke(args):
    with_connection = http.client.HTTPConnection(*addresses(args)[0], timeout=5)
    try:
        call = lambda *a, **kw: request(with_connection, *a, **kw)
        token = call("/user/login", body={"phone": "19900000002", "code": args.code}, method="POST")
        assert call("/user/me", token)["id"] == 2
        assert call("/user/me", "catalog-10000")["id"] == 10_000
        assert call("/user/100000", token)["id"] == 100_000
        assert call("/shop/10000")["id"] == 10_000
        assert len(call("/shop-type/list")) == TYPES
        assert call("/shop/of/name?name=09999")[0]["id"] == 9999
        for center in range(len(CENTERS)):
            rows = call(nearby(1, center, 1))
            distances = [row["distance"] for row in rows]
            assert len(rows) == 5 and distances == sorted(distances)
            assert all(row["typeId"] == 1 and 0 <= row["distance"] <= 5000 for row in rows)
        assert len(call(nearby(1, 0, 90))) == 5
        first = call("/blog/of/follow", "catalog-1")
        second = call(f"/blog/of/follow?cursor={first['nextCursor']}", "catalog-1")
        ids = [row["id"] for page in (first, second) for row in page["list"]]
        assert len(ids) == len(set(ids)) == 20 and ids == sorted(ids, reverse=True)
        assert all(2 <= row["userId"] <= 11 for page in (first, second) for row in page["list"])
        blog_id = call("/blog", token, {"shopId": 1, "title": "Notification smoke", "content": "Kafka delivery check"}, "POST")
        for _ in range(60):
            notifications = call("/blog/notifications", "catalog-1")
            if any(row["id"] == blog_id and row["userId"] == 2 for row in notifications):
                print(f"PASS login, shops, GEO, feed cursor, Kafka notification (blog {blog_id})")
                return
            time.sleep(.5)
        raise RuntimeError(f"Notification for blog {blog_id} was not delivered")
    finally:
        with_connection.close()


def pagination(args):
    connection = http.client.HTTPConnection(*addresses(args)[0], timeout=5)
    try:
        for path, cursor, next_key in (
            ("/blog/hot", "cursor", "nextCursor"),
            ("/shop/of/type?typeId=1", "afterId", "nextAfterId"),
            ("/shop/of/name?name=Catalog%20Shop", "afterId", "nextAfterId"),
        ):
            join = "&" if "?" in path else "?"
            first = request(connection, f"{path}{join}{cursor}=0")
            second = request(connection, f"{path}{join}{cursor}={first[next_key]}")
            assert first["list"] == request(connection, f"{path}{join}current=1"), path
            assert second["list"] == request(connection, f"{path}{join}current=2"), path
            assert not ({row["id"] for row in first["list"]} & {row["id"] for row in second["list"]}), path
        legacy = request(connection, "/blog/hot?current=1000")
        prior = request(connection, "/blog/hot?current=999")[-1]
        deep = request(connection, f"/blog/hot?cursor={prior['liked']}:{prior['id']}")
        assert legacy == deep["list"] and len(legacy) == 10
        assert request(connection, "/shop/of/type?typeId=1&afterId=9991") == {"list": []}
        assert request(connection, "/shop/of/name?name=09999&afterId=0")["list"][0]["id"] == 9999
        print("PASS legacy and cursor pages match, including deep hot blog page")
    finally:
        connection.close()


def geo_burst(args):
    targets = addresses(args)

    def load(worker):
        connection = http.client.HTTPConnection(*targets[worker % len(targets)], timeout=5)
        try:
            started = time.monotonic()
            rows = request(connection, nearby(1, 0, 1))
            assert len(rows) == 5
            return (time.monotonic() - started) * 1000
        finally:
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        print(json.dumps({"requests": args.clients, **latency(list(pool.map(load, range(args.clients))))}))


def warm(args):
    # Prewarm the entire detail keyspace and every GEO category through the real API.
    targets = addresses(args)

    def prewarm(worker):
        connection = http.client.HTTPConnection(*targets[worker % len(targets)], timeout=5)
        try:
            for i in range(worker + 1, SHOPS + 1, args.clients):
                request(connection, f"/shop/{i}")
            for kind in range(worker + 1, TYPES + 1, args.clients):
                request(connection, nearby(kind, 0, 1))
        finally:
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        list(pool.map(prewarm, range(args.clients)))


def workload(rng, hot):
    shop = rng.randint(1, 20 if hot else SHOPS)
    viewer = rng.randint(1, 20 if hot else SESSIONS)
    kind = 1 if hot else rng.randint(1, TYPES)
    auth = f"catalog-{viewer}"
    return (
        ("detail", f"/shop/{shop}", None),
        ("name_shallow", "/shop/of/name?name=Catalog%20Shop", None),
        ("name_deep", "/shop/of/name?name=Catalog%20Shop&current=990", None),
        ("name_rare", "/shop/of/name?name=09999", None),
        ("type_deep", f"/shop/of/type?typeId={kind}&current=196", None),
        ("geo_shallow", nearby(kind, 0 if hot else rng.randrange(len(CENTERS)), 1), None),
        ("geo_deep", nearby(kind, 0, 90), None),
        ("hot_blog_shallow", "/blog/hot", None),
        ("hot_blog_deep", "/blog/hot?current=1000", None),
        ("feed_first", "/blog/of/follow", auth),
        ("feed_next", "/blog/of/follow?cursor=900000", auth),
        ("user_me", "/user/me", auth),
    )


def latency(values):
    if not values:
        return {}
    values.sort()
    return {f"p{p}Ms": round(values[math.ceil(len(values) * p / 100) - 1], 3) for p in (50, 95, 99)}


def measure(args):
    targets = addresses(args)
    start = time.monotonic()
    deadline = start + args.seconds

    def worker(index):
        rng = random.Random(20261120 + index)
        connections = [http.client.HTTPConnection(*target, timeout=5) for target in targets]
        values, errors, samples = defaultdict(list), Counter(), {}
        index_in_routes = index
        requests = 0
        try:
            while time.monotonic() < deadline:
                routes = workload(rng, args.traffic == "hot" and rng.random() < .9)
                route, path, token = routes[index_in_routes % len(routes)]
                index_in_routes += 1
                connection = connections[(index + requests) % len(connections)]
                requests += 1
                began = time.monotonic()
                try:
                    data = request(connection, path, token)
                    if data is None or data == [] or (route.startswith("feed_") and not data["list"]):
                        raise ValueError(f"{path}: empty payload")
                except (OSError, http.client.HTTPException, ValueError, KeyError) as exc:
                    errors[route] += 1
                    samples.setdefault(route, str(exc))
                    connection.close()
                else:
                    values[route].append((time.monotonic() - began) * 1000)
        finally:
            for connection in connections:
                connection.close()
        return values, errors, samples

    values, errors, samples = defaultdict(list), Counter(), {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        for timings, failures, examples in pool.map(worker, range(args.clients)):
            for route, series in timings.items():
                values[route].extend(series)
            errors.update(failures)
            samples.update(examples)
    elapsed = time.monotonic() - start
    all_values = [value for series in values.values() for value in series]
    by_route = {
        route: {"ok": len(values[route]), "errors": errors[route], **latency(values[route])}
        for route in sorted(values.keys() | errors.keys())
    }
    print(json.dumps({"scenario": args.scenario, "elapsedSeconds": round(elapsed, 3), "clients": args.clients,
                      "ok": len(all_values), "errors": sum(errors.values()), "okPerSecond": round(len(all_values) / elapsed, 2),
                      **latency(all_values), "routes": by_route, "errorExamples": samples}), flush=True)
    if errors or len(by_route) != len(workload(random.Random(0), False)):
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("smoke", "pagination", "geo-burst", "warm", "measure"))
    parser.add_argument("--ports", type=int, nargs="+", required=True)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--traffic", choices=("uniform", "hot"), default="uniform")
    parser.add_argument("--scenario")
    parser.add_argument("--code")
    args = parser.parse_args()
    if args.clients < 1 or not math.isfinite(args.seconds) or args.seconds <= 0:
        parser.error("clients and seconds must be positive")
    {"smoke": smoke, "pagination": pagination, "geo-burst": geo_burst, "warm": warm, "measure": measure}[args.mode](args)
