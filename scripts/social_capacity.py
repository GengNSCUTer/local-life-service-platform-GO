"""Measure notification visibility and fixed-rate read latency with a cold fixture."""
import argparse
import http.client
import json
import math
import time
from itertools import chain


def seed(followers):
    from catalog_seed import insert

    print("SET SESSION sql_log_bin = 0;")
    print("START TRANSACTION;")
    print("INSERT INTO tb_shop_type (id, name) VALUES (1, 'Food');")
    print("INSERT INTO tb_shop (id, name, type_id, images, address, x, y) "
          "VALUES (1, 'Demo shop', 1, '', '', 0, 0);")
    insert("tb_user", "id, phone, nick_name",
           (f"({i}, '199{i:08d}', 'Follower {i}')" for i in chain((1, 2, 7), range(101, 101 + followers))))
    insert("social_follows", "user_id, follow_user_id",
           (f"({i}, 2)" for i in range(101, 101 + followers)))
    print("INSERT INTO social_follows (user_id, follow_user_id) VALUES (1, 7), (101, 7);")
    print("COMMIT;")
    print("ANALYZE TABLE social_follows, tb_blog;")


def measure(port, followers, seconds):
    host = "127.0.0.1"
    conn = http.client.HTTPConnection(host, port, timeout=8)

    def request(method, path, user, body=None):
        headers = {"Authorization": f"Bearer social-{user}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(body)
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        if response.status != 200:
            raise RuntimeError((response.status, path, data))
        return data.get("data")

    def publish(author, index):
        start = time.monotonic()
        blog_id = request("POST", "/blog", author, {"shopId": 1, "title": f"Fanout {index}", "content": "Exercise"})
        return blog_id, start

    try:
        popular = dict(publish(2, i) for i in range(3))
        ordinary, ordinary_at = publish(7, 4)
        viewers = {101: {}, followers + 100: {}, 1: {}}
        started = time.monotonic()
        latencies = []
        visits = 0
        while time.monotonic() - started < seconds:
            tick = time.monotonic()
            viewer = list(viewers)[visits % len(viewers)]
            rows = request("GET", "/blog/notifications", viewer)
            elapsed = time.monotonic()
            ids = [row["id"] for row in rows]
            if ids != sorted(set(ids), reverse=True):
                raise RuntimeError(f"unexpected order or duplicate: {ids}")
            for blog_id in ids:
                if blog_id in popular or blog_id == ordinary:
                    recorded = popular.get(blog_id, ordinary_at)
                    viewers[viewer].setdefault(blog_id, round(elapsed - recorded, 3))
            latencies.append((elapsed - tick) * 1000)
            visits += 1
            if elapsed - started >= 10 and all(popular.keys() <= viewers[v].keys() for v in (101, followers + 100)) and ordinary in viewers[1]:
                break
            time.sleep(max(0, started + visits * .05 - elapsed))
        else:
            raise RuntimeError(f"delivery timed out: {viewers}")
        ordered = sorted(latencies[:200])
        print(json.dumps({"followers": followers, "notifications": viewers, "popularPosts": list(popular),
                          "ordinaryPost": ordinary, "readRequests": visits,
                          "first200ReadsP99ms": round(ordered[math.ceil(len(ordered) * .99) - 1], 3)}), flush=True)
        request("PUT", "/follow/2/false", 101)
        if any(row["id"] in popular for row in request("GET", "/blog/notifications", 101)):
            raise RuntimeError("unfollowed author's post is visible")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "measure"))
    parser.add_argument("followers", type=int)
    parser.add_argument("port", type=int, nargs="?")
    parser.add_argument("--seconds", type=int, default=1200)
    args = parser.parse_args()
    if args.mode == "seed":
        seed(args.followers)
    else:
        measure(args.port, args.followers, args.seconds)
