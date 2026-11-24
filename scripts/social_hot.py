"""Fixed-rate, isolated social workload and compatibility checks."""
import concurrent.futures as cf
import http.client
import json
import math
import random
import sys
import threading
import time


def seed(blogs, comments):
    from catalog_seed import insert

    print("SET SESSION sql_log_bin = 0;")
    print("START TRANSACTION;")
    print("INSERT INTO tb_shop_type (id,name) VALUES (1,'Food');")
    print("INSERT INTO tb_shop (id,name,type_id,images,address,x,y) VALUES (1,'Fixture',1,'','address',0,0);")
    insert("tb_user", "id,phone,nick_name", (f"({i},'199{i:08d}','user {i}')" for i in range(1, 12001)))
    insert("tb_blog", "id,shop_id,user_id,title,images,content",
           (f"({i},1,{i % 2001 + 100},'Post {i}','','fixture')" for i in range(1, blogs + 1)))
    insert("social_blog_comments", "id,blog_id,author_id,parent_id,content",
           (f"({i},1,1,{1 if 1 < i <= min(10001, comments) else 'NULL'},'fixture')" for i in range(1, comments + 1)))
    print(f"UPDATE tb_blog SET comments={comments} WHERE id=1;")
    insert("social_follows", "user_id,follow_user_id", (f"({viewer},{i + 100})" for viewer, count in ((1, 10), (2, 500), (3, 2000)) for i in range(count)))
    print("COMMIT;")


local = threading.local()


def request(port, method, path, user=0, body=None, key=""):
    if not hasattr(local, "conns"):
        local.conns = {}
    conn = local.conns.get(port)
    if conn is None:
        conn = local.conns[port] = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {"Authorization": f"Bearer stage7-{user}"} if user else {}
    if body is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(body)
    if key:
        headers["Idempotency-Key"] = key
    try:
        conn.request(method, path, body, headers)
        res = conn.getresponse()
        raw = res.read()
        return res.status, json.loads(raw).get("data")
    except (OSError, ValueError, http.client.HTTPException):
        conn.close()
        return 0, None


def verify(port, second, api_count):
    def check(method, path, user=0, body=None, key="", status=200, target=port):
        result, data = request(target, method, path, user, body, key)
        assert result == status, (method, path, result, status, data)
        return data

    path = "/blog/1/comments"
    root = check("POST", path, 1, {"content": "root"}, status=201)["id"]
    reply = check("POST", path, 2, {"content": "reply", "parentId": root}, status=201)["id"]
    with cf.ThreadPoolExecutor(8) as executor:
        ids = list(executor.map(lambda i: check("POST", path, 3, {"content": "deep", "parentId": reply}, "same", 201,
                                                   second if i % 2 and api_count == 2 else port)["id"], range(8)))
    assert len(set(ids)) == 1
    check("POST", path, 3, {"content": "another", "parentId": reply}, "same", 409)
    check("POST", "/blog/2/comments", 3, {"content": "cross", "parentId": reply}, status=404)
    assert [(row["id"], row["replyToCommentId"]) for row in check("GET", f"{path}?parentId={root}")["list"]] == [(reply, root), (ids[0], reply)]
    page = check("GET", f"{path}?parentId=1")
    if len(page["list"]) == 20:
        next_page = check("GET", f"{path}?parentId=1&afterId={page['nextAfterId']}")
        assert next_page["list"][0]["id"] > page["list"][-1]["id"]
    check("DELETE", f"{path}/{reply}", 2)
    check("DELETE", f"{path}/{reply}", 2)
    check("POST", path, 3, {"content": "deleted", "parentId": reply}, status=404)
    assert check("GET", f"{path}?parentId={root}")["list"][0]["id"] == ids[0]
    check("DELETE", f"{path}/{root}", 1)
    assert check("GET", f"{path}?parentId={root}")["list"][0]["id"] == ids[0]
    check("PUT", "/blog/like/1?liked=true", 1)
    check("PUT", "/blog/like/1?liked=true", 1)
    assert check("GET", "/blog/1", 1)["isLike"] and not check("GET", "/blog/1", 2)["isLike"]
    check("DELETE", "/blog/like/1", 1)
    assert not check("GET", "/blog/1", 1)["isLike"]
    for viewer in (1, 2, 3):
        page = check("GET", "/blog/of/follow", viewer)
        ids = [row["id"] for row in page["list"]]
        assert ids == sorted(set(ids), reverse=True)
        if page.get("nextCursor"):
            tail = check("GET", "/blog/of/follow?cursor=" + page["nextCursor"], viewer)
            assert tail["list"][0]["id"] < ids[-1]
    assert check("GET", "/blog/of/follow", 4)["list"] == []
    for _ in range(30):
        status, page = request(port, "GET", "/blog/hot?snapshot=true")
        if status == 200 and page.get("nextCursor"):
            break
        time.sleep(.5)
    else:
        raise RuntimeError("rank snapshot never became visible")
    other = check("GET", "/blog/hot?snapshot=true&cursor=" + page["nextCursor"])
    assert not {row["id"] for row in page["list"]} & {row["id"] for row in other["list"]}
    check("GET", "/blog/hot?snapshot=true&cursor=1:10", status=410)
    print("deep replies, idempotency, deletion, personal likes, feed cursors and rank snapshots passed")


def load(rate, duration, mode, first_user, ports):
    rng = random.Random(43)
    started = time.monotonic()
    results = {}
    pending = set()
    attempted = 0
    dropped = 0

    def perform(port, action, method, path, user, body, sent):
        status, data = request(port, method, path, user, body)
        if action == "comment" and status == 201:
            status, _ = request(port, "DELETE", f"{path}/{data['id']}", user)
            if status == 200:
                status = 201
        return action, status, (time.monotonic() - sent) * 1000

    def collect(futures):
        for f in futures:
            action, code, elapsed = f.result()
            results.setdefault(action, {}).setdefault(code, []).append(elapsed)

    with cf.ThreadPoolExecutor(64) as executor:
        for i in range(rate * duration):
            scheduled = started + i / rate
            time.sleep(max(0, scheduled - time.monotonic()))
            done = {f for f in pending if f.done()}
            collect(done)
            pending -= done
            if len(pending) >= 64 or time.monotonic() > scheduled + .25:
                dropped += 1
                continue
            blog = 1 if rng.random() < .9 else rng.randint(2, 33)
            user = first_user + rng.randrange(4096)
            write = mode == "write" or (mode == "mixed" and rng.random() < .1)
            if write and rng.random() < .5:
                args = ("like", "PUT" if rng.random() < .5 else "DELETE", f"/blog/like/{blog}?liked=true", user, None)
            elif write:
                args = ("comment", "POST", f"/blog/{blog}/comments", user, {"content": "load"})
            elif mode == "feed":
                viewer = 1 + i % 3
                before = "?cursor=900000" if (i // 3) % 2 else ""
                args = (f"feed_{viewer}", "GET", "/blog/of/follow" + before, viewer, None)
            elif rng.random() < .25:
                args = ("comment_read", "GET", f"/blog/{blog}/comments", 0, None)
            else:
                args = ("detail", "GET", f"/blog/{blog}", 0 if i % 2 else user, None)
            pending.add(executor.submit(perform, ports[i % len(ports)], *args, scheduled))
            attempted += 1
            if i and i % (rate * 60) == 0:
                print(f"elapsed={i // rate}s dispatched={attempted} dropped={dropped}", file=sys.stderr, flush=True)
        collect(pending)
    report = {}
    for action, groups in results.items():
        ok = sorted(v for code, times in groups.items() if code in (200, 201) for v in times)
        report[action] = {"statuses": {k: len(v) for k, v in groups.items()}, "success": len(ok),
                          "p99_ms": round(ok[math.ceil(.99 * len(ok)) - 1], 3) if ok else None}
    print(json.dumps({"mode": mode, "seconds": duration, "offered_rps": rate, "attempted": attempted, "dropped": dropped,
                      "requestGroups": report}))


if __name__ == "__main__":
    modes = {"seed": seed, "verify": verify}
    if sys.argv[1] in modes:
        modes[sys.argv[1]](*map(int, sys.argv[2:]))
    else:
        load(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], int(sys.argv[5]), list(map(int, sys.argv[6:])))
