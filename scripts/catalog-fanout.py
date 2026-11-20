"""Measure delivery through HTTP, Kafka, and Redis to a high-fanout author."""
import http.client
import json
import sys
import time

from catalog_load import request


with_connection = http.client.HTTPConnection("127.0.0.1", int(sys.argv[1]), timeout=5)
try:
    posted = {}
    for number in range(3):
        started = time.monotonic()
        blog = request(with_connection, "/blog", "catalog-2",
                       {"shopId": 1, "title": f"Fanout {number}", "content": "High fanout test"}, "POST")
        posted[blog] = started
    seen = {viewer: {} for viewer in (1, 100000)}
    while time.monotonic() - min(posted.values()) < 180:
        for viewer, arrivals in seen.items():
            rows = request(with_connection, "/blog/notifications", f"catalog-{viewer}")
            for row in rows:
                blog = row["id"]
                if blog in posted:
                    arrivals.setdefault(blog, round(time.monotonic() - posted[blog], 3))
        if all(len(arrivals) == len(posted) for arrivals in seen.values()):
            break
        time.sleep(.1)
    else:
        raise RuntimeError(f"Missing notifications in {seen}")
    print(json.dumps({"followers": 99999, "posts": list(posted), "secondsToRead": seen}), flush=True)
finally:
    with_connection.close()
