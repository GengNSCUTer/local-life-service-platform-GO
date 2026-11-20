#!/usr/bin/env python3
"""Submit a fixed-rate stream of distinct buyers to an isolated loopback API."""

import argparse
import concurrent.futures
import http.client
import json
import math
import time


def submit(port, voucher, prefix, sequence):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    start = time.monotonic()
    error = None
    try:
        connection.request('POST', f'/voucher-order/seckill/{voucher}',
                           headers={'Authorization': f'Bearer {prefix}{sequence}'})
        response = connection.getresponse()
        body = json.loads(response.read())
        if response.status != 202 or not (body.get('data') or {}).get('orderId'):
            error = f'HTTP {response.status}: {body}'
    except (OSError, ValueError, http.client.HTTPException) as exc:
        error = str(exc)
    finally:
        connection.close()
    return (time.monotonic() - start) * 1000, error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--voucher', type=int, required=True)
    parser.add_argument('--seconds', type=int, default=3660)
    parser.add_argument('--rate', type=int, default=10)
    parser.add_argument('--prefix', default='soak-')
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or min(args.voucher, args.seconds, args.rate) < 1:
        parser.error('provide positive values and a valid port')

    latencies = []
    errors = 0
    first_error = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.rate) as pool:
        started = time.monotonic()
        for second in range(args.seconds):
            time.sleep(max(0, started + second - time.monotonic()))
            first = second * args.rate + 1
            results = pool.map(
                lambda sequence: submit(args.port, args.voucher, args.prefix, sequence),
                range(first, first + args.rate),
            )
            for elapsed, error in results:
                latencies.append(elapsed)
                if error is not None:
                    errors += 1
                    if first_error is None:
                        first_error = error
            if (second + 1) % 60 == 0:
                print(json.dumps({'seconds': second + 1, 'submitted': len(latencies), 'errors': errors}), flush=True)
        time.sleep(max(0, started + args.seconds - time.monotonic()))
    latencies.sort()
    percentile = latencies[math.ceil(len(latencies) * .99) - 1]
    print(json.dumps({'seconds': round(time.monotonic() - started, 3),
                      'requests': len(latencies), 'accepted': len(latencies) - errors,
                      'errors': errors, 'firstError': first_error, 'p99Ms': round(percentile, 2)}), flush=True)
    return int(errors > 0)


if __name__ == '__main__':
    raise SystemExit(main())
