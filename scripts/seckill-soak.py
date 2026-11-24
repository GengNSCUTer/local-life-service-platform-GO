#!/usr/bin/env python3
"""Submit a fixed-rate stream of distinct buyers to an isolated loopback API."""

import argparse
import concurrent.futures
import http.client
import json
import math
import time


def submit(port, voucher, prefix, sequence, check_confirmation):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    start = time.monotonic()
    error = None
    accepted = None
    confirmed = None
    try:
        connection.request('POST', f'/voucher-order/seckill/{voucher}',
                           headers={'Authorization': f'Bearer {prefix}{sequence}'})
        response = connection.getresponse()
        body = json.loads(response.read())
        if response.status != 202 or not (body.get('data') or {}).get('orderId'):
            error = f'HTTP {response.status}: {body}'
        else:
            accepted = (time.monotonic() - start) * 1000
            if check_confirmation:
                order = body['data']['orderId']
                while time.monotonic() - start < 4:
                    connection.request('GET', f'/voucher-order/{order}',
                                       headers={'Authorization': f'Bearer {prefix}{sequence}'})
                    result = connection.getresponse()
                    data = json.loads(result.read()).get('data') or {}
                    if result.status == 200 and data.get('status') == 'confirmed':
                        confirmed = (time.monotonic() - start) * 1000
                        break
                    time.sleep(.05)
    except (OSError, ValueError, http.client.HTTPException) as exc:
        if accepted is None:
            error = str(exc)
    finally:
        connection.close()
    return accepted if accepted is not None else (time.monotonic() - start) * 1000, error, confirmed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, action='append', help='loopback API port, repeat to distribute requests')
    parser.add_argument('--voucher', type=int, required=True)
    parser.add_argument('--seconds', type=int, default=3660)
    parser.add_argument('--rate', type=int, default=10)
    parser.add_argument('--prefix', default='soak-')
    parser.add_argument('--measure-confirmation', action='store_true', help='sample confirmation visibility by polling one order per second')
    args = parser.parse_args()
    ports = args.port or [8080]
    if any(not 1 <= port <= 65535 for port in ports) or min(args.voucher, args.seconds, args.rate) < 1:
        parser.error('provide positive values and a valid port')

    latencies = []
    confirmation = []
    errors = 0
    first_error = None
    accepted_by_port = dict.fromkeys(ports, 0)

    def send(sequence):
        port = ports[(sequence - 1) % len(ports)]
        check = args.measure_confirmation and (sequence - 1) % args.rate == 0
        return port, submit(port, args.voucher, args.prefix, sequence, check)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.rate) as pool:
        started = time.monotonic()
        for second in range(args.seconds):
            time.sleep(max(0, started + second - time.monotonic()))
            first = second * args.rate + 1
            for port, (elapsed, error, confirmed) in pool.map(send, range(first, first + args.rate)):
                latencies.append(elapsed)
                if confirmed is not None:
                    confirmation.append(confirmed)
                if error is None:
                    accepted_by_port[port] += 1
                else:
                    errors += 1
                    if first_error is None:
                        first_error = error
            if (second + 1) % 60 == 0:
                print(json.dumps({'seconds': second + 1, 'submitted': len(latencies), 'errors': errors}), flush=True)
        time.sleep(max(0, started + args.seconds - time.monotonic()))
    latencies.sort()
    percentile = latencies[math.ceil(len(latencies) * .99) - 1]
    confirmation.sort()
    print(json.dumps({'seconds': round(time.monotonic() - started, 3),
                      'requests': len(latencies), 'accepted': len(latencies) - errors,
                      'errors': errors, 'firstError': first_error, 'p99Ms': round(percentile, 2),
                      'confirmationSamples': len(confirmation),
                      'confirmationMissing': args.seconds - len(confirmation) if args.measure_confirmation else None,
                      'confirmationP99Ms': round(confirmation[math.ceil(len(confirmation)*.99)-1], 2) if confirmation else None,
                      'acceptedByPort': accepted_by_port}), flush=True)
    return int(errors > 0)


if __name__ == '__main__':
    raise SystemExit(main())
