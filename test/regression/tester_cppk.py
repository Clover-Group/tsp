#!/usr/bin/python3.6

# This script implements a regression testing the the REST API
# It sends several requests in parallel threads to the TSP backend, checks responses and measures the execution timeouts
# Designed by Nikita Novikov
# Clover Group, Feb 21 2019

import json
import time
import glob

import requests
import threading

import sys
import argparse
from pprint import pprint


def send(idx: int, file: str, req: dict, resps: list):
    print(f"Request #{idx} ({file}) sent")
    resp = requests.post(url, json=req, headers={"Content-Type": "application/json"})
    print(f"Request #{idx} received response")

    try:
        p_resp = resp.json()
        is_error = p_resp.get("errorCode") is not None
    except json.JSONDecodeError:
        p_resp = str(resp)
        is_error = True

    resps.append((idx, p_resp, is_error))


if __name__ == "__main__":
    if sys.version_info[0] != 3 or sys.version_info[1] < 6:
        print("This script requires Python version 3.6+")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Run requests to TSP")
    parser.add_argument("--host", default="127.0.0.1", help="IP address of TSP server, defaults to localhost")
    parser.add_argument("--port", default=8080, type=int, help="Port number, defaults to 8080")
    parser.add_argument("--batch-size", default=1, type=int, help="Request batch size, defaults to 1")
    parser.add_argument("--timeout", default=1000.0, type=float, help="Request timeout in seconds, defaults to 180")
    parser.add_argument("files", metavar="FILE", type=str, nargs="+", help="File name or mask")
    args = parser.parse_args()

    host = args.host
    port = args.port
    # TSP server. Localhost or production
    url = f'http://{host}:{port}/streamJob/from-influxdb/to-jdbc/?run_async=1'
    print(f"Sending to {url}")

    files = sum([glob.glob(p) for p in args.files], [])
    reqs = [(f, json.load(open(f, "r"))) for f in files]
    batch_size = args.batch_size
    timeout = args.timeout

    req_count = len(reqs)
    for start_idx in range(0, req_count, batch_size):
        print(f"Sending batch of requests "
              f"#{start_idx}--{min(req_count, start_idx + batch_size) - 1}")
        batch = reqs[start_idx:start_idx + batch_size]
        threads = []
        responses = []
        for idx, (f, r) in enumerate(batch):
            threads.append(threading.Thread(
                target=send, args=(start_idx + idx, f, r, responses),
                daemon=True))

        t1 = time.time()
        for t in threads:
            t.start()

        # thread sync
        for t in threads:
            t.join(timeout)
            if t.is_alive():
                print("Thread timed out")

        t2 = time.time()

        if t2 - t1 > timeout:
            print("WARNING: Time limit exceeded")

        print(f'Time: {t2 - t1:.3f}')
        error_indices = [(r[0], r[1]) for r in responses if r[2]]
        if error_indices:
            print("Errors occurred while processing the following requests:")
            for (i, e) in error_indices:
                print(f"Request #{i}")
                pprint(e)
            sys.exit(1)
        # Sleep for 5 seconds after a batch being processed
        time.sleep(5)
