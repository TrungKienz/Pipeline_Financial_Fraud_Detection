#!/usr/bin/env python3
from __future__ import annotations

import argparse
import http.client
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Callable


DEFAULT_TIMEOUT = 5.0
LOCAL_DEFAULT_HOST = "localhost"
REMOTE_DEFAULT_HOST = "163.223.13.187"


@dataclass(frozen=True)
class ServiceCheck:
    name: str
    endpoint: str
    checker: Callable[[str, float], "CheckResult"]


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    endpoint: str
    detail: str
    latency_ms: int


def tcp_probe(name: str, host: str, port: int, timeout: float, note: str = "") -> CheckResult:
    endpoint = f"{host}:{port}"
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency_ms = int((time.perf_counter() - started) * 1000)
            detail = "TCP connection established"
            if note:
                detail = f"{detail}. {note}"
            return CheckResult(name=name, ok=True, endpoint=endpoint, detail=detail, latency_ms=latency_ms)
    except OSError as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return CheckResult(
            name=name,
            ok=False,
            endpoint=endpoint,
            detail=f"TCP connection failed: {exc}",
            latency_ms=latency_ms,
        )


def http_probe(name: str, url: str, timeout: float) -> CheckResult:
    attempts = 3
    last_error = "HTTP request failed"
    started = time.perf_counter()

    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "bigdata-compose-healthcheck/1.0",
                "Accept": "application/json,text/html,text/plain,*/*",
                "Connection": "close",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                latency_ms = int((time.perf_counter() - started) * 1000)
                return CheckResult(
                    name=name,
                    ok=200 <= response.status < 400,
                    endpoint=url,
                    detail=f"HTTP {response.status}",
                    latency_ms=latency_ms,
                )
        except urllib.error.HTTPError as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            return CheckResult(
                name=name,
                ok=False,
                endpoint=url,
                detail=f"HTTP error {exc.code}",
                latency_ms=latency_ms,
            )
        except (OSError, http.client.HTTPException) as exc:
            last_error = str(exc)
            if attempt < attempts:
                time.sleep(0.5)

    latency_ms = int((time.perf_counter() - started) * 1000)
    return CheckResult(
        name=name,
        ok=False,
        endpoint=url,
        detail=f"HTTP request failed: {last_error}",
        latency_ms=latency_ms,
    )


def redis_ping(host: str, port: int, timeout: float) -> CheckResult:
    endpoint = f"{host}:{port}"
    started = time.perf_counter()
    payload = b"*1\r\n$4\r\nPING\r\n"
    try:
        with socket.create_connection((host, port), timeout=timeout) as conn:
            conn.settimeout(timeout)
            conn.sendall(payload)
            response = conn.recv(64)
        latency_ms = int((time.perf_counter() - started) * 1000)
        ok = response.startswith(b"+PONG")
        detail = f"Redis replied with {response!r}" if response else "Redis closed connection"
        return CheckResult(name="redis", ok=ok, endpoint=endpoint, detail=detail, latency_ms=latency_ms)
    except OSError as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return CheckResult(
            name="redis",
            ok=False,
            endpoint=endpoint,
            detail=f"Redis ping failed: {exc}",
            latency_ms=latency_ms,
        )


def build_local_checks(host: str) -> list[ServiceCheck]:
    return [
        ServiceCheck(
            "kafka",
            f"{host}:9092",
            lambda h, t: tcp_probe(
                "kafka",
                h,
                9092,
                t,
                note="Broker TCP is reachable. This does not validate topic production or consumption.",
            ),
        ),
        ServiceCheck("kafka-ui", f"http://{host}:8085/", lambda h, t: http_probe("kafka-ui", f"http://{h}:8085/", t)),
        ServiceCheck(
            "cassandra",
            f"{host}:9042",
            lambda h, t: tcp_probe(
                "cassandra",
                h,
                9042,
                t,
                note="CQL port is reachable. This does not validate schema or authentication.",
            ),
        ),
        ServiceCheck("redis", f"{host}:6379", lambda h, t: redis_ping(h, 6379, t)),
        ServiceCheck(
            "spark-rpc",
            f"{host}:7077",
            lambda h, t: tcp_probe("spark-rpc", h, 7077, t, note="Spark master RPC is reachable."),
        ),
        ServiceCheck("spark-master-ui", f"http://{host}:8080/", lambda h, t: http_probe("spark-master-ui", f"http://{h}:8080/", t)),
        ServiceCheck("spark-worker-ui", f"http://{host}:8081/", lambda h, t: http_probe("spark-worker-ui", f"http://{h}:8081/", t)),
        ServiceCheck("spark-app-ui", f"http://{host}:4040/", lambda h, t: http_probe("spark-app-ui", f"http://{h}:4040/", t)),
        ServiceCheck("spark-history", f"http://{host}:18080/", lambda h, t: http_probe("spark-history", f"http://{h}:18080/", t)),
        ServiceCheck("streamlit", f"http://{host}:8501/_stcore/health", lambda h, t: http_probe("streamlit", f"http://{h}:8501/_stcore/health", t)),
    ]


def build_remote_checks(host: str) -> list[ServiceCheck]:
    return [
        ServiceCheck("kafka", f"{host}:9092", lambda h, t: tcp_probe("kafka", h, 9092, t)),
        ServiceCheck("cassandra", f"{host}:9042", lambda h, t: tcp_probe("cassandra", h, 9042, t)),
        ServiceCheck("redis", f"{host}:6379", lambda h, t: redis_ping(h, 6379, t)),
        ServiceCheck("spark-rpc", f"{host}:7077", lambda h, t: tcp_probe("spark-rpc", h, 7077, t)),
        ServiceCheck("spark-master-ui", f"http://{host}:8080/", lambda h, t: http_probe("spark-master-ui", f"http://{h}:8080/", t)),
        ServiceCheck("spark-worker-ui", f"http://{host}:8081/", lambda h, t: http_probe("spark-worker-ui", f"http://{h}:8081/", t)),
        ServiceCheck("spark-app-ui", f"http://{host}:4040/", lambda h, t: http_probe("spark-app-ui", f"http://{h}:4040/", t)),
        ServiceCheck("streamlit", f"http://{host}:8501/_stcore/health", lambda h, t: http_probe("streamlit", f"http://{h}:8501/_stcore/health", t)),
    ]


def run_checks(mode: str, host: str, timeout: float) -> list[CheckResult]:
    checks = build_local_checks(host) if mode == "local" else build_remote_checks(host)
    results: list[CheckResult] = []
    with ThreadPoolExecutor(max_workers=min(8, len(checks))) as executor:
        future_map = {executor.submit(check.checker, host, timeout): check for check in checks}
        for future in as_completed(future_map):
            check = future_map[future]
            try:
                results.append(future.result())
            except Exception as exc:  # pragma: no cover
                results.append(
                    CheckResult(
                        name=check.name,
                        ok=False,
                        endpoint=check.endpoint,
                        detail=f"Unhandled error: {exc}",
                        latency_ms=0,
                    )
                )
    return sorted(results, key=lambda item: item.name)


def print_report(results: list[CheckResult]) -> None:
    name_width = max(len("SERVICE"), *(len(item.name) for item in results))
    status_width = len("STATUS")
    endpoint_width = max(len("ENDPOINT"), *(len(item.endpoint) for item in results))
    latency_width = len("LATENCY")

    header = (
        f"{'SERVICE':<{name_width}}  "
        f"{'STATUS':<{status_width}}  "
        f"{'LATENCY':>{latency_width}}  "
        f"{'ENDPOINT':<{endpoint_width}}  "
        "DETAIL"
    )
    print(header)
    print("-" * len(header))
    for item in results:
        status = "OK" if item.ok else "FAIL"
        print(
            f"{item.name:<{name_width}}  "
            f"{status:<{status_width}}  "
            f"{item.latency_ms:>{latency_width}}ms  "
            f"{item.endpoint:<{endpoint_width}}  "
            f"{item.detail}"
        )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check connectivity to the local Docker Compose stack or a remote deployment."
    )
    parser.add_argument(
        "--mode",
        choices=("local", "remote"),
        default="local",
        help="Connectivity profile to use. Default: %(default)s",
    )
    parser.add_argument(
        "--host",
        help="Target host. Defaults to localhost for local mode and 163.223.13.187 for remote mode.",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Per-service timeout in seconds.")
    parser.add_argument("--json-out", help="Optional path to save the raw JSON report.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    host = args.host or (LOCAL_DEFAULT_HOST if args.mode == "local" else REMOTE_DEFAULT_HOST)
    results = run_checks(args.mode, host, args.timeout)
    print_report(results)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump([asdict(item) for item in results], handle, indent=2, ensure_ascii=False)
        print(f"\nSaved JSON report to {args.json_out}")

    return 0 if all(item.ok for item in results) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
