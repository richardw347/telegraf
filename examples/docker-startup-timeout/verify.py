"""Repeat the synthetic startup failure on disposable local Linux processes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time


def wait_for(predicate, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


def owns_udp(pid, port):
    inodes = set()
    for fd in Path(f"/proc/{pid}/fd").glob("*"):
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        if target.startswith("socket:["):
            inodes.add(target[8:-1])
    for table in ("udp", "udp6"):
        path = Path(f"/proc/{pid}/net/{table}")
        if not path.exists():
            continue
        for line in path.read_text().splitlines()[1:]:
            fields = line.split()
            if fields[1].endswith(f":{port:04X}") and fields[3] == "07" and fields[9] in inodes:
                return True
    return False


def trial(args, number):
    directory = args.output / f"{number:03}"
    directory.mkdir()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        udp_port = reservation.getsockname()[1]
    online, requested = threading.Event(), threading.Event()

    class Docker(BaseHTTPRequestHandler):
        def respond(self):
            if not online.is_set():
                if args.mode != "control" and not requested.is_set():
                    payload = "".join(f"startup_probe,n={i} value=1i\n" for i in range(101))
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
                        sender.sendto(payload.encode(), ("127.0.0.1", udp_port))
                requested.set()
                time.sleep(2)
                status, data = 503, {}
            else:
                path = re.sub(r"^/v[0-9.]+", "", self.path.split("?")[0])
                responses = {
                    "/_ping": {},
                    "/info": {"Name": "fixture", "ServerVersion": "28.0.0", "OSType": "linux"},
                    "/containers/json": [{"Id": "test", "Names": ["/startup-fixture"],
                                          "Image": "fixture:latest", "State": "running"}],
                    "/containers/test/json": {"Id": "test", "Config": {}},
                    "/containers/test/stats": {
                        "read": "2026-01-01T00:00:00Z",
                        "memory_stats": {"usage": 2048, "limit": 4096},
                        "cpu_stats": {"cpu_usage": {"total_usage": 20}, "system_cpu_usage": 200, "online_cpus": 1},
                        "precpu_stats": {"cpu_usage": {"total_usage": 10}, "system_cpu_usage": 100},
                    },
                }
                status, data = (200, responses[path]) if path in responses else (404, {})
            body = json.dumps(data).encode()
            try:
                self.send_response(status)
                self.send_header("API-Version", "1.24")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_HEAD = respond
        do_GET = respond

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Docker)
    threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05), daemon=True).start()
    listener = directory / "listener.conf"
    docker = directory / "docker.conf"
    # Same two-file trigger as the minimal example; randomized ports and a
    # faster collection/flush interval allow independent repeat trials.
    listener.write_text(args.config.joinpath("listener.conf").read_text()
                        .replace(":18094", f":{udp_port}").replace('= "1s"', '= "100ms"'))
    docker.write_text(args.config.joinpath("docker.conf").read_text()
                      .replace(":18095", f":{server.server_port}"))
    output, log = directory / "metrics.lp", directory / "telegraf.log"
    record = {"trial": number, "mode": args.mode}
    process = None
    try:
        with output.open("w") as stdout, log.open("w") as stderr:
            process = subprocess.Popen([str(args.binary), "--config", str(listener), "--config", str(docker)],
                                       stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL)
            assert requested.wait(5), "Docker startup never reached"
            if args.mode == "control":
                assert process.wait(timeout=4) == 1, "Expected startup error with no traffic"
                record["exit_code"] = 1
            elif args.mode == "broken":
                # Do not mistake a transient /proc snapshot during socket
                # creation for the post-timeout shutdown state.
                time.sleep(1.25)
                wait_for(lambda: not owns_udp(process.pid, udp_port))
                time.sleep(0.15)
                assert process.poll() is None, "Process unexpectedly exited"
                assert not owns_udp(process.pid, udp_port), "UDP listener is still present"
                assert output.stat().st_size == 0, "Output pipeline unexpectedly ran"
                # SIGQUIT affects only this disposable child; it records the
                # actual blocking stacks instead of inferring a hang by time.
                process.send_signal(signal.SIGQUIT)
                process.wait(timeout=4)
                stacks = log.read_text()
                for frame in ("stopRunningInputs", "startInputs", "StopAndWait", "(*accumulator).AddMetric"):
                    assert frame in stacks, f"Missing stack frame {frame}"
                assert "[chan send]" in stacks
                record.update(owned_udp=False, output_bytes=0, cleanup_stack=True, blocked_parser_stack=True)
            else:
                wait_for(lambda: output.read_text().count("startup_probe,") == 101)
                assert process.poll() is None
                assert owns_udp(process.pid, udp_port)
                # Docker remains unavailable while the other input is flowing.
                record["udp_metrics_before_docker_recovery"] = 101
                online.set()
                wait_for(lambda: all(name in output.read_text() for name in ("docker_container_cpu", "docker_container_mem")))
                assert process.poll() is None
                record.update(docker_cpu=True, docker_memory=True, same_process=True)
            record["passed"] = True
    except Exception as exc:
        record.update(passed=False, error=str(exc))
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        server.shutdown()
        server.server_close()
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--mode", choices=["broken", "control", "patched"], required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    version = subprocess.check_output([str(args.binary), "--version"], text=True).strip()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda i: trial(args, i), range(args.count)))
    summary = {"version": version, "mode": args.mode, "passed": sum(r["passed"] for r in results),
               "total": len(results), "results": results}
    (args.output / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}), flush=True)
    for result in results:
        if not result["passed"]:
            print(json.dumps(result), flush=True)
    raise SystemExit(0 if all(r["passed"] for r in results) else 1)
