#!/usr/bin/env python3
"""Local synthetic Docker timeout; no Docker daemon or extra packages required."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import time


class SlowDocker(BaseHTTPRequestHandler):
    def do_HEAD(self):
        # Docker starts after socket_listener. Fill the 100-metric channel
        # before the Docker request times out; metric 101 blocks the parser.
        payload = "".join(f"startup_probe,n={i} value=1i\n" for i in range(101))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(payload.encode(), ("127.0.0.1", 18094))
        time.sleep(2)
        try:
            self.send_response(503)
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            pass

    do_GET = do_HEAD

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    with ThreadingHTTPServer(("127.0.0.1", 18095), SlowDocker) as server:
        print("Synthetic Docker endpoint listening on 127.0.0.1:18095", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
