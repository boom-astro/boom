from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess

def check_process(process_name):
    """Call the shell process-healthcheck.sh script and return True if healthy."""
    try:
        subprocess.run(
            [f"apptainer/scripts/healthcheck/process-healthcheck.sh", process_name],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        return True
    except subprocess.CalledProcessError:
        return False

# HTTP server to check if the kafka consumer or scheduler are running
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.respond(200, "ok\n")
        elif self.path.startswith("/consumer_health"):
            survey = self.path.removeprefix("/consumer_health").strip("/")
            process = f"/app/kafka_consumer {survey}" if survey else "/app/kafka_consumer"
            label = f"consumer {survey}" if survey else "consumer"
            status = check_process(process)
            self.respond(200 if status else 503,
                         f"{label} {'is healthy' if status else 'unhealthy'}\n")
        elif self.path.startswith("/scheduler_health"):
            survey = self.path.removeprefix("/scheduler_health").strip("/")
            process = f"/app/scheduler {survey}" if survey else "/app/scheduler"
            label = f"scheduler {survey}" if survey else "scheduler"
            status = check_process(process)
            self.respond(200 if status else 503,
                         f"{label} {'is healthy' if status else 'unhealthy'}\n")
        elif self.path == "/otel_health":
            status = check_process("/otelcol")
            self.respond(200 if status else 503,
                         f"Otel collector {'is healthy' if status else 'unhealthy'}\n")
        else:
            self.respond(404, "unknown endpoint")

    def respond(self, code, message):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(message)))
        self.end_headers()
        self.wfile.write(message.encode())

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5554), HealthHandler)  # type: ignore[arg-type]
    server.serve_forever()
