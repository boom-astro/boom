from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess

def check_process(pattern):
    result = subprocess.run(
        ["pgrep", "-f", pattern],
        stdout=subprocess.DEVNULL
    )
    print(result)
    return result.returncode == 0

# HTTP server to check if the kafka consumer or scheduler are running
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/scheduler_health":
            status = check_process("/app/scheduler")
            self.respond(200 if status else 503,
                         f"scheduler {'is healthy' if status else 'unhealthy'}\n")
        elif self.path == "/consumer_health":
            status = check_process("/app/kafka_consume")
            self.respond(200 if status else 503,
                         f"consumer {'is healthy' if status else 'unhealthy'}\n")
        else:
            self.respond(404, "unknown endpoint")

    def respond(self, code, message):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(message)))
        self.end_headers()
        self.wfile.write(message.encode())

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5555), HealthHandler)
    server.serve_forever()
