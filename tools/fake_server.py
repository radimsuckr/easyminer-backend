#!/usr/bin/env python3
import json
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer


class MockServerHandler(BaseHTTPRequestHandler):
    """Mock server for Easyminer Center API"""

    def dump_request(self):
        """Helper method to dump request information"""
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Get request headers
        headers = {}
        for header in self.headers:
            headers[header] = self.headers[header]

        # Build request info
        request_info = {
            "timestamp": timestamp,
            "method": self.command,
            "path": self.path,
            "headers": headers,
            "client_address": f"{self.client_address[0]}:{self.client_address[1]}",
            "protocol_version": self.protocol_version,
        }

        # Print to console
        print("\n=== INCOMING REQUEST ===")
        print(f"Timestamp: {timestamp}")
        print(f"Method: {self.command}")
        print(f"Path: {self.path}")
        print(f"Client: {self.client_address[0]}:{self.client_address[1]}")
        print(f"Protocol: {self.protocol_version}")
        print("Headers:")
        for header, value in headers.items():
            print(f"  {header}: {value}")
        print("=====================\n")

        return request_info

    def do_GET(self):
        # Dump the request details
        self.dump_request()

        # Set headers for JSON responses
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        # Handle /api/auth endpoint
        if self.path == "/api/auth":
            response = {
                "id": 2325,  # Changed to integer
                "name": "test202307@vojir.net",
                "email": "test202307@vojir.net",
                "role": [  # Changed from "roles" to "role"
                    "authenticated",
                    "owner:2325",
                ],
            }
            self.wfile.write(json.dumps(response, indent=2).encode("utf-8"))

        # Handle /databases/{dbType} endpoint with regex pattern
        elif re.match(r"/api/databases/\w+", self.path):
            response = {
                "server": "192.168.88.253",
                "port": 3306,  # Added port field as integer
                "username": "easyminer",
                "password": "easyminer",
                "database": "easyminer",
                # Removed nested "database" object
            }
            self.wfile.write(json.dumps(response, indent=2).encode("utf-8"))

        else:
            # If path doesn't match any endpoint, return a 404
            self.send_error(404, "Not Found")


def run(port: int, server_class=HTTPServer, handler_class=MockServerHandler):
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting mock server at http://localhost:{port}")
    print("Press Ctrl+C to stop the server")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server")
        httpd.server_close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the mock server")
    parser.add_argument(
        "--port", type=int, default=8001, help="Port to run the server on"
    )
    args = parser.parse_args()
    run(port=args.port)
