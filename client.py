#!/usr/bin/env python3
import os
import sys
import requests
import json
import webbrowser
import argparse

class ClientApi:
    """
    Python Bridge API exposed to JavaScript.
    Handles communication with server.py, JWT storage, and request telemetry.
    """
    def __init__(self, server_url):
        self.token = None
        self.server_url = server_url
        print(f"[CLIENT] Native bridge initialized pointing to server: {self.server_url}")

    def signup(self, name, email, password, avatarUrl, role):
        print(f"\n[CLIENT] Sign-up request for: {email} ({role})")
        try:
            url = f"{self.server_url}/api/auth/register"
            payload = {
                "name": name,
                "email": email,
                "password": password,
                "avatarUrl": avatarUrl,
                "role": role
            }
            res = requests.post(url, json=payload)
            data = res.json()
            if res.status_code in (200, 201) and "user" in data:
                print(f"[CLIENT] Sign-up successful for {data['user']['name']}!")
            else:
                print(f"[CLIENT] Sign-up failed: {data.get('error', 'Unknown error')}")
            return data
        except Exception as e:
            print(f"[CLIENT] Sign-up connection error: {e}")
            return {"error": f"Connection failed: {str(e)}"}

    def login(self, email, password):
        print(f"\n[CLIENT] Authenticating user: {email}")
        try:
            url = f"{self.server_url}/api/auth/login"
            payload = {
                "email": email,
                "password": password
            }
            res = requests.post(url, json=payload)
            data = res.json()
            if res.status_code == 200:
                self.token = data.get("access_token")
                print(f"[CLIENT] Secure authentication successful!")
                print(f"[CLIENT] In-memory Access Token locked: {self.token[:12]}...")
            else:
                print(f"[CLIENT] Authentication failed: {data.get('error', 'Invalid Credentials')}")
            return data
        except Exception as e:
            print(f"[CLIENT] Authentication connection error: {e}")
            return {"error": f"Connection failed: {str(e)}"}

    def send_authenticated_request(self, method, endpoint, data=None):
        print(f"[CLIENT] HTTP {method} -> {endpoint}")
        try:
            headers = {}
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            url = f"{self.server_url}{endpoint}"

            if method.upper() == "GET":
                res = requests.get(url, headers=headers)
            elif method.upper() == "POST":
                res = requests.post(url, headers=headers, json=data)
            elif method.upper() == "PUT":
                res = requests.put(url, headers=headers, json=data)
            elif method.upper() == "DELETE":
                res = requests.delete(url, headers=headers)
            else:
                return {"error": f"HTTP method '{method}' is not supported."}

            return res.json()
        except Exception as e:
            print(f"[CLIENT] Request failed: {e}")
            return {"error": f"Request failed: {str(e)}"}

def main():
    parser = argparse.ArgumentParser(description="Digital Pathways Multi-Client")
    parser.add_argument("--port", type=int, default=8000, help="Central server port")
    parser.add_argument("--server", type=str, default="http://localhost:8000", help="Full server URL override")
    args = parser.parse_args()

    port = os.environ.get("PORT", args.port)
    server_url = f"http://localhost:{port}" if not args.server else args.server

    print("\n==========================================================")
    print("      DIGITAL PATHWAYS 3.0 STUDENT WORKBENCH CLIENT")
    print("==========================================================")

    api = ClientApi(server_url)

    try:
        import webview
        print("* Starting secure PyWebView desktop window shell...")
        webview.create_window(
            title="Digital Pathways 3.0 Studio - Student Client Workspace",
            url=f"{server_url}/?pov=client",
            js_api=api,
            width=1200,
            height=850
        )
        webview.start()
    except Exception as e:
        print(f"\n[!] Notice: PyWebView native UI display block is unavailable (Headless Workspace environment).")
        print(f"    Error detail: {e}")
        print(f"    Conduiting login workflow through standard web service portal instead:")
        print(f"    {server_url}/?pov=client")
        webbrowser.open(f"{server_url}/?pov=client")

if __name__ == "__main__":
    main()
