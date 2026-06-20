#!/usr/bin/env python3
import os
import sys
import json
import hmac
import hashlib
import base64
import time
import datetime
import threading
import argparse
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response, Depends, status, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# ANSI CLI custom logs styling
ANSI_RESET = "\033[0m"
ANSI_BLUE = "\033[94m"
ANSI_GREEN = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_RED = "\033[91m"
ANSI_CYAN = "\033[96m"
ANSI_BOLD = "\033[1m"

# Token Secret Key coordinates
JWT_SECRET = "digital-pathways-super-secret-key-3.0"

def hash_password(password: str) -> str:
    """Consistently hashes clear password using secure SHA256 hashing."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def check_password(plain_password: str, stored_password_or_hash: str) -> bool:
    """Verifies clear password against stored hash, supporting plain text for backward compatibility."""
    if plain_password == stored_password_or_hash:
        return True
    return hash_password(plain_password) == stored_password_or_hash

def create_access_token(data: dict) -> str:
    """Generates standard secure access token with signature."""
    payload_str = json.dumps(data)
    encoded_payload = base64.urlsafe_b64encode(payload_str.encode('utf-8')).decode('utf-8')
    sig = hmac.new(JWT_SECRET.encode('utf-8'), encoded_payload.encode('utf-8'), hashlib.sha256).hexdigest()
    return f"{encoded_payload}.{sig}"

def verify_access_token(token: str) -> dict:
    """Decodes and validates secure token coordinates."""
    try:
        parts = token.split(".")
        if len(parts) != 2:
            return None
        encoded_payload, sig = parts[0], parts[1]
        expected_sig = hmac.new(JWT_SECRET.encode('utf-8'), encoded_payload.encode('utf-8'), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        payload_str = base64.urlsafe_b64decode(encoded_payload.encode('utf-8')).decode('utf-8')
        return json.loads(payload_str)
    except Exception:
        return None

# Database lock setup
def get_db_file_path():
    import sys
    if getattr(sys, "frozen", False):
        return os.path.join(os.path.dirname(sys.executable), "database.json")
    return os.path.join(os.getcwd(), "database.json")

DB_FILE = get_db_file_path()
db_lock = threading.Lock()

def load_db():
    """Reads structured state safely from JSON file."""
    with db_lock:
        if not os.path.exists(DB_FILE):
            initial = {"opportunities": [], "registrations": [], "users": []}
            with open(DB_FILE, "w", encoding="utf-8") as f:
                json.dump(initial, f, indent=2)
            return initial
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {
                    "opportunities": data.get("opportunities", []),
                    "registrations": data.get("registrations", []),
                    "users": data.get("users", [])
                }
        except Exception as e:
            print(f"[DB Load Error]: {e}")
            return {"opportunities": [], "registrations": [], "users": []}

def save_db(data):
    """Saves structured state safely into JSON file."""
    with db_lock:
        try:
            with open(DB_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[DB Save Error]: {e}")

# Application core state trackers
active_websockets = set()
system_logs = []
emails_delivered_count = 0
DEBUG_MODE = False

def add_log(source: str, level: str, message: str, details: dict = None):
    """Logs system telemetry to CMD CLI stdout, stores, and schedules WS alerts."""
    global DEBUG_MODE
    if DEBUG_MODE:
        # In debug mode, print extra tracing information to standard output
        print(f"[DEBUG_TRACE] Enqueueing system log details. Source: {source}, Priority: {level}")
        if details:
            print(f"[DEBUG_TRACE] Metadata keys payload: {list(details.keys())}")
            
    timestamp = datetime.datetime.now().isoformat()
    log_id = f"log-{int(time.time()*1000)}-{os.urandom(2).hex()}"
    new_log = {
        "id": log_id,
        "timestamp": timestamp,
        "source": source,
        "level": level,
        "message": message,
        "details": details or {}
    }

    system_logs.append(new_log)
    if len(system_logs) > 200:
        system_logs.pop(0)

    # CLI Output formatting
    level_color = ANSI_BLUE
    if level == "success":
        level_color = ANSI_GREEN
    elif level == "warn":
        level_color = ANSI_YELLOW
    elif level == "error":
        level_color = ANSI_RED

    time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ANSI_BOLD}[{time_str}]{ANSI_RESET} {level_color}{level.upper()}{ANSI_RESET} ({source}): {message}")
    if details:
        try:
            print(f"    Details: {json.dumps(details, indent=2)}")
        except Exception:
            pass

    # Schedule WebSocket push if loop exists
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            loop.create_task(broadcast_to_clients({"type": "SYSTEM_LOG", "log": new_log}))
    except RuntimeError:
        # No running event loop
        pass
    except Exception:
        pass

async def broadcast_to_clients(payload: dict):
    """Dispatches JSON stringified payload to active WebSocket dashboards."""
    if not active_websockets:
        return
    message_str = json.dumps(payload)
    disconnected = set()
    for ws in list(active_websockets):
        try:
            await ws.send_text(message_str)
        except Exception:
            disconnected.add(ws)
    for ws in disconnected:
        active_websockets.discard(ws)

def get_stats():
    """Compiles statistics counters across database records."""
    db = load_db()
    return {
        "connectedWorkers": len(active_websockets),
        "totalRegistrations": len(db.get("registrations", [])),
        "totalOpportunities": len(db.get("opportunities", [])),
        "emailsDelivered": emails_delivered_count,
        "activeClientsList": [],
        "totalRegisteredUsers": len(db.get("users", []))
    }

async def broadcast_stats():
    """Syncs aggregate stats metrics across all visual displays."""
    await broadcast_to_clients({
        "type": "STATS_UPDATE",
        "stats": get_stats()
    })

# FastAPI configuration
app = FastAPI(title="Digital Pathways 3.0 Platform Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication Bearer handler
security = HTTPBearer()

def get_current_user_identity(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Validates authorization token and maps individual user scope on demand."""
    token = credentials.credentials
    identity = verify_access_token(token)
    if not identity:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials. Access Token is invalid or expired.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return identity

# --- API ENDPOINTS ---

@app.post("/signup")
@app.post("/api/auth/register")
async def api_signup(request: Request):
    """Registers user accounts, hashes passwords, and locks persistent identity records."""
    try:
        body = await request.json()
        name = body.get("name")
        email = body.get("email")
        password = body.get("password")
        avatarUrl = body.get("avatarUrl", "https://api.dicebear.com/7.x/pixel-art/svg?seed=Jack")
        role = body.get("role", "Scholar")
        try:
            age = int(body.get("age")) if body.get("age") is not None else None
        except (ValueError, TypeError):
            age = 20
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid registration payload specifications.")

    if not name or not email or not password:
        raise HTTPException(status_code=400, detail="Name, email and password coordinates are required.")

    db = load_db()
    for u in db.get("users", []):
        if u.get("email", "").strip().lower() == email.strip().lower():
            raise HTTPException(status_code=400, detail="An account verified for this email already exists.")

    user_id = f"usr-{int(time.time()*1000)}"
    new_user = {
        "id": user_id,
        "name": name,
        "email": email,
        "password": hash_password(password),
        "avatarUrl": avatarUrl,
        "role": role,
        "age": age,
        "created_at": datetime.datetime.utcnow().isoformat() + "Z"
    }

    db["users"].append(new_user)
    save_db(db)

    add_log("PLATFORM_API", "success", f"Registered new user profile '{name}' <{email}> successfully.")
    
    token = create_access_token({"email": email, "id": user_id})
    client_user = {"name": name, "email": email, "avatarUrl": avatarUrl, "role": role, "age": age}

    await broadcast_stats()

    return {
        "status": "success",
        "access_token": token,
        "token_type": "bearer",
        "user": client_user
    }

@app.post("/login")
@app.post("/api/auth/login")
async def api_login(request: Request):
    """Authenticates student accounts, signs security token credentials, and tracks sessions."""
    try:
        body = await request.json()
        email = body.get("email")
        password = body.get("password")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid credential parameters.")

    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password coordinates are required.")

    db = load_db()
    user = None
    for u in db.get("users", []):
        if u.get("email", "").strip().lower() == email.strip().lower():
            if check_password(password, u.get("password")):
                user = u
                break

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials. Verify login coordinates.")

    token = create_access_token({"email": user.get("email"), "id": user.get("id")})
    client_user = {
        "name": user.get("name"),
        "email": user.get("email"),
        "avatarUrl": user.get("avatarUrl", ""),
        "role": user.get("role", "Scholar"),
        "age": user.get("age")
    }

    add_log("PLATFORM_API", "success", f"Successful login verification for user '{user.get('name')}' <{email}>.")
    await broadcast_stats()

    return {
        "status": "success",
        "access_token": token,
        "token_type": "bearer",
        "user": client_user
    }

@app.get("/api/opportunities")
@app.get("/opportunities")
async def api_get_opportunities():
    """Serves active opportunities catalog index listings."""
    db = load_db()
    return db.get("opportunities", [])

@app.get("/api/opportunities/{opportunity_id}")
@app.get("/opportunities/{opportunity_id}")
async def api_get_opp_by_id(opportunity_id: str):
    """Details specific programmatic opportunity parameters."""
    db = load_db()
    for opp in db.get("opportunities", []):
        if opp.get("id") == opportunity_id:
            return opp
    raise HTTPException(status_code=404, detail="Matching education program was not found.")

@app.post("/api/opportunities")
@app.post("/opportunities")
async def api_post_opportunity(request: Request):
    """Stores custom newly published opportunity into the catalogue."""
    db = load_db()
    try:
        opp = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Failed to parse custom program context.")

    opp_id = f"opp-py-{int(time.time()*1000)}"
    opp["id"] = opp_id
    opp["created_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    opp["seats_left"] = opp.get("seats", 100)

    db["opportunities"].insert(0, opp)
    save_db(db)

    add_log("PLATFORM_API", "success", f"Platform registered custom opportunity: '{opp.get('title')}'")
    await broadcast_stats()
    return {"status": "success", "opportunity": opp}

@app.post("/api/register/{opportunity_id}")
async def api_register_candidate(opportunity_id: str, request: Request):
    """Coordinates candidate applications, decrements capacity metrics, and simulates SMTP mail pipelines."""
    try:
        body = await request.json()
        responses = body.get("responses", {})
        user_email = body.get("user_email")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid candidate questionnaire responses.")

    db = load_db()
    opportunity = None
    for opp in db.get("opportunities", []):
        if opp.get("id") == opportunity_id:
            opportunity = opp
            break

    if not opportunity:
        raise HTTPException(status_code=404, detail="Opportunity block not found.")

    # Backend Age restriction check
    required_age = opportunity.get("required_age")
    if required_age and int(required_age) > 0:
        candidate_email = user_email or responses.get("Email Address") or responses.get("Email") or responses.get("email")
        user_age = None
        if candidate_email:
            for u in db.get("users", []):
                if u.get("email", "").strip().lower() == str(candidate_email).strip().lower():
                    user_age = u.get("age")
                    break
        if user_age is None:
            # Check responses as a fallback
            for key, val in responses.items():
                if "age" in key.lower() or "dob" in key.lower():
                    try:
                        user_age = int(val)
                        break
                    except Exception:
                        pass
        
        # Enforce minimum age
        if user_age is None or int(user_age) < int(required_age):
            user_age_str = str(user_age) if user_age is not None else "unspecified"
            raise HTTPException(
                status_code=403, 
                detail=f"Access Denied: You must be at least {required_age} years old to register for this opportunity. (Your age is {user_age_str})."
            )

    if opportunity.get("seats_left", 0) <= 0:
        raise HTTPException(status_code=403, detail="Lockout: Seat capacity limit has been exceeded.")

    # Decrements workshop capacity
    opportunity["seats_left"] = max(0, opportunity.get("seats_left", 1) - 1)

    reg_id = f"reg-{int(time.time()*1000)}"
    new_reg = {
        "id": reg_id,
        "opportunity_id": opportunity_id,
        "user_email": user_email,
        "status": "registered",
        "responses": responses,
        "submitted_at": datetime.datetime.utcnow().isoformat() + "Z"
    }

    db["registrations"].append(new_reg)
    save_db(db)

    # Dispatches live SMTP email log updates
    recipient_mail = responses.get("Email Address") or responses.get("Email") or responses.get("email") or "Candidate Scholar"
    email_details = {
        "recipient": recipient_mail,
        "subject": f"Digital Pathways Registration: {opportunity.get('title')}",
        "body": f"Welcome! This acts as an automated acknowledgment confirming program entry for registration ID {reg_id}."
    }

    global emails_delivered_count
    emails_delivered_count += 1

    add_log("SMTP_SERVER", "success", f"SMTP dispatcher relayed email bulletin to <{recipient_mail}>.", email_details)
    add_log("PLATFORM_API", "success", f"Applied dynamically to catalog program ID '{opportunity_id}' successfully.", {"id": reg_id})

    await broadcast_stats()
    return {"status": "success", "registration_id": reg_id}

@app.get("/api/my-registrations")
async def api_get_my_registrations(email: str):
    """Filters registered entries mapped to student candidate profile email."""
    db = load_db()
    joined = []
    for reg in db.get("registrations", []):
        responses = reg.get("responses", {})
        reg_user_email = reg.get("user_email") or ""
        is_match = (reg_user_email.strip().lower() == email.strip().lower())
        if not is_match:
            for val in responses.values():
                if isinstance(val, str) and val.strip().lower() == email.strip().lower():
                    is_match = True
                    break
        if is_match:
            opp = next((o for o in db.get("opportunities", []) if o.get("id") == reg.get("opportunity_id")), None)
            joined.append({
                "registration_id": reg.get("id"),
                "opportunity_id": reg.get("opportunity_id"),
                "opportunity_title": opp.get("title") if opp else "Unknown Program",
                "date_of_event": opp.get("date") if opp else None,
                "status": reg.get("status") or "registered",
                "submitted_at": reg.get("submitted_at"),
                "responses": responses
            })
    return joined

@app.get("/api/poster-data")
async def api_get_poster_data(email: str):
    """Filters posted opportunities and the corresponding candidates' form answers."""
    db = load_db()
    my_opps = [opp for opp in db.get("opportunities", []) if opp.get("poster_email") and opp.get("poster_email").strip().lower() == email.strip().lower()]
    opp_ids = {opp.get("id") for opp in my_opps}
    my_applicants = []
    for reg in db.get("registrations", []):
        if reg.get("opportunity_id") in opp_ids:
            opp = next((o for o in my_opps if o.get("id") == reg.get("opportunity_id")), None)
            my_applicants.append({
                "registration_id": reg.get("id"),
                "opportunity_id": reg.get("opportunity_id"),
                "opportunity_title": opp.get("title") if opp else "Unknown Program",
                "status": reg.get("status") or "registered",
                "user_email": reg.get("user_email") or "",
                "submitted_at": reg.get("submitted_at"),
                "responses": reg.get("responses", {})
            })
    return {
        "opportunities": my_opps,
        "applicants": my_applicants
    }

@app.get("/api/admin/users")
async def api_get_users_matrix():
    """Generates deep profile audit records mapping users across hosted and joined initiatives."""
    db = load_db()
    matrix = []
    for u in db.get("users", []):
        email = u.get("email", "")

        hosted = []
        for opp in db.get("opportunities", []):
            if opp.get("poster_email", "").strip().lower() == email.strip().lower():
                hosted.append({
                    "id": opp.get("id"),
                    "title": opp.get("title"),
                    "date": opp.get("date"),
                    "category": opp.get("category"),
                    "seats": opp.get("seats"),
                    "seats_left": opp.get("seats_left")
                })

        joined = []
        for reg in db.get("registrations", []):
            responses = reg.get("responses", {})
            is_match = False
            for val in responses.values():
                if isinstance(val, str) and val.strip().lower() == email.strip().lower():
                    is_match = True
                    break
            if is_match:
                opp = next((o for o in db.get("opportunities", []) if o.get("id") == reg.get("opportunity_id")), None)
                joined.append({
                    "registration_id": reg.get("id"),
                    "opportunity_id": reg.get("opportunity_id"),
                    "opportunity_title": opp.get("title") if opp else "Unknown Program",
                    "date_of_event": opp.get("date") if opp else None,
                    "submitted_at": reg.get("submitted_at"),
                    "responses": responses
                })

        matrix.append({
            "name": u.get("name"),
            "email": u.get("email"),
            "avatarUrl": u.get("avatarUrl"),
            "role": u.get("role"),
            "joinedAt": u.get("created_at") or datetime.datetime.utcnow().isoformat() + "Z",
            "hosted": hosted,
            "joined": joined
        })
    return matrix

@app.get("/api/system-logs")
async def api_get_system_logs():
    """Retrieves transactional logs backlog."""
    return system_logs

@app.post("/api/system-reset")
async def api_system_reset():
    """Purges persistent database records and updates connected visual clients immediately."""
    empty_state = {"opportunities": [], "registrations": [], "users": []}
    save_db(empty_state)

    global emails_delivered_count
    emails_delivered_count = 0
    system_logs.clear()

    add_log("PLATFORM_API", "success", "Database reset complete: No opportunities remain in the registry catalog.")
    await broadcast_stats()
    return {"status": "success", "message": "Database reset completed with zero records."}

@app.post("/api/admin/seed-opportunity")
async def api_seed_sys_opportunity(request: Request):
    """Enables server-side direct seeding of default computational mathematics resources."""
    try:
        body = await request.json()
        opportunity = body.get("opportunity")
    except Exception:
        opportunity = None

    db = load_db()
    opp_id = f"opp-seed-py-{int(time.time()*1000)}"
    if opportunity:
        new_opp = {**opportunity, "id": opp_id, "created_at": datetime.datetime.utcnow().isoformat() + "Z"}
    else:
        new_opp = {
            "id": opp_id,
            "title": "Academic Research Symposium (Seeded by Server)",
            "description": "Official server-registered computational math seminar. Explore graph theories, high density network flows, and automatic scheduling solvers.",
            "date": "2026-08-10",
            "category": "workshop",
            "poster_email": "administrative-core@school.edu",
            "seats": 45,
            "seats_left": 45,
            "fields": [
                {"label": "Full Name", "type": "text", "required": True},
                {"label": "Email Address", "type": "email", "required": True},
                {"label": "Research Interest Proposal", "type": "textarea", "required": True}
            ],
            "created_at": datetime.datetime.utcnow().isoformat() + "Z"
        }

    db["opportunities"].insert(0, new_opp)
    save_db(db)

    add_log("PLATFORM_API", "success", f"Server administrative core registered opportunity '{new_opp['title']}' successfully.", {"id": opp_id})
    await broadcast_stats()
    return {"status": "success", "opportunity": new_opp}

@app.post("/api/admin/shutdown")
async def api_shutdown_system():
    """Unloads websocket connections and exits server wrapper processes cleanly."""
    add_log("PLATFORM_API", "warn", "Server shutdown request received. Terminating all active sockets.")
    await broadcast_to_clients({
        "type": "SERVER_SHUTDOWN",
        "message": "The administrative core has issued a server shutdown notice. WebSocket pipelines detached."
    })

    def shutdown_process():
        time.sleep(1.0)
        os._exit(0)

    threading.Thread(target=shutdown_process, daemon=True).start()
    return {"status": "success", "message": "Administrative server termination initiated."}

# --- WEBSOCKET HANDLERS ---

@app.websocket("/ws/client")
async def websocket_endpoint(websocket: WebSocket):
    """Establishes persistent full-duplex communication links with frontend visualization segments."""
    await websocket.accept()
    active_websockets.add(websocket)
    add_log("PLATFORM_API", "info", "Visual monitor attached to secure WebSocket pipeline.")

    # Send initial diagnostic payload
    initial_payload = {
        "type": "INITIAL_SETUP",
        "logs": system_logs[-100:] if len(system_logs) > 100 else system_logs,
        "stats": get_stats()
    }
    await websocket.send_json(initial_payload)

    try:
        while True:
            # Maintain linkage open and handle active session packets
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "CLIENT_SESSION_ACTIVE":
                    user = msg.get("user")
                    if user:
                        email = user.get("email")
                        add_log("PLATFORM_API", "info", f"User session validated as Active: '{email}' over WebSocket.")
                        await broadcast_stats()
            except Exception:
                pass
    except WebSocketDisconnect:
        active_websockets.discard(websocket)
        add_log("PLATFORM_API", "info", "Visual monitor detached from secure WebSocket pipeline.")

# --- STATIC FILES SERVING ---

def get_dist_path():
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, "dist")
    file_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dist")
    if os.path.exists(file_dir):
        return file_dir
    return os.path.join(os.getcwd(), "dist")

dist_path = get_dist_path()
if os.path.exists(dist_path):
    print(f"* Compiled React assets detected in {dist_path}. Mounting express-compatible assets...")
    app.mount("/assets", StaticFiles(directory=os.path.join(dist_path, "assets")), name="assets")

    @app.get("/{rest_of_path:path}", response_class=FileResponse)
    async def serve_spa_built_resources(rest_of_path: str):
        filepath = os.path.join(dist_path, rest_of_path)
        if rest_of_path and os.path.exists(filepath) and os.path.isfile(filepath):
            return FileResponse(filepath)
        return FileResponse(os.path.join(dist_path, "index.html"))
else:
    print(f"\n[!] Warning: Built resources path 'dist/' is missing.")
    @app.get("/", response_class=HTMLResponse)
    async def fallback_home():
        return """
        <html>
            <head><title>Digital Pathways 3.0 Fallback</title></head>
            <style>
                body { font-family: -apple-system, sans-serif; background: #0f172a; color: #f8fafc; padding: 100px; text-align: center; }
                code { background: #1e293b; padding: 4px 8px; border-radius: 4px; }
            </style>
            <body>
                <h1>dist/ Static Files Missing</h1>
                <p>Run <code>npm run build</code> first to compile the Vite/React frontend, then restart this server.</p>
            </body>
        </html>
        """

# --- GUI LAUNCHER AND INTEGRATION ---
try:
    import tkinter as tk
    from tkinter import ttk, scrolledtext, messagebox
    import webbrowser
    HAS_GUI = True
except ImportError:
    HAS_GUI = False

class ClientApi:
    """
    Python Bridge API exposed to JavaScript (PyWebView).
    Handles communication with server.py, JWT storage, and request telemetry.
    """
    def __init__(self, server_url):
        self.token = None
        self.server_url = server_url
        print(f"[CLIENT] Native bridge initialized pointing to server: {self.server_url}")

    def signup(self, name, email, password, avatarUrl, role):
        print(f"\n[CLIENT] Sign-up request for: {email} ({role})")
        try:
            import requests
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
            import requests
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
            import requests
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

    def download_csv(self, filename, content):
        """Allows Saving CSV Files natively inside pywebview."""
        print(f"[CLIENT] Native CSV download request for '{filename}'")
        try:
            import webview
            active_win = webview.active_window()
            if active_win:
                save_path = active_win.create_file_dialog(
                    webview.SAVE_DIALOG,
                    save_filename=filename,
                    file_types=('CSV Files (*.csv)', 'All files (*.*)')
                )
                if save_path:
                    if isinstance(save_path, list) or isinstance(save_path, tuple):
                        if len(save_path) > 0:
                            save_path = save_path[0]
                        else:
                            return {"success": False, "error": "No file path selected"}
                    with open(save_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"[CLIENT] Saved native file CSV to: {save_path}")
                    return {"success": True, "path": save_path}
            return {"success": False, "error": "User cancelled or no active window"}
        except Exception as e:
            print(f"[CLIENT] Save File native error: {e}")
            return {"success": False, "error": str(e)}

def run_server_gui(port, debug):
    # Set process DPI awareness on Windows for ultra-sharp typography
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass

    root = tk.Tk()
    root.title("Digital Pathways 3.0 — Visual Control Desk")
    root.geometry("1000x650")
    root.configure(bg="#0f172a") # Slate 900 background

    # Force a dark theme palette styling
    style = ttk.Style()
    style.theme_use("clam")
    style.configure(".", background="#0f172a", foreground="#f8fafc")
    style.configure("TFrame", background="#0f172a")
    style.configure("Card.TFrame", background="#1e293b", relief="flat")
    
    # Header Panel
    header = tk.Frame(root, bg="#1e293b", height=70, bd=0)
    header.pack(fill=tk.X, side=tk.TOP)
    header.pack_propagate(False)

    title_label = tk.Label(
        header, 
        text="DIGITAL PATHWAYS 3.0", 
        font=("Helvetica", 14, "bold"), 
        bg="#1e293b", 
        fg="#6366f1"
    )
    title_label.pack(side=tk.LEFT, padx=20, pady=10)

    sub_title = tk.Label(
        header, 
        text="— ADMINISTRATIVE SERVER DESK", 
        font=("Helvetica", 10, "bold"), 
        bg="#1e293b", 
        fg="#94a3b8"
    )
    sub_title.pack(side=tk.LEFT, pady=13)

    # Status indicator badge
    status_indicator = tk.Label(
        header, 
        text="● ONLINE", 
        font=("Helvetica", 10, "bold"), 
        bg="#1e293b", 
        fg="#10b981", # Emerald green
        padx=15
    )
    status_indicator.pack(side=tk.RIGHT, padx=20)

    # Body Container Frame
    body = tk.Frame(root, bg="#0f172a")
    body.pack(fill=tk.BOTH, expand=True, padx=20, pady=25)

    # Column 1: Metrics panel (Left)
    col_left = tk.Frame(body, bg="#0f172a", width=380)
    col_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(0, 20))
    col_left.pack_propagate(False)

    # Stats Card
    stats_frame = tk.LabelFrame(
        col_left, 
        text=" REAL-TIME STATISTICS ", 
        font=("Helvetica", 10, "bold"),
        bg="#1e293b", 
        fg="#818cf8",
        labelanchor="nw",
        padx=15, 
        pady=15,
        bd=1,
        relief="solid",
        highlightbackground="#334155"
    )
    stats_frame.pack(fill=tk.X, pady=(0, 20))

    # Stats detail rows
    def create_stat_row(parent, title, value_var):
        row = tk.Frame(parent, bg="#1e293b")
        row.pack(fill=tk.X, pady=6)
        lbl = tk.Label(row, text=title, font=("Helvetica", 9), bg="#1e293b", fg="#94a3b8", anchor="w")
        lbl.pack(side=tk.LEFT)
        val = tk.Label(row, textvariable=value_var, font=("Helvetica", 11, "bold"), bg="#1e293b", fg="#f8fafc", anchor="e")
        val.pack(side=tk.RIGHT)

    # Thread-safe stats variables
    active_monitors_var = tk.StringVar(value="0")
    total_opps_var = tk.StringVar(value="0")
    total_regs_var = tk.StringVar(value="0")
    emails_sent_var = tk.StringVar(value="0")
    server_port_var = tk.StringVar(value=f"{port}")

    create_stat_row(stats_frame, "Central Service Port:", server_port_var)
    create_stat_row(stats_frame, "Active Socket Workers:", active_monitors_var)
    create_stat_row(stats_frame, "Total Opportunities Cataloged:", total_opps_var)
    create_stat_row(stats_frame, "Candidate Registrations Submitted:", total_regs_var)
    create_stat_row(stats_frame, "SMTP Bulk Email Bulletins Sent:", emails_sent_var)

    # Action Frame
    actions_frame = tk.LabelFrame(
        col_left, 
        text=" SYSTEM OPERATIONS CENTER ", 
        font=("Helvetica", 10, "bold"),
        bg="#1e293b", 
        fg="#818cf8",
        labelanchor="nw",
        padx=15, 
        pady=15,
        bd=1,
        relief="solid",
        highlightbackground="#334155"
    )
    actions_frame.pack(fill=tk.BOTH, expand=True)

    # Actions buttons
    btn_style = {
        "font": ("Helvetica", 9, "bold"),
        "bd": 0,
        "height": 2,
        "cursor": "hand2",
        "activebackground": "#4f46e5",
        "activeforeground": "#ffffff"
    }

    # Action methods
    def launch_desktop():
        def run_gui():
            try:
                import webview
                api = ClientApi(f"http://localhost:{port}")
                webview.create_window(
                    title="Digital Pathways 3.0 Studio - Student Client Workspace",
                    url=f"http://localhost:{port}/?pov=client",
                    js_api=api,
                    width=1220,
                    height=840
                )
                webview.start()
            except ImportError:
                messagebox.showerror(
                    "WebView Integration Error", 
                    "PyWebView library is not installed locally. Conduiting platform login through external browser instead!"
                )
                webbrowser.open(f"http://localhost:{port}/?pov=client")
            except Exception as e:
                add_log("PLATFORM_API", "error", f"Desktop Webview launch issue: {e}")
                webbrowser.open(f"http://localhost:{port}/?pov=client")

        threading.Thread(target=run_gui, daemon=True).start()

    def launch_organizer():
        webbrowser.open(f"http://localhost:{port}/")

    def seed_academic():
        try:
            db = load_db()
            opp_id = f"opp-seed-py-{int(time.time()*1000)}"
            new_opp = {
                "id": opp_id,
                "title": "Academic Research Symposium (Seeded via Client Panel)",
                "description": "Premium administrative-seeded research course. Dive deep into computational mathematics, relational database engines, dynamic network matrices, and visual graphing structures.",
                "date": "2026-09-24",
                "category": "symposium",
                "poster_email": "administrative-core@school.edu",
                "seats": 50,
                "seats_left": 50,
                "fields": [
                    {"label": "Full Name", "type": "text", "required": True},
                    {"label": "Email Address", "type": "email", "required": True},
                    {"label": "Research Proposal Pitch", "type": "textarea", "required": True}
                ],
                "created_at": datetime.datetime.utcnow().isoformat() + "Z"
            }
            db["opportunities"].insert(0, new_opp)
            save_db(db)
            add_log("PLATFORM_API", "success", f"Successfully seeded program opportunity: '{new_opp['title']}'", {"id": opp_id})
            
            # Request all websocket clients to force refresh
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(broadcast_stats())
                    loop.create_task(broadcast_to_clients({"type": "FORCE_REFRESH"}))
            except Exception:
                pass
                
            messagebox.showinfo("Seeding Complete", "Successfully registered Academic Research Seminar into the database catalog.")
        except Exception as e:
            messagebox.showerror("Error Seeding", f"Failed to seed demo database opportunity: {e}")

    def clear_database():
        if messagebox.askyesno("Confirm Purge", "Are you absolute sure you want to permanently purge all opportunities, student registrations, candidate profiles, and account credentials? This action is non-reversible."):
            empty = {"opportunities": [], "registrations": [], "users": []}
            save_db(empty)
            global emails_delivered_count
            emails_delivered_count = 0
            system_logs.clear()
            add_log("PLATFORM_API", "warn", "Database fully purged following administrative control board reset request.")
            
            # Broadcast updates
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(broadcast_stats())
                    loop.create_task(broadcast_to_clients({"type": "FORCE_REFRESH"}))
            except Exception:
                pass
                
            messagebox.showinfo("Workspace Purged", "All database tables and logs have been successfully cleared.")

    btn_client = tk.Button(
        actions_frame, 
        text="💻 LAUNCH STUDENT DESKTOP WINDOW", 
        bg="#6366f1", 
        fg="#ffffff", 
        **btn_style,
        command=launch_desktop
    )
    btn_client.pack(fill=tk.X, pady=7)

    btn_org = tk.Button(
        actions_frame, 
        text="🌐 OPEN ORGANIZER / ADMIN PORTAL", 
        bg="#4f46e5", 
        fg="#ffffff", 
        **btn_style,
        command=launch_organizer
    )
    btn_org.pack(fill=tk.X, pady=7)

    btn_seed = tk.Button(
        actions_frame, 
        text="🌱 SEED PROGRAM SYMPOSIUM OPPORTUNITY", 
        bg="#0d9488", 
        fg="#ffffff", 
        **btn_style,
        command=seed_academic
    )
    btn_seed.pack(fill=tk.X, pady=7)

    btn_reset = tk.Button(
        actions_frame, 
        text="🚨 RESET ATOMIC PERSISTENCE LAYER (PURGE)", 
        bg="#dc2626", 
        fg="#ffffff", 
        **btn_style,
        command=clear_database
    )
    btn_reset.pack(fill=tk.X, pady=7)

    log_export_btn = tk.Button(
        actions_frame, 
        text="💾 EXPORT SESSION LOG ARCHIVES (.TXT)", 
        bg="#475569", 
        fg="#ffffff", 
        **btn_style,
        command=lambda: export_logs()
    )
    log_export_btn.pack(fill=tk.X, pady=7)

    def export_logs():
        try:
            filename = f"server_log_export_{int(time.time())}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                for l in system_logs:
                    f.write(f"[{l['timestamp']}] {l['level'].upper()} ({l['source']}): {l['message']}\n")
            messagebox.showinfo("Logs Exported", f"Session transactions log exported to workspace path:\n./{filename}")
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to save log exports to disk: {e}")

    # Column 2: Log Viewer Terminal Console (Right)
    col_right = tk.Frame(body, bg="#0f172a")
    col_right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

    terminal_label = tk.Label(
        col_right, 
        text="LIVE SYSTEM TRANSACTION TELEMETRY", 
        font=("Helvetica", 9, "bold"), 
        bg="#0f172a", 
        fg="#94a3b8",
        anchor="w"
    )
    terminal_label.pack(fill=tk.X, pady=(0, 6))

    terminal_txt = scrolledtext.ScrolledText(
        col_right, 
        bg="#020617", 
        fg="#cbd5e1", 
        font=("Consolas", 9), 
        bd=1, 
        relief="solid",
        highlightbackground="#334155",
        insertbackground="#ffffff"
    )
    terminal_txt.pack(fill=tk.BOTH, expand=True)

    # Configure terminal text coloring tags
    terminal_txt.tag_config("INFO", foreground="#38bdf8")       # Light Blue
    terminal_txt.tag_config("SUCCESS", foreground="#34d399")    # Emerald Green
    terminal_txt.tag_config("WARN", foreground="#fbbf24")       # Amber
    terminal_txt.tag_config("ERROR", foreground="#f87171")      # Scarlet Red
    terminal_txt.tag_config("TIMESTAMP", foreground="#64748b")  # Muted Gray

    # Periodic polling loop
    current_idx = [0]
    
    def refresh_panel():
        # Read from core logs list thread-safely
        all_logs = list(system_logs)
        if len(all_logs) > current_idx[0]:
            terminal_txt.configure(state=tk.NORMAL)
            for i in range(current_idx[0], len(all_logs)):
                item = all_logs[i]
                ts = datetime.datetime.fromisoformat(item["timestamp"].split(".")[0]).strftime("%H:%M:%S")
                # Insert timestamps
                terminal_txt.insert(tk.END, f"[{ts}] ", "TIMESTAMP")
                # Insert colored level tag
                level = item["level"].upper()
                terminal_txt.insert(tk.END, f"{level.ljust(8)} ", level)
                # Insert message
                terminal_txt.insert(tk.END, f"({item['source']}) {item['message']}\n")
            
            terminal_txt.configure(state=tk.DISABLED)
            terminal_txt.see(tk.END)
            current_idx[0] = len(all_logs)

        # Refresh stats dashboard cards
        stats = get_stats()
        active_monitors_var.set(str(stats["connectedWorkers"]))
        total_opps_var.set(str(stats["totalOpportunities"]))
        total_regs_var.set(str(stats["totalRegistrations"]))
        emails_sent_var.set(str(stats["emailsDelivered"]))

        # Schedule next refresh
        root.after(200, refresh_panel)

    _ = threading.Thread(
        target=lambda: uvicorn.run(app, host="0.0.0.0", port=port, log_config=None),
        daemon=True
    ).start()

    # Trigger first refresh
    root.after(100, refresh_panel)
    
    # Handle clean window close
    def on_closing():
        root.destroy()
        os._exit(0)
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    # Run loop
    root.mainloop()

def main():
    global DEBUG_MODE
    parser = argparse.ArgumentParser(description="Digital Pathways FastAPI Server")
    parser.add_argument("--port", type=int, default=8000, help="Server port (Defaults to 8000)")
    parser.add_argument("--debug", action="store_true", help="Enable verbose debug logs")
    parser.add_argument("--headless", action="store_true", help="Force headless CLI/container server mode")
    parser.add_argument("--visual", action="store_true", help="Launch visual pywebview desktop application window")
    args = parser.parse_args()

    # Enable debug mode global state if flagged
    if args.debug:
        DEBUG_MODE = True
        print(f"{ANSI_BOLD}{ANSI_YELLOW}[DEBUG ACTIVE]{ANSI_RESET} Enabled deep diagnostic print streams in terminal outputs.")

    # Dynamic binding for container ingress (defaults to PORT env or args)
    port = int(os.environ.get("PORT", args.port))

    # Pre-seed initial diagnostic logs
    add_log("PLATFORM_API", "info", f"Initializing Digital Pathways 3.0 Platform Python FastAPI engine. Debug mode: {'Active' if DEBUG_MODE else 'Disabled'}")
    db = load_db()
    add_log("PLATFORM_API", "success", f"Atomic JSON store loaded: {len(db['opportunities'])} opportunities, {len(db['registrations'])} registrations, {len(db['users'])} accounts.")

    # Select GUI (PyWebView) or standard CLI based on the --visual flag
    if args.visual:
        try:
            import webview
            import threading
            import time

            print(f"* Starting uvicorn server thread on port {port} for Visual Display...")
            server_thread = threading.Thread(
                target=lambda: uvicorn.run(app, host="0.0.0.0", port=port, log_config=None),
                daemon=True
            )
            server_thread.start()

            # Give uvicorn a brief moment to bind to the port
            time.sleep(1.2)

            api = ClientApi(f"http://localhost:{port}")
            print(f"* Launching native pywebview visual control center pointing to http://localhost:{port}...")
            
            webview.create_window(
                title="Digital Pathways 3.0 — Server Control Center",
                url=f"http://localhost:{port}/?pov=server",
                js_api=api,
                width=1220,
                height=840
            )
            webview.start()
            return
        except ImportError:
            print("[!] Error: 'pywebview' package is not installed. Please run 'pip install pywebview' first.")
            print("Falling back to standard CLI terminal...")
        except Exception as e:
            print(f"[Visual/pywebview Init Failed]: {e}. Falling back to standard CLI terminal...")

    print(f"\n* Launching application engine in CLI mode at http://localhost:{port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
