"""
server.py — Airbnb Revenue Engine (multi-user)
===============================================
Each browser session gets its own OUT_DIR = BASE_DIR/strategy/{session_id}/
Files from different users never touch each other.

    pip install flask flask-cors
    python server.py  →  http://localhost:5000
"""

import os, re, sys, json, time, uuid, shutil, queue, threading, logging
from pathlib import Path
from datetime import datetime, timedelta

from flask import Flask, request, jsonify, send_file, Response, stream_with_context
from flask_cors import CORS

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

app      = Flask(__name__)
CORS(app, expose_headers=["X-Session-Id"])

BASE_DIR     = Path(__file__).parent
STRATEGY_DIR = BASE_DIR / "strategy"          # parent of all session dirs
STRATEGY_DIR.mkdir(parents=True, exist_ok=True)

SESSION_TTL_HOURS = 24
OUTPUT_FILES = [
    "strategy_full.json", "comps.csv",
    "price_calendar.csv", "strategy_report.txt", "claude_prompt.txt",
]

# ── Shared Airbnb API keys (not user-specific) ───────────────────
_KEY_STORE = {
    "api_key":      "d306zoyjsyarp7ifhu67rjxn52tv0t20",
    "hash":         "48738417b534264d0467b107152dbe756ecb4b548418b180b2b431556245c28f",
    "last_updated": None,
    "source":       "default",
}

# ── Job registry (job_id is unique globally) ─────────────────────
_jobs:       dict = {}
_log_queues: dict = {}


# ══════════════════════════════════════════════════════════════════
# Session helpers
# ══════════════════════════════════════════════════════════════════

def _new_session_id() -> str:
    return uuid.uuid4().hex[:16]


def _session_dir(session_id: str) -> Path:
    """Each session gets its own isolated directory."""
    # Validate: only hex chars, length 16
    if not re.fullmatch(r"[0-9a-f]{16}", session_id):
        raise ValueError(f"Invalid session_id: {session_id!r}")
    p = STRATEGY_DIR / session_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _get_session_id() -> str:
    """
    Read session id from:
      1. X-Session-Id request header   (fetch calls)
      2. ?sid= query parameter         (window.open / direct URL navigation)
    Generates a new id if neither is valid.
    """
    sid = (request.headers.get("X-Session-Id", "")
           or request.args.get("sid", ""))
    if not re.fullmatch(r"[0-9a-f]{16}", sid):
        sid = _new_session_id()
    return sid


def _add_session_header(response, session_id: str):
    """Attach session id to every response so the frontend can store it."""
    response.headers["X-Session-Id"] = session_id
    return response


# ── Startup: delete sessions older than SESSION_TTL_HOURS ────────
def _gc_old_sessions():
    cutoff = datetime.now() - timedelta(hours=SESSION_TTL_HOURS)
    removed = []
    for d in STRATEGY_DIR.iterdir():
        if d.is_dir() and re.fullmatch(r"[0-9a-f]{16}", d.name):
            try:
                mtime = datetime.fromtimestamp(d.stat().st_mtime)
                if mtime < cutoff:
                    shutil.rmtree(d, ignore_errors=True)
                    removed.append(d.name)
            except Exception:
                pass
    if removed:
        log.info(f"GC: removed {len(removed)} old session(s): {removed}")
    log.info(f"Session GC complete. Active sessions: "
             f"{sum(1 for d in STRATEGY_DIR.iterdir() if d.is_dir())}")

_gc_old_sessions()


# ══════════════════════════════════════════════════════════════════
# Per-session file helpers
# ══════════════════════════════════════════════════════════════════

def _session_file(session_id: str, filename: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9_.\-]", "", filename)
    return _session_dir(session_id) / safe


def _cleanup_session(session_id: str):
    """Delete all output files in this session's directory."""
    d = _session_dir(session_id)
    removed = []
    for fname in OUTPUT_FILES:
        p = d / fname
        if p.exists():
            p.unlink()
            removed.append(fname)
    if removed:
        log.info(f"[{session_id}] Cleaned: {removed}")


def _list_files(session_id: str) -> dict:
    result = {}
    for fname in OUTPUT_FILES:
        p = _session_dir(session_id) / fname
        if p.exists():
            s = p.stat()
            result[fname] = {
                "size":     s.st_size,
                "modified": datetime.fromtimestamp(s.st_mtime).strftime("%H:%M:%S"),
            }
    return result


# ══════════════════════════════════════════════════════════════════
# Pipeline loader
# ══════════════════════════════════════════════════════════════════

def _load_pipeline():
    sys.path.insert(0, str(BASE_DIR))
    mod = "airbnb_strategy_merged_final"
    if mod in sys.modules:
        del sys.modules[mod]
    import importlib
    pipe = importlib.import_module(mod)
    # Inject current shared keys
    pipe.AIRBNB_API_KEY    = _KEY_STORE["api_key"]
    pipe.STAYS_SEARCH_HASH = _KEY_STORE["hash"]
    pipe.GRAPHQL_URL = f"https://www.airbnb.com/api/v3/StaysSearch/{_KEY_STORE['hash']}"
    pipe.GRAPHQL_HEADERS["X-Airbnb-API-Key"] = _KEY_STORE["api_key"]
    return pipe


def _make_log_handler(job_id: str):
    q   = _log_queues[job_id]
    job = _jobs[job_id]

    class H(logging.Handler):
        def emit(self, r):
            msg = self.format(r)
            job["logs"].append(msg)
            q.put({"type": "log", "data": msg})

    h = H()
    h.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
    return h


def _build_summary(result: dict, session_id: str) -> dict:
    p   = result.get("pricing", {})
    l   = result.get("listing", {})
    ai  = result.get("ai_strategy", {})

    # Hàm hỗ trợ ép kiểu an toàn: 
    # Nếu là object phức tạp (như CompTier), nó sẽ lấy giá trị chuỗi hoặc tên của object đó
    def to_safe_val(val):
        if val is None: return None
        if isinstance(val, (int, float, bool, str)): return val
        # Nếu là Enum hoặc Object có thuộc tính .value hoặc .name
        if hasattr(val, "value"): return val.value
        if hasattr(val, "name"): return val.name
        return str(val)

    return _make_json_safe({
    "comp_count":    to_safe_val(p.get("comp_count", 0)),
    "direct_count":  to_safe_val(p.get("direct_count", 0)),
    "median":        to_safe_val(p.get("median", 0)),
    "p25":           to_safe_val(p.get("p25", 0)),
    "p75":           to_safe_val(p.get("p75", 0)),
    "p10":           to_safe_val(p.get("p10", 0)),
    "p90":           to_safe_val(p.get("p90", 0)),
    "suggested_base": to_safe_val(p.get("suggested_base", 0)),
    "currency":      str(p.get("currency", "USD")),
    "tier_position": to_safe_val(p.get("tier_position", "")),
    "confidence":    str(result.get("confidence", "?")),

    "listing_name":  str(l.get("name", "")),
    "listing_url":   str(l.get("url", "")),
    "listing_rating":   to_safe_val(l.get("rating", 0)),
    "listing_reviews":  to_safe_val(l.get("review_count", 0)),
    "listing_bedrooms": to_safe_val(l.get("bedrooms", 0)),
    "listing_beds":     to_safe_val(l.get("beds", 0)),
    "listing_baths":    to_safe_val(l.get("bathrooms", 0)),
    "listing_guests":   to_safe_val(l.get("max_guests", 0)),
    "listing_city":     str(l.get("city", "")),
    "listing_state":    str(l.get("state", "")),
    "listing_lat":      to_safe_val(l.get("latitude", 0)),
    "listing_lng":      to_safe_val(l.get("longitude", 0)),
    "listing_type":     str(l.get("room_type", "")),
    "listing_superhost": bool(l.get("is_superhost", False)),
    "listing_badge":    str(l.get("badge", "")),

    "your_rate":        to_safe_val(l.get("nightly_rate", 0)),

    # 🔥 những chỗ dễ crash nhất
    "monthly_plans":    result.get("monthly_plans", []),
    "date_samples":     result.get("date_samples", []),
    "date_summary":     result.get("date_summary", {}),
    "comps":            result.get("comps", [])[:30],
    "ai_strategy":      ai,
    "sample_days":      result.get("sample_days", []),

    "files":            _list_files(session_id),
    "session_id":       str(session_id),
})

def _make_json_safe(obj):
    """Recursively convert any object into JSON-serializable form."""
    if obj is None:
        return None

    # Primitive types
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # List / tuple
    if isinstance(obj, (list, tuple)):
        return [_make_json_safe(x) for x in obj]

    # Dict
    if isinstance(obj, dict):
        return {str(k): _make_json_safe(v) for k, v in obj.items()}

    # Datetime
    if isinstance(obj, (datetime,)):
        return obj.isoformat()

    # Has __dict__ (class instance như CompTier)
    if hasattr(obj, "__dict__"):
        return _make_json_safe(vars(obj))

    # Has to_dict()
    if hasattr(obj, "to_dict"):
        return _make_json_safe(obj.to_dict())

    # Enum
    if hasattr(obj, "value"):
        return obj.value

    # Fallback
    return str(obj)
# ══════════════════════════════════════════════════════════════════
# API: /api/session  — issue or validate a session
# ══════════════════════════════════════════════════════════════════

@app.route("/api/session", methods=["GET", "POST"])
def session_endpoint():
    sid = _get_session_id()
    resp = jsonify({"session_id": sid, "files": _list_files(sid)})
    return _add_session_header(resp, sid)


# ══════════════════════════════════════════════════════════════════
# API: /api/get-info
# ══════════════════════════════════════════════════════════════════

@app.route("/api/get-info", methods=["POST"])
def get_info():
    sid  = _get_session_id()
    body = request.get_json(force=True)
    url  = (body.get("url") or "").strip()
    if not re.search(r"airbnb\.com/rooms/\d+", url):
        return _add_session_header(jsonify({"error": "Invalid Airbnb listing URL"}), sid), 400

    try:
        pipe    = _load_pipeline()
        req_obj = pipe.AnalysisRequest(listing_url=url, currency="USD")
        l = pipe.stage_b_extract_listing_patched(req_obj, pipe.ListingProfile, pipe._dig)
        resp = jsonify({"success": True, "listing": {
            "listing_id":   l.listing_id, "name": l.name, "url": l.url,
            "room_type":    l.room_type, "bedrooms": l.bedrooms,
            "beds":         l.beds, "bathrooms": l.bathrooms,
            "max_guests":   l.max_guests, "latitude": l.latitude,
            "longitude":    l.longitude, "city": l.city, "state": l.state,
            "rating":       l.rating, "review_count": l.review_count,
            "is_superhost": l.is_superhost, "badge": l.badge,
            "extracted_via":l.extracted_via,
        }})
    except Exception as e:
        log.error(f"get-info [{sid}]: {e}")
        resp = jsonify({"error": str(e)})
        resp.status_code = 500

    return _add_session_header(resp, sid)


# ══════════════════════════════════════════════════════════════════
# API: /api/reload-keys  (shared — any user can trigger)
# ══════════════════════════════════════════════════════════════════

@app.route("/api/reload-keys", methods=["POST"])
def reload_keys():
    sid   = _get_session_id()
    found = {}
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                locale="en-US", timezone_id="Asia/Ho_Chi_Minh",
            )
            page = ctx.new_page()

            def on_req(r):
                if "api/v3/StaysSearch/" in r.url and not found.get("hash"):
                    m = re.search(r"/StaysSearch/([a-f0-9]{40,})", r.url)
                    if m: found["hash"] = m.group(1)
                    k = r.headers.get("x-airbnb-api-key") or r.headers.get("X-Airbnb-API-Key")
                    if k: found["api_key"] = k

            page.on("request", on_req)
            page.goto("https://www.airbnb.com/s/Da-Nang/homes"
                      "?search_type=user_map_move&search_by_map=true",
                      timeout=60_000, wait_until="domcontentloaded")
            page.wait_for_timeout(10_000)
            browser.close()

        if not found.get("api_key") or not found.get("hash"):
            resp = jsonify({"error": "Could not intercept keys — try again", "found": found})
            resp.status_code = 502
            return _add_session_header(resp, sid)

        _KEY_STORE.update({
            "api_key":      found["api_key"],
            "hash":         found["hash"],
            "last_updated": datetime.now().isoformat(),
            "source":       "live",
        })
        log.info(f"Keys refreshed by [{sid}]: {found['api_key'][:12]}…")
        resp = jsonify({"success": True, "api_key": found["api_key"],
                        "hash": found["hash"], "updated_at": _KEY_STORE["last_updated"]})
    except ImportError:
        resp = jsonify({"error": "Playwright not installed"})
        resp.status_code = 500
    except Exception as e:
        log.error(f"reload-keys [{sid}]: {e}")
        resp = jsonify({"error": str(e)})
        resp.status_code = 500

    return _add_session_header(resp, sid)


# ══════════════════════════════════════════════════════════════════
# API: /api/run-analysis  (per-session job)
# ══════════════════════════════════════════════════════════════════

def _run_job(job_id: str, params: dict, session_id: str):
    q   = _log_queues[job_id]
    job = _jobs[job_id]
    job["status"] = "running"

    out_dir = _session_dir(session_id)
    rl = logging.getLogger()
    h  = _make_log_handler(job_id)
    rl.addHandler(h)

    try:
        pipe = _load_pipeline()
        tm   = params.get("target_months")
        if isinstance(tm, str) and tm.strip():
            tm = [int(x.strip()) for x in tm.split(",") if x.strip().isdigit()]
        elif not tm:
            tm = None

        result = pipe.run_pipeline_patched(
            listing_url   = params["url"],
            radius_km     = float(params.get("radius_km", 3.0)),
            similarity    = params.get("similarity", "medium"),
            target_months = tm,
            target_year   = int(params.get("target_year") or 0),
            currency      = params.get("currency", "USD"),
            goal          = params.get("goal", "balanced"),
            your_rate     = float(params.get("your_rate") or 0),
            max_pages     = int(params.get("max_pages") or 5),
            target_comps  = int(params.get("target_comps") or 60),
            out_dir       = out_dir,
        )
        job["status"]     = "done"
        job["result"]     = result
        job["summary"]    = _build_summary(result, session_id)
        q.put({"type": "done", "data": "Analysis complete ✓"})

    except Exception as e:
        log.error(f"Job {job_id} [{session_id}]: {e}", exc_info=True)
        job["status"] = "error"
        job["error"]  = str(e)
        q.put({"type": "error", "data": str(e)})
    finally:
        rl.removeHandler(h)
        q.put(None)


@app.route("/api/run-analysis", methods=["POST"])
def run_analysis():
    sid  = _get_session_id()
    body = request.get_json(force=True)
    url  = (body.get("url") or "").strip()
    if not re.search(r"airbnb\.com/rooms/\d+", url):
        resp = jsonify({"error": "Invalid Airbnb listing URL"})
        resp.status_code = 400
        return _add_session_header(resp, sid)

    # Wipe THIS SESSION's old files only
    _cleanup_session(sid)

    job_id = str(uuid.uuid4())[:8]
    _log_queues[job_id] = queue.Queue()
    _jobs[job_id] = {
        "status": "queued", "logs": [],
        "result": None, "summary": None, "error": None,
        "session_id": sid,
    }

    threading.Thread(
        target=_run_job, args=(job_id, body, sid), daemon=True
    ).start()

    resp = jsonify({"job_id": job_id, "session_id": sid})
    return _add_session_header(resp, sid)


# ══════════════════════════════════════════════════════════════════
# API: /api/stream/<job_id>  &  /api/job/<job_id>
# ══════════════════════════════════════════════════════════════════

@app.route("/api/stream/<job_id>")
def stream_job(job_id: str):
    if job_id not in _log_queues:
        return jsonify({"error": "Unknown job"}), 404

    def gen():
        q = _log_queues[job_id]
        while True:
            try:
                msg = q.get(timeout=60)
                if msg is None:
                    yield 'data: {"type":"end"}\n\n'
                    break
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
            except queue.Empty:
                yield 'data: {"type":"ping"}\n\n'

    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/job/<job_id>")
def job_status(job_id: str):
    if job_id not in _jobs:
        return jsonify({"error": "Unknown job"}), 404
    
    job = _jobs[job_id]

    safe_response = _make_json_safe({
        "status":  job.get("status", "unknown"),
        "logs":    job.get("logs", [])[-100:],
        "error":   job.get("error"),
        "summary": job.get("summary"),
    })

    return jsonify(safe_response)


# ══════════════════════════════════════════════════════════════════
# API: file endpoints  (all scoped to session)
# ══════════════════════════════════════════════════════════════════

@app.route("/api/files")
def list_files():
    sid  = _get_session_id()
    resp = jsonify(_list_files(sid))
    return _add_session_header(resp, sid)


@app.route("/api/file-content/<filename>")
def file_content(filename: str):
    sid  = _get_session_id()
    p    = _session_file(sid, filename)
    if not p.exists():
        resp = jsonify({"error": "File not found"})
        resp.status_code = 404
        return _add_session_header(resp, sid)

    text = p.read_text(encoding="utf-8")
    if p.suffix == ".json":
        resp = jsonify({"content": json.loads(text), "type": "json"})
    else:
        resp = jsonify({"content": text, "type": "text"})
    return _add_session_header(resp, sid)


@app.route("/api/download/<filename>")
def download_file(filename: str):
    sid  = _get_session_id()
    p    = _session_file(sid, filename)
    if not p.exists():
        return jsonify({"error": "File not found"}), 404

    mime = {".csv": "text/csv", ".json": "application/json",
            ".txt": "text/plain"}.get(p.suffix, "application/octet-stream")
    return send_file(str(p), mimetype=mime, as_attachment=True,
                     download_name=p.name)


@app.route("/api/cleanup", methods=["POST"])
def cleanup():
    sid = _get_session_id()
    _cleanup_session(sid)
    resp = jsonify({"success": True, "session_id": sid})
    return _add_session_header(resp, sid)


# ══════════════════════════════════════════════════════════════════
# API: /api/keys  (global status)
# ══════════════════════════════════════════════════════════════════

@app.route("/api/update-keys", methods=["POST"])
def update_keys_manual():
    """Manual fallback: user pastes api_key and hash directly."""
    sid  = _get_session_id()
    body = request.get_json(force=True)
    api_key = (body.get("api_key") or "").strip()
    hash_   = (body.get("hash") or "").strip()
    if not re.fullmatch(r"[a-z0-9]{20,}", api_key):
        resp = jsonify({"error": "Invalid API key format"}); resp.status_code=400
        return _add_session_header(resp, sid)
    if not re.fullmatch(r"[a-f0-9]{40,}", hash_):
        resp = jsonify({"error": "Invalid hash format (must be 40+ hex chars)"}); resp.status_code=400
        return _add_session_header(resp, sid)
    _KEY_STORE.update({"api_key": api_key, "hash": hash_,
                       "last_updated": datetime.now().isoformat(), "source": "manual"})
    _load_pipeline()  # reload with new keys
    log.info(f"Keys updated manually by [{sid}]: {api_key[:12]}…")
    resp = jsonify({"success": True, "api_key": api_key, "hash": hash_,
                    "updated_at": _KEY_STORE["last_updated"]})
    return _add_session_header(resp, sid)


@app.route("/api/keys")
def key_status():
    sid = _get_session_id()
    resp = jsonify({
        "api_key_preview": _KEY_STORE["api_key"][:12] + "…",
        "hash_preview":    _KEY_STORE["hash"][:16] + "…",
        "last_updated":    _KEY_STORE["last_updated"],
        "source":          _KEY_STORE["source"],
    })
    return _add_session_header(resp, sid)


# ══════════════════════════════════════════════════════════════════
# Serve frontend
# ══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    p = BASE_DIR / "index.html"
    if p.exists():
        return p.read_text(encoding="utf-8"), 200, \
               {"Content-Type": "text/html; charset=utf-8"}
    return "<h1>Place index.html next to server.py</h1>", 404


# ══════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\n{'═'*55}")
    print("  Airbnb Revenue Engine — Multi-user server")
    print(f"{'═'*55}")
    print(f"  Sessions : {STRATEGY_DIR}")
    print(f"  Session TTL : {SESSION_TTL_HOURS}h")
    print(f"  URL     : http://localhost:5000\n")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
