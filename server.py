#!/usr/bin/env python3
"""
Outreach Dashboard — FastAPI backend
Serves dashboard UI + API for controlling the email pipeline
Run: python3 server.py
Open: http://localhost:5050
"""
import asyncio, csv, json, os, re, subprocess, sys, time, threading
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
import uvicorn
import requests

# ── PATHS ─────────────────────────────────────────────────────────
LEADS_DIR  = Path("/Users/abzal.bolatkhan.01/AI projects/leads")
MASTER_CSV = LEADS_DIR / "emails_generated.csv"
SENT_LOG   = LEADS_DIR / "sent_log.csv"
RAW_CSV    = LEADS_DIR / "leads_raw_new.csv"
PIPELINE   = LEADS_DIR / "daily_outreach.py"
PYTHON     = sys.executable

MASTER_COLS = [
    "Business Name","Category","City","State","To Email",
    "Phone","Website","Rating","Reviews","Subject","Email Body"
]
SENT_LOG_COLS = ["To Email", "Business Name", "Subject", "Sent At", "Status"]

# ── SECRETS ───────────────────────────────────────────────────────
secrets = Path.home() / ".claude/ai-secrets.env"
for line in secrets.read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'\""))

GROQ_KEY   = os.environ.get("GROQ_API_KEY","")
GROQ_KEY2  = os.environ.get("GROQ_API_KEY_2","")
GH_TOKEN   = os.environ.get("GITHUB_TOKEN","")
PORTFOLIO  = "https://aviel-bolatkhan-01.github.io"

# ── BLOCKLIST ─────────────────────────────────────────────────────
BLOCKED_DOMAINS  = ["wixpress.com","squarespace.com","godaddysites.com","weebly.com",
    "mailer-daemon","noreply","no-reply","postmaster","bounce","sentry-next","example.com"]
BLOCKED_PREFIXES = ["info@","info+","hello@","contact@","support@","admin@","office@",
    "first.last","test@","name@","user@","webmaster@","mail@"]
IMAGE_EXTS = {"png","jpg","jpeg","gif","webp","svg","ico","avif","bmp","woff","ttf","mp4","mp3"}

def is_bad_email(email):
    e = (email or "").lower().strip()
    if not e or "@" not in e or len(e) < 6: return True
    if any(d in e for d in BLOCKED_DOMAINS): return True
    if any(e.startswith(p) for p in BLOCKED_PREFIXES): return True
    tld = e.rsplit(".",1)[-1] if "." in e else ""
    if tld in IMAGE_EXTS: return True
    local = e.split("@")[0]
    if re.match(r'^[a-f0-9]{20,}$', local): return True
    if re.match(r'^\d+$', local): return True
    if "2x" in local or "scaled" in local: return True
    return False

# ── DATA HELPERS ──────────────────────────────────────────────────
def read_csv(path: Path):
    if not path.exists(): return []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except: return []

def write_csv(path: Path, rows: list, fieldnames: list):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def read_sent_rows():
    if not SENT_LOG.exists():
        return []
    rows = []
    try:
        with open(SENT_LOG, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            for raw in reader:
                if not raw:
                    continue
                if len(raw) >= 5:
                    row = dict(zip(SENT_LOG_COLS, raw[:5]))
                elif len(raw) == 3:
                    row = {
                        "To Email": raw[0],
                        "Business Name": raw[1],
                        "Subject": "",
                        "Sent At": raw[2],
                        "Status": "sent",
                    }
                else:
                    row = {
                        "To Email": raw[0] if len(raw) > 0 else "",
                        "Business Name": raw[1] if len(raw) > 1 else "",
                        "Subject": raw[2] if len(raw) > 2 else "",
                        "Sent At": raw[3] if len(raw) > 3 else "",
                        "Status": raw[4] if len(raw) > 4 else "",
                    }
                rows.append(row)
    except Exception:
        return []
    return rows

def load_sent_set():
    sent = set()
    for row in read_sent_rows():
        e = (row.get("To Email") or "").strip().lower()
        if e: sent.add(e)
    return sent

# ── PIPELINE STATE ────────────────────────────────────────────────
_pipeline_proc      = None
_pipeline_stage     = "idle"
_pipeline_lock      = threading.Lock()
_continuous_stop    = threading.Event()   # set to break the continuous scrape loop
_continuous_running = False               # True while continuous loop is active
LIVE_LOG        = Path("/tmp/pipeline_live.log")
SAMPLES_FILE    = Path("/tmp/email_samples.json")
APPROVAL_FILE   = LEADS_DIR / ".pending_approval"

# ── ACTIVITY LOG (server-side, survives page refresh) ─────────────
_activity: list = []
def log_activity(msg: str, cls: str = ""):
    _activity.append({"t": datetime.now().strftime("%H:%M:%S"), "msg": msg, "cls": cls})
    if len(_activity) > 100:
        _activity.pop(0)

def pipeline_running():
    global _continuous_running
    if _continuous_running:
        return True
    with _pipeline_lock:
        if _pipeline_proc and _pipeline_proc.poll() is None:
            return True
    # Also detect cron-started pipeline processes
    try:
        import psutil
        for proc in psutil.process_iter(['cmdline']):
            try:
                cmd = ' '.join(proc.info['cmdline'] or [])
                if ('daily_outreach.py' in cmd or 'scrape_maps.py' in cmd or 'extract_emails.py' in cmd) and 'server.py' not in cmd:
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except ImportError:
        try:
            import subprocess as _sp
            r = _sp.run(['pgrep', '-f', 'daily_outreach.py'], capture_output=True)
            if r.returncode == 0:
                return True
            r2 = _sp.run(['pgrep', '-f', 'scrape_maps.py'], capture_output=True)
            if r2.returncode == 0:
                return True
        except Exception:
            pass
    return False

def _stream_proc_to_log(proc, log_path: Path):
    """Background thread: read subprocess stdout line-by-line → write to log file."""
    try:
        with open(log_path, "a", buffering=1, encoding="utf-8") as f:
            for line in iter(proc.stdout.readline, ""):
                f.write(line)
                f.flush()
    except Exception:
        pass

# ── GROQ EMAIL GENERATION ─────────────────────────────────────────
PLAYBOOK = {
    "dental clinic":    ("patients ghost appointment reminders and no-shows drain revenue","an AI that sends automated reminders, books recalls, and reactivates lapsed patients"),
    "law firm":         ("website visitors leave without contacting — intake forms sit empty","an AI that engages visitors, qualifies case type, and books consultations automatically"),
    "med spa":          ("booking and rescheduling burns hours of staff time weekly","an AI receptionist that books, confirms, and reschedules appointments 24/7"),
    "real estate agent":("leads go cold while you're showing other properties","an AI that instantly engages new leads, qualifies buyers, and books showings"),
    "insurance agency": ("agents spend hours following up on quote requests that go cold","an AI that responds to new quote requests instantly and follows up automatically"),
    "hvac contractor":  ("after-hours emergency calls go to voicemail and customers call a competitor","an AI that answers after-hours calls, captures urgency, and dispatches or books next morning"),
    "roofing contractor":("calls come in while you're on a roof — they call the next guy","an AI that answers missed calls, captures job details, and books estimates automatically"),
    "chiropractor":     ("patients drop off before completing care plans","an AI that sends personalized check-ins and re-engages lapsed patients automatically"),
    "physical therapist":("last-minute cancellations leave empty slots","an AI that monitors your waitlist and fills cancellations automatically"),
    "veterinary clinic":("appointment no-shows and wellness reminders going ignored","an AI that sends reminder calls and reschedules missed appointments automatically"),
    "auto repair shop": ("customers approve repairs verbally then never schedule","an AI that follows up on deferred repairs via text and books the job automatically"),
    "plumber":          ("calls come in while under a sink — you miss them, they move on","an AI that answers calls 24/7 and books the next available slot automatically"),
    "mortgage broker":  ("borrowers request quotes then ghost you before you can follow up","an AI that responds to inquiries instantly and pre-qualifies the borrower"),
    "accounting firm":  ("client onboarding is a 2-week email chain of document requests","an AI that automates client intake and follows up on missing documents automatically"),
    "default":          ("manual follow-ups eat into your billable time","an AI that handles repetitive tasks — lead responses, scheduling, follow-ups"),
}

def get_playbook(category: str):
    cat = (category or "").lower().strip()
    if cat in PLAYBOOK: return PLAYBOOK[cat]
    for key, val in PLAYBOOK.items():
        if key in cat or cat in key: return val
    return PLAYBOOK["default"]

def call_groq(prompt: str, temperature=0.9, key=None) -> str:
    k = key or GROQ_KEY or GROQ_KEY2
    if not k: return ""
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {k}", "Content-Type": "application/json"},
            json={"model":"llama-3.3-70b-versatile","messages":[{"role":"user","content":prompt}],
                  "max_tokens":350,"temperature":temperature},
            timeout=20
        )
        return resp.json()["choices"][0]["message"]["content"].strip()
    except: return ""

def generate_variants(business_name: str, category: str, city: str, state: str,
                      website: str, rating: str, reviews: str, n=3) -> list:
    pain, offer = get_playbook(category)
    context_parts = [f"{business_name} is a {category} in {city}, {state}."]
    try:
        if rating and reviews and int(reviews) > 20:
            context_parts.append(f"They have {reviews} Google reviews at {float(rating):.1f} stars.")
    except: pass
    if website:
        domain = re.sub(r'https?://(www\.)?','',website).split('/')[0]
        context_parts.append(f"Their website: {domain}.")
    context = " ".join(context_parts)

    variants = []
    temps = [0.75, 0.90, 1.05]
    keys  = [GROQ_KEY, GROQ_KEY2 or GROQ_KEY, GROQ_KEY]

    for i in range(n):
        prompt = f"""Write a cold email from Aviel (AI automation specialist) to {business_name}, a {category} in {city}, {state}.

Context: {context}
Pain: {pain}
Solution: {offer}
Portfolio: {PORTFOLIO}

Rules:
- Open: "Hey {business_name},"
- 3 short paragraphs, 80-110 words total
- Specific to {category}, not generic
- Mention portfolio link naturally
- Soft CTA: reply to chat
- Sign off: — Aviel
- Output format exactly:
Subject: [subject line]

[email body]"""

        text = call_groq(prompt, temperature=temps[i % len(temps)], key=keys[i % len(keys)])
        if not text: continue

        subject, body = "", text
        lines = text.splitlines()
        for j, line in enumerate(lines):
            if line.lower().startswith("subject:"):
                subject = line[8:].strip()
                body = "\n".join(lines[j+1:]).strip().lstrip("\n")
                break

        if subject and body:
            variants.append({"subject": subject, "body": body, "variant": chr(65+i)})
        time.sleep(0.8)

    return variants

# ── FASTAPI APP ───────────────────────────────────────────────────
app = FastAPI(title="Outreach Dashboard")

# ── API: STATS ────────────────────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    master = read_csv(MASTER_CSV)
    sent   = load_sent_set()
    sent_rows = read_sent_rows()

    total_collected = len(master)
    total_sent      = len(sent)
    pending = sum(1 for r in master
                  if r.get("To Email","").lower() not in sent
                  and r.get("Email Body","").strip()
                  and not is_bad_email(r.get("To Email","")))
    bad = sum(1 for r in master if is_bad_email(r.get("To Email","")))

    # Raw leads waiting for email generation
    raw = read_csv(RAW_CSV)
    raw_pending = sum(1 for r in raw
                      if (r.get("emails","") or "[]") not in ("","[]")
                      and r.get("title",""))
    raw_scraped = len(raw)  # total businesses scraped (before email extraction)

    # Count unique emails sent today (deduped to be consistent with total_sent)
    sent_today_emails = set()
    today = date.today().strftime("%Y-%m-%d")
    for row in sent_rows:
        if (row.get("Sent At","") or "").startswith(today):
            e = (row.get("To Email") or "").strip().lower()
            if e:
                sent_today_emails.add(e)
    sent_today = len(sent_today_emails)

    running = pipeline_running()
    stage = _pipeline_stage
    if running and stage == "idle":
        # Detect cron stage from live log
        try:
            log_lines = LIVE_LOG.read_text(encoding="utf-8") if LIVE_LOG.exists() else ""
            if "scrape_maps" in log_lines or "Scraping" in log_lines or "running total:" in log_lines or "Query:" in log_lines:
                stage = "scraping"
            elif "extract" in log_lines.lower():
                stage = "extracting"
            elif "Generating" in log_lines or "Groq" in log_lines:
                stage = "generating"
            elif "Sending" in log_lines or "smtp" in log_lines.lower():
                stage = "sending"
            else:
                stage = "running"
        except Exception:
            stage = "running"

    return {
        "total_collected": total_collected,
        "total_sent": total_sent,
        "pending": pending,
        "sent_today": sent_today,
        "bad_emails": bad,
        "raw_pending": raw_pending,
        "raw_scraped": raw_scraped,
        "pipeline_running": running,
        "pipeline_stage": stage,
    }

@app.get("/api/all")
def get_all_emails(page: int = 1, limit: int = 100, search: str = ""):
    master = read_csv(MASTER_CSV)
    sent = load_sent_set()

    if search:
        s = search.lower()
        master = [
            r for r in master
            if s in " ".join([
                r.get("Business Name", ""),
                r.get("Category", ""),
                r.get("City", ""),
                r.get("State", ""),
                r.get("To Email", ""),
                r.get("Subject", ""),
            ]).lower()
        ]

    total = len(master)
    start = (page - 1) * limit
    rows = []
    for row in master[start:start + limit]:
        item = dict(row)
        item["Status"] = "sent" if (row.get("To Email", "").strip().lower() in sent) else "queued"
        rows.append(item)

    return {
        "total": total,
        "page": page,
        "pages": max(1, (total + limit - 1) // limit),
        "rows": rows,
    }

# ── API: QUEUE (pending emails) ────────────────────────────────────
@app.get("/api/queue")
def get_queue(page: int = 1, limit: int = 20, search: str = ""):
    master = read_csv(MASTER_CSV)
    sent   = load_sent_set()

    pending = [
        r for r in master
        if r.get("To Email","").lower() not in sent
        and r.get("Email Body","").strip()
        and not is_bad_email(r.get("To Email",""))
    ]

    if search:
        s = search.lower()
        pending = [r for r in pending if s in (r.get("Business Name","") + r.get("Category","") + r.get("City","")).lower()]

    total = len(pending)
    start = (page-1) * limit
    page_rows = pending[start:start+limit]

    return {
        "total": total,
        "page": page,
        "pages": (total + limit - 1) // limit,
        "rows": page_rows
    }

# ── API: SENT HISTORY ──────────────────────────────────────────────
@app.get("/api/sent/dates")
def get_sent_dates():
    """Return list of unique send dates (YYYY-MM-DD), newest first."""
    seen = {}
    for row in read_sent_rows():
        raw = (row.get("Sent At") or "").strip()
        day = raw[:10] if len(raw) >= 10 else None
        if day and re.match(r'^\d{4}-\d{2}-\d{2}$', day):
            seen[day] = seen.get(day, 0) + 1
    dates = sorted(seen.keys(), reverse=True)
    return {"dates": [{"date": d, "count": seen[d]} for d in dates]}

@app.get("/api/sent")
def get_sent(page: int = 1, limit: int = 30, date: str = ""):
    all_rows = list(reversed(read_sent_rows()))
    if date:
        all_rows = [r for r in all_rows
                    if (r.get("Sent At") or "").strip().startswith(date)]
    total = len(all_rows)
    start = (page-1)*limit
    return {
        "total": total,
        "page": page,
        "pages": max(1, (total+limit-1)//limit),
        "rows": all_rows[start:start+limit],
        "date": date,
    }

# ── API: GENERATE VARIANTS ─────────────────────────────────────────
class VariantRequest(BaseModel):
    business_name: str
    category: str
    city: str = ""
    state: str = ""
    website: str = ""
    rating: str = ""
    reviews: str = ""

@app.post("/api/generate-variants")
def api_generate_variants(req: VariantRequest):
    variants = generate_variants(
        req.business_name, req.category, req.city, req.state,
        req.website, req.rating, req.reviews, n=3
    )
    if not variants:
        raise HTTPException(500, "Failed to generate variants — check Groq API key")
    return {"variants": variants}

# ── API: APPROVE EMAIL ─────────────────────────────────────────────
class ApproveRequest(BaseModel):
    email: str
    subject: str
    body: str

@app.post("/api/approve")
def approve_email(req: ApproveRequest):
    master = read_csv(MASTER_CSV)
    updated = False
    for row in master:
        if row.get("To Email","").lower() == req.email.lower():
            row["Subject"]    = req.subject
            row["Email Body"] = req.body
            updated = True
            break
    if updated:
        write_csv(MASTER_CSV, master, MASTER_COLS)
    return {"ok": True, "updated": updated}

# ── API: SKIP EMAIL ────────────────────────────────────────────────
class SkipRequest(BaseModel):
    email: str

@app.post("/api/skip")
def skip_email(req: SkipRequest):
    master = read_csv(MASTER_CSV)
    before = len(master)
    master = [r for r in master if r.get("To Email","").lower() != req.email.lower()]
    write_csv(MASTER_CSV, master, MASTER_COLS)
    return {"ok": True, "removed": before - len(master)}

# ── API: BULK APPROVE (approve current body as-is) ─────────────────
@app.post("/api/bulk-approve")
def bulk_approve():
    # Already approved — emails in master CSV with body are ready to send
    # Just return count of approved
    master = read_csv(MASTER_CSV)
    sent   = load_sent_set()
    count  = sum(1 for r in master
                 if r.get("Email Body","").strip()
                 and r.get("To Email","").lower() not in sent
                 and not is_bad_email(r.get("To Email","")))
    return {"ok": True, "approved": count}

# ── API: PIPELINE CONTROL ──────────────────────────────────────────
@app.post("/api/pipeline/{action}")
def pipeline_action(action: str, background_tasks: BackgroundTasks):
    global _pipeline_proc, _pipeline_stage

    if action == "scrape":
        if pipeline_running():
            return {"ok": False, "msg": "Pipeline already running"}

        def run_scrape():
            global _pipeline_proc, _pipeline_stage, _continuous_running
            import random as _random, json as _json, csv as _csv

            CITIES = [
                ("Charlotte","NC"),("Atlanta","GA"),("Tampa","FL"),("Las Vegas","NV"),
                ("Portland","OR"),("Minneapolis","MN"),("San Diego","CA"),("Detroit","MI"),
                ("Baltimore","MD"),("Raleigh","NC"),("Phoenix","AZ"),("Sacramento","CA"),
                ("Kansas City","MO"),("Columbus","OH"),("Indianapolis","IN"),
                ("Louisville","KY"),("Memphis","TN"),("Richmond","VA"),
                ("Oklahoma City","OK"),("Salt Lake City","UT"),
                ("Nashville","TN"),("Denver","CO"),("Austin","TX"),("Seattle","WA"),
                ("Miami","FL"),("Dallas","TX"),("Houston","TX"),("Chicago","IL"),
                ("Boston","MA"),("Pittsburgh","PA"),("Cincinnati","OH"),("St. Louis","MO"),
            ]
            CATEGORIES = [
                "dental clinic","law firm","med spa","real estate agent",
                "insurance agency","HVAC contractor","roofing contractor",
                "chiropractor","physical therapist","veterinary clinic",
                "auto repair shop","plumber","mortgage broker","accounting firm",
            ]

            _continuous_stop.clear()
            _continuous_running = True
            LIVE_LOG.write_text("")
            iteration = 0

            def _llog(msg: str):
                with open(LIVE_LOG, "a", encoding="utf-8") as _f:
                    _f.write(msg + "\n")

            def _run_proc(cmd, timeout=None):
                """Run subprocess, stream output to LIVE_LOG, return proc."""
                proc = subprocess.Popen(
                    cmd, cwd=str(LEADS_DIR),
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1
                )
                with _pipeline_lock:
                    globals()['_pipeline_proc'] = proc
                t = threading.Thread(target=_stream_proc_to_log, args=(proc, LIVE_LOG), daemon=True)
                t.start()
                try:
                    proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    _llog("⚠️ Process hit timeout — killed, continuing with partial results")
                return proc

            try:
                while not _continuous_stop.is_set():
                    iteration += 1
                    log_activity(f"Continuous scrape — iteration {iteration}", "run")
                    _llog(f"\n{'='*50}\n🔁 Iteration {iteration} — {datetime.now().strftime('%H:%M:%S')}\n{'='*50}")

                    # ── Step 1: SCRAPE ───────────────────────────────
                    _pipeline_stage = "scraping"
                    pairs = [(city, state, cat) for city, state in CITIES for cat in CATEGORIES]
                    _random.shuffle(pairs)
                    queries = [f"{cat} in {city} {state}" for city, state, cat in pairs[:25]]
                    queries_path = LEADS_DIR / "daily_queries.txt"
                    queries_path.write_text("\n".join(queries))

                    raw_csv = LEADS_DIR / "leads_raw_new.csv"
                    if raw_csv.exists():
                        raw_csv.unlink()

                    _llog(f"🔍 Scraping {len(queries)} queries...")
                    _run_proc([PYTHON, str(LEADS_DIR / "scrape_maps.py"),
                               "--queries", str(queries_path), "--output", str(raw_csv)],
                              timeout=3600)

                    if _continuous_stop.is_set():
                        break

                    raw_count = 0
                    if raw_csv.exists():
                        try:
                            with open(raw_csv) as f:
                                raw_count = max(0, sum(1 for _ in f) - 1)
                        except: pass
                    _llog(f"✅ Scrape done — {raw_count} businesses found")

                    # ── Step 2: EXTRACT EMAILS ───────────────────────
                    _pipeline_stage = "extracting"
                    _llog("📧 Extracting emails from business websites...")
                    _run_proc([PYTHON, str(LEADS_DIR / "extract_emails.py")], timeout=1800)

                    if _continuous_stop.is_set():
                        break

                    # Count businesses with emails found
                    email_count = 0
                    if raw_csv.exists():
                        try:
                            with open(raw_csv, newline="", encoding="utf-8") as f:
                                for row in _csv.DictReader(f):
                                    e = row.get("emails","")
                                    if e and e != "[]": email_count += 1
                        except: pass
                    _llog(f"✅ Extraction done — {email_count} businesses have emails")

                    # ── Step 3: GENERATE AI EMAILS ───────────────────
                    _pipeline_stage = "generating"
                    _llog("✍️ Generating personalized emails (Groq)...")
                    _run_proc([PYTHON, str(LEADS_DIR / "daily_outreach.py"), "--generate-only"],
                              timeout=1800)

                    if _continuous_stop.is_set():
                        break

                    # Count new records in master CSV
                    try:
                        master_count = sum(1 for _ in open(MASTER_CSV)) - 1
                    except: master_count = 0
                    _llog(f"✅ Iteration {iteration} complete — {master_count} total emails queued")
                    log_activity(f"Iteration {iteration} done — {master_count} queued", "ok")

            finally:
                _continuous_running = False
                _pipeline_stage = "idle"
                with _pipeline_lock:
                    globals()['_pipeline_proc'] = None
                _llog(f"\n⏹ Continuous scrape stopped after {iteration} iteration(s)")
                log_activity("Continuous scrape stopped", "info")

        background_tasks.add_task(run_scrape)
        return {"ok": True, "msg": "Continuous scrape started — runs until you press Stop"}

    if action == "send":
        if pipeline_running():
            return {"ok": False, "msg": "Pipeline already running"}
        def run_send():
            global _pipeline_proc, _pipeline_stage
            _pipeline_stage = "sending"
            log_activity("Pipeline started — sending", "run")
            LIVE_LOG.write_text("")
            send_script = str(LEADS_DIR / "send_emails.py")
            _pipeline_proc = subprocess.Popen(
                [PYTHON, send_script], cwd=str(LEADS_DIR),
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1
            )
            t = threading.Thread(target=_stream_proc_to_log, args=(_pipeline_proc, LIVE_LOG), daemon=True)
            t.start()
            _pipeline_proc.wait()
            log_activity("Send complete", "ok")
            _pipeline_stage = "idle"
        background_tasks.add_task(run_send)
        return {"ok": True, "msg": "Send started"}

    if action == "stop":
        global _continuous_running
        stopped_something = False
        # Signal continuous loop to stop
        _continuous_stop.set()
        if _continuous_running:
            stopped_something = True
        # Kill any running subprocess
        with _pipeline_lock:
            if _pipeline_proc and _pipeline_proc.poll() is None:
                _pipeline_proc.terminate()
                stopped_something = True
        if stopped_something:
            log_activity("Pipeline stopped by user", "err")
            _pipeline_stage = "idle"
            return {"ok": True, "msg": "Pipeline stopped"}
        return {"ok": False, "msg": "Nothing running"}

    raise HTTPException(400, f"Unknown action: {action}")

@app.get("/api/pipeline/status")
def pipeline_status():
    return {
        "running": pipeline_running(),
        "stage": _pipeline_stage,
    }

# ── API: SEND HISTORY (daily breakdown) ──────────────────────────
@app.get("/api/stats/history")
def get_history():
    rows = read_sent_rows()
    from collections import defaultdict
    import re as _re
    by_day = defaultdict(int)
    date_pat = _re.compile(r'^\d{4}-\d{2}-\d{2}')
    for r in rows:
        day = (r.get("Sent At") or "")[:10]
        if day and date_pat.match(day): by_day[day] += 1
    # Last 30 days sorted
    sorted_days = sorted(by_day.items())[-30:]
    return {"history": [{"date": d, "count": c} for d, c in sorted_days]}

# ── API: SAMPLE EMAILS ─────────────────────────────────────────────
@app.get("/api/samples")
def get_samples(n: int = 5):
    # Return format-specific samples if pipeline generated them
    if SAMPLES_FILE.exists():
        try:
            data = json.loads(SAMPLES_FILE.read_text())
            if isinstance(data, dict) and data:
                return {"format_samples": data, "samples": [], "total_pending": 0}
        except:
            pass
    # Fallback: random emails from existing queue
    master = read_csv(MASTER_CSV)
    sent   = load_sent_set()
    pool   = [r for r in master
              if r.get("To Email","").lower() not in sent
              and r.get("Email Body","").strip()
              and not is_bad_email(r.get("To Email",""))]
    import random as _random
    samples = _random.sample(pool, min(n, len(pool))) if pool else []
    return {"format_samples": None, "samples": samples, "total_pending": len(pool)}

# ── API: APPROVE BATCH (write approval token, pipeline picks it up) ─
class ApproveBatchRequest(BaseModel):
    format_id: str = "default"

@app.post("/api/approve-batch")
def approve_batch(req: ApproveBatchRequest, background_tasks: BackgroundTasks):
    format_id = req.format_id or "professional"
    APPROVAL_FILE.write_text(format_id)
    if not pipeline_running():
        def run_generate_and_send():
            global _pipeline_proc, _pipeline_stage
            LIVE_LOG.unlink(missing_ok=True)
            _pipeline_stage = "generating"
            _pipeline_proc = subprocess.Popen(
                [PYTHON, str(LEADS_DIR / "daily_outreach.py"),
                 "--generate-and-send", format_id],
                cwd=str(LEADS_DIR),
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, bufsize=1
            )
            t = threading.Thread(target=_stream_proc_to_log,
                                 args=(_pipeline_proc, LIVE_LOG), daemon=True)
            t.start()
            _pipeline_proc.wait()
            _pipeline_stage = "idle"
            if APPROVAL_FILE.exists(): APPROVAL_FILE.unlink()
            if SAMPLES_FILE.exists(): SAMPLES_FILE.unlink()  # clear samples after send
        background_tasks.add_task(run_generate_and_send)
        return {"ok": True, "msg": f"Approved — generating {format_id} emails and sending"}
    return {"ok": True, "msg": "Approval saved — pipeline will pick it up"}

# ── API: APPROVAL STATUS ───────────────────────────────────────────
@app.get("/api/approval/status")
def approval_status():
    master = read_csv(MASTER_CSV)
    sent   = load_sent_set()
    pending = sum(1 for r in master
                  if r.get("To Email","").lower() not in sent
                  and r.get("Email Body","").strip()
                  and not is_bad_email(r.get("To Email","")))
    return {
        "needs_approval": not APPROVAL_FILE.exists() and pending > 0,
        "pending_count": pending,
        "approved": APPROVAL_FILE.exists(),
    }

# ── API: LIVE LOG STREAM (SSE) ────────────────────────────────────
@app.get("/api/stream")
async def stream_logs():
    """Server-Sent Events: tail pipeline_live.log and push new lines."""
    async def generator():
        pos = 0
        idle_ticks = 0
        max_idle = 120  # 60s with 0.5s sleep
        yield f"data: {json.dumps({'line': '--- Stream connected ---', 'type': 'info'})}\n\n"
        while True:
            await asyncio.sleep(0.5)
            got_data = False
            if LIVE_LOG.exists():
                try:
                    with open(LIVE_LOG, "r", encoding="utf-8") as f:
                        f.seek(pos)
                        new_data = f.read()
                        pos = f.tell()
                    if new_data:
                        got_data = True
                        idle_ticks = 0
                        for line in new_data.splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            ltype = "info"
                            if "error" in line.lower() or "❌" in line:
                                ltype = "err"
                            elif "✅" in line or "done" in line.lower():
                                ltype = "ok"
                            elif "found:" in line or "total:" in line:
                                ltype = "progress"
                            yield f"data: {json.dumps({'line': line, 'type': ltype})}\n\n"
                    else:
                        idle_ticks += 1
                except Exception:
                    pass
            else:
                idle_ticks += 1

            # When the pipeline has stopped and there's no new data, count down faster
            # but only increment once total per tick (not twice) to preserve the ~60s window
            if not got_data and not pipeline_running():
                idle_ticks += 1

            if idle_ticks >= max_idle:
                yield f"data: {json.dumps({'line': '--- Stream ended ---', 'type': 'info', 'done': True})}\n\n"
                break

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )

@app.get("/api/logs")
def get_logs(lines: int = 200):
    """Return last N lines of the live log."""
    if not LIVE_LOG.exists():
        return {"lines": []}
    try:
        all_lines = LIVE_LOG.read_text(encoding="utf-8").splitlines()
        return {"lines": all_lines[-lines:]}
    except Exception:
        return {"lines": []}

# ── API: GITHUB SYNC ───────────────────────────────────────────────
@app.post("/api/sync")
def sync_github():
    msgs = []
    errors = []

    # 1. Sync leads data → private outreach-data repo
    try:
        subprocess.run(["git", "add", "emails_generated.csv", "sent_log.csv", "replies_log.csv"],
                       cwd=str(LEADS_DIR), capture_output=True)
        r = subprocess.run(["git", "commit", "-m", f"data: sync {date.today()}"],
                           cwd=str(LEADS_DIR), capture_output=True, text=True)
        if "nothing to commit" in r.stdout + r.stderr:
            msgs.append("Data: no changes")
        else:
            r2 = subprocess.run(["git", "push", "origin", "main"],
                                 cwd=str(LEADS_DIR), capture_output=True, text=True)
            if r2.returncode == 0:
                msgs.append("Data pushed → outreach-data (private)")
            else:
                errors.append(f"Data push: {r2.stderr.strip()[:120]}")
    except Exception as e:
        errors.append(f"Data sync: {e}")

    # 2. Sync dashboard code → outreach-dashboard repo
    dashboard_dir = Path(__file__).parent
    try:
        subprocess.run(["git", "add", "-A"], cwd=str(dashboard_dir), capture_output=True)
        r = subprocess.run(["git", "commit", "-m", f"dashboard: sync {date.today()}"],
                           cwd=str(dashboard_dir), capture_output=True, text=True)
        if "nothing to commit" in r.stdout + r.stderr:
            msgs.append("Dashboard: no changes")
        else:
            r2 = subprocess.run(["git", "push", "origin", "main"],
                                 cwd=str(dashboard_dir), capture_output=True, text=True)
            if r2.returncode == 0:
                msgs.append("Dashboard pushed → outreach-dashboard")
            else:
                errors.append(f"Dashboard push: {r2.stderr.strip()[:120]}")
    except Exception as e:
        errors.append(f"Dashboard sync: {e}")

    if errors:
        return {"ok": False, "msg": " | ".join(errors)}
    return {"ok": True, "msg": " | ".join(msgs) or "All synced"}

# ── SERVE FRONTEND ────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/index.html")

# ── RUN ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Outreach Dashboard running at http://localhost:5050")
    uvicorn.run(app, host="0.0.0.0", port=5050, reload=False)
