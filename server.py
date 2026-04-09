#!/usr/bin/env python3
"""
Outreach Dashboard — FastAPI backend
Serves dashboard UI + API for controlling the email pipeline
Run: python3 server.py
Open: http://localhost:5050
"""
import csv, json, os, re, subprocess, sys, time, threading
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
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

def load_sent_set():
    sent = set()
    for row in read_csv(SENT_LOG):
        e = (row.get("To Email") or "").strip().lower()
        if e: sent.add(e)
    return sent

# ── PIPELINE STATE ────────────────────────────────────────────────
_pipeline_proc  = None
_pipeline_stage = "idle"
_pipeline_lock  = threading.Lock()

def pipeline_running():
    global _pipeline_proc
    if _pipeline_proc and _pipeline_proc.poll() is None:
        return True
    return False

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

    sent_today = 0
    today = date.today().strftime("%Y-%m-%d")
    for row in read_csv(SENT_LOG):
        if (row.get("Sent At","") or "").startswith(today):
            sent_today += 1

    return {
        "total_collected": total_collected,
        "total_sent": total_sent,
        "pending": pending,
        "sent_today": sent_today,
        "bad_emails": bad,
        "raw_pending": raw_pending,
        "pipeline_running": pipeline_running(),
        "pipeline_stage": _pipeline_stage,
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
@app.get("/api/sent")
def get_sent(page: int = 1, limit: int = 30):
    rows  = list(reversed(read_csv(SENT_LOG)))
    total = len(rows)
    start = (page-1)*limit
    return {
        "total": total,
        "page": page,
        "pages": (total+limit-1)//limit,
        "rows": rows[start:start+limit]
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
            global _pipeline_stage
            _pipeline_stage = "scraping"
            _pipeline_proc = subprocess.Popen(
                [PYTHON, str(PIPELINE)],
                cwd=str(LEADS_DIR),
                env={**os.environ}
            )
            _pipeline_proc.wait()
            _pipeline_stage = "idle"
        background_tasks.add_task(run_scrape)
        return {"ok": True, "msg": "Pipeline started"}

    if action == "send":
        if pipeline_running():
            return {"ok": False, "msg": "Pipeline already running"}
        def run_send():
            global _pipeline_stage
            _pipeline_stage = "sending"
            # Run just the send portion via a minimal script
            send_script = str(LEADS_DIR / "send_emails_batch_475.py")
            _pipeline_proc = subprocess.Popen([PYTHON, send_script], cwd=str(LEADS_DIR))
            _pipeline_proc.wait()
            _pipeline_stage = "idle"
        background_tasks.add_task(run_send)
        return {"ok": True, "msg": "Send started"}

    if action == "stop":
        if _pipeline_proc and _pipeline_proc.poll() is None:
            _pipeline_proc.terminate()
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

# ── API: GITHUB SYNC ───────────────────────────────────────────────
@app.post("/api/sync")
def sync_github():
    try:
        # Git add + commit + push from leads dir
        result = subprocess.run(
            ["git", "add", "emails_generated.csv", "sent_log.csv"],
            cwd=str(LEADS_DIR), capture_output=True, text=True
        )
        result2 = subprocess.run(
            ["git", "commit", "-m", f"data: sync {date.today()}"],
            cwd=str(LEADS_DIR), capture_output=True, text=True
        )
        result3 = subprocess.run(
            ["git", "push", "origin", "main"],
            cwd=str(LEADS_DIR), capture_output=True, text=True
        )
        return {
            "ok": True,
            "msg": result3.stdout or result3.stderr or "Pushed"
        }
    except Exception as e:
        return {"ok": False, "msg": str(e)}

# ── SERVE FRONTEND ────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/index.html")

# ── RUN ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Outreach Dashboard running at http://localhost:5050")
    uvicorn.run(app, host="0.0.0.0", port=5050, reload=False)
