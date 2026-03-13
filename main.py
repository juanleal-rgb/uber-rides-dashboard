import os
import hashlib
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Depends, Request, Form, Cookie, Query
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, text, case, and_

from database import engine, get_db, Base
from models import CallRecord
from schemas import CallRecordCreate, CallRecordResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD")
if not DASHBOARD_PASSWORD:
    raise RuntimeError("DASHBOARD_PASSWORD env var not set")
AUTH_TOKEN = hashlib.sha256(DASHBOARD_PASSWORD.encode()).hexdigest()

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
if not ADMIN_PASSWORD:
    raise RuntimeError("ADMIN_PASSWORD env var not set")
ADMIN_TOKEN = hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — creating database tables...")
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        conn.execute(text(
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS duration INTEGER NOT NULL DEFAULT 0"
        ))
        conn.execute(text(
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS country VARCHAR(10) NOT NULL DEFAULT 'PT'"
        ))
        conn.execute(text(
            "ALTER TABLE call_records ALTER COLUMN country TYPE VARCHAR(10)"
        ))
        conn.execute(text(
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS call_url TEXT"
        ))
        conn.commit()
    logger.info("Database tables ready.")
    yield
    logger.info("Shutting down.")


app = FastAPI(
    title="Uber Rides Call Analytics Dashboard",
    description="Analytics dashboard for call data ingestion and visualization",
    version="1.0.0",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="."), name="static")


@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login_post(request: Request, password: str = Form(...)):
    if password == ADMIN_PASSWORD:
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(key="dashboard_auth", value=ADMIN_TOKEN, httponly=True, samesite="lax")
        return response
    if password == DASHBOARD_PASSWORD:
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(key="dashboard_auth", value=AUTH_TOKEN, httponly=True, samesite="lax")
        return response
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Incorrect password. Please try again."},
        status_code=401,
    )


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, dashboard_auth: str = Cookie(default=None)):
    if dashboard_auth not in (AUTH_TOKEN, ADMIN_TOKEN):
        return RedirectResponse(url="/login", status_code=303)
    is_admin = dashboard_auth == ADMIN_TOKEN
    return templates.TemplateResponse("dashboard.html", {"request": request, "is_admin": is_admin})



BATCH_URLS = {
    "PT":  "https://workflows.platform.happyrobot.ai/hooks/7s25ex11glf5",
    "ES":  None,
    "ES2": "https://workflows.platform.happyrobot.ai/hooks/pdfsa96ul697",
}

@app.post("/api/launch-batch")
async def launch_batch(request: Request, dashboard_auth: str = Cookie(default=None)):
    if dashboard_auth != ADMIN_TOKEN:
        return JSONResponse({"error": "No autorizado."}, status_code=403)

    body = await request.json()
    country = body.get("country", "")

    url = BATCH_URLS.get(country)
    if not url:
        return JSONResponse({"error": f"No hay webhook configurado para {country}."}, status_code=400)

    async with httpx.AsyncClient() as client:
        await client.post(url)

    return JSONResponse({"ok": True})


@app.post("/api/calls", response_model=CallRecordResponse, status_code=201)
async def receive_call(payload: CallRecordCreate, db: Session = Depends(get_db)):
    record = CallRecord(
        phone      = payload.phone,
        status     = payload.status,
        sentiment  = payload.sentiment,
        call_human = payload.call_human,
        summary    = payload.summary,
        call_url   = payload.call_url,
        attempt    = payload.attempt,
        duration   = payload.duration,
        country    = payload.country,
    )
    if payload.created_at:
        record.created_at = payload.created_at
    db.add(record)
    db.commit()
    db.refresh(record)
    logger.info(
        f"New call record: #{record.id} | phone={record.phone} "
        f"| status={record.status} | attempt={record.attempt}"
    )
    return record


def _assign_category(status: str) -> str:
    return {
        "success":              "success",
        "callback requested":   "callback",
        "not interested":       "not_interested",
        "avoid callback":       "avoid",
        "not the right person": "wrong_contact",
        "wrong flow":           "wrong_flow",
        "hang up":              "hung_up",
        "voicemail":            "voicemail",
        "failed":               "failed",
        "already complete":     "already_complete",
    }.get((status or "").lower(), "hung_up")


@app.get("/api/monitor")
async def get_monitor(
    db: Session = Depends(get_db),
    country: str = Query(default="ALL"),
):
    def cf(q):
        if country != "ALL":
            selected = [c.strip() for c in country.split(",") if c.strip()]
            if selected:
                return q.filter(CallRecord.country.in_(selected))
        return q

    rows = cf(db.query(CallRecord)).order_by(
        CallRecord.phone,
        CallRecord.created_at.desc(),
    ).all()

    partners: dict = {}
    for r in rows:
        if r.phone not in partners:
            partners[r.phone] = {
                "phone":          r.phone,
                "country":        r.country,
                "total_calls":    0,
                "last_status":    r.status,
                "last_sentiment": r.sentiment,
                "last_contact":   r.created_at.isoformat(),
                "last_duration":  r.duration,
                "last_summary":   r.summary or "",
                "last_call_human": r.call_human,
                "category":       _assign_category(r.status),
                "calls":          [],
            }
        partners[r.phone]["total_calls"] += 1
        partners[r.phone]["calls"].append({
            "id":         r.id,
            "status":     r.status,
            "sentiment":  r.sentiment,
            "call_human": r.call_human,
            "attempt":    r.attempt,
            "duration":   r.duration,
            "summary":    r.summary or "",
            "call_url":   r.call_url or "",
            "created_at": r.created_at.isoformat(),
        })

    sorted_partners = sorted(
        partners.values(),
        key=lambda p: p["last_contact"],
        reverse=True,
    )
    return {"partners": sorted_partners}


@app.get("/api/analytics")
async def get_analytics(
    db: Session = Depends(get_db),
    country: str = Query(default="ALL"),
):
    # Helper: apply country filter to any query
    def cf(q):
        if country != "ALL":
            selected = [c.strip() for c in country.split(",") if c.strip()]
            if selected:
                return q.filter(CallRecord.country.in_(selected))
        return q

    # ── 1 query: all summary stats in one pass ────────────────────────────────
    s = cf(db.query(
        func.count(CallRecord.id).label("total_calls"),
        func.count(case((CallRecord.call_human == True, 1))).label("human_needed"),
        func.avg(CallRecord.attempt).label("avg_attempts"),
        func.avg(CallRecord.duration).label("avg_duration"),
        func.sum(CallRecord.duration + 120).label("total_seconds_saved"),
    )).one()

    total_calls       = s.total_calls or 0
    human_needed      = s.human_needed or 0
    avg_attempts      = round(float(s.avg_attempts), 2) if s.avg_attempts else 0.0
    avg_duration      = round(float(s.avg_duration), 1) if s.avg_duration else 0.0
    total_hours_saved = round(float(s.total_seconds_saved or 0) / 3600, 1)
    handoff_rate      = round((human_needed / total_calls * 100) if total_calls > 0 else 0.0, 1)
    # ── 1b: new KPIs ─────────────────────────────────────────────────────────
    partners_contacted = cf(db.query(
        func.count(func.distinct(CallRecord.phone))
    )).scalar() or 0

    connected_calls = cf(db.query(
        func.count(CallRecord.id)
    )).filter(
        CallRecord.status.notin_(["voicemail", "hang up"])
    ).scalar() or 0

    # ── 2 query: status distribution ─────────────────────────────────────────
    status_excluded = ["voicemail", "hang up"]
    status_dist = {
        row.status: row.count
        for row in cf(db.query(
            CallRecord.status,
            func.count(CallRecord.id).label("count")
        )).filter(
            CallRecord.status.notin_(status_excluded)
        ).group_by(CallRecord.status).all()
    }

    # ── 3 query: sentiment distribution (normalize casing) ──────────────────
    sentiment_dist = {
        row.sentiment: row.count
        for row in cf(db.query(
            func.lower(CallRecord.sentiment).label("sentiment"),
            func.count(CallRecord.id).label("count")
        )).filter(
            CallRecord.status != "voicemail"
        ).group_by(func.lower(CallRecord.sentiment)).all()
    }

    # ── 4 query: calls + avg duration per day (combined) ─────────────────────
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    time_rows = cf(db.query(
        func.date_trunc("day", CallRecord.created_at).label("day"),
        func.count(CallRecord.id).label("count"),
        func.avg(CallRecord.duration).label("avg_duration"),
    )).filter(
        CallRecord.created_at >= thirty_days_ago,
        CallRecord.status.notin_(["voicemail", "hang up"])
    ).group_by("day").order_by("day").all()

    calls_over_time = [
        {"date": row.day.strftime("%Y-%m-%d"), "count": row.count}
        for row in time_rows
    ]
    duration_over_time = [
        {"date": row.day.strftime("%Y-%m-%d"), "avg_duration": round(float(row.avg_duration), 1)}
        for row in time_rows
    ]

    # ── 5 query: attempts distribution + recent calls (combined fetch) ────────
    attempt_rows = cf(db.query(
        CallRecord.attempt,
        func.count(CallRecord.id).label("count")
    )).filter(
        CallRecord.status != "voicemail"
    ).group_by(CallRecord.attempt).order_by(CallRecord.attempt).all()
    attempts_dist = {str(row.attempt): row.count for row in attempt_rows}

    recent = cf(db.query(CallRecord)).order_by(
        CallRecord.created_at.desc()
    ).limit(20).all()
    recent_calls = [
        {
            "id":         r.id,
            "phone":      r.phone,
            "status":     r.status,
            "sentiment":  r.sentiment,
            "call_human": r.call_human,
            "summary":    r.summary or "",
            "attempt":    r.attempt,
            "duration":   r.duration,
            "created_at": r.created_at.isoformat(),
        }
        for r in recent
    ]

    # ── 7 query: connected calls by hour of day ──────────────────────────────
    hour_rows = cf(db.query(
        func.extract("hour", CallRecord.created_at).label("hour"),
        func.count(CallRecord.id).label("count"),
    )).filter(
        CallRecord.status.notin_(["failed", "voicemail", "hang up"])
    ).group_by("hour").order_by("hour").all()
    calls_by_hour = [{"hour": int(row.hour), "count": row.count} for row in hour_rows]

    # ── 8 query: connected calls by day of week ───────────────────────────────
    dow_rows = cf(db.query(
        func.extract("dow", CallRecord.created_at).label("dow"),
        func.count(CallRecord.id).label("count"),
    )).filter(
        CallRecord.status.notin_(["failed", "voicemail", "hang up"])
    ).group_by("dow").order_by("dow").all()
    calls_by_dow = [{"dow": int(row.dow), "count": row.count} for row in dow_rows]

    # ── 9 query: retry intelligence (partner-level aggregation) ─────────
    partner_sub = cf(db.query(
        CallRecord.phone,
        func.max(CallRecord.attempt).label("max_attempt"),
        func.max(case((CallRecord.status == "success", 1), else_=0)).label("has_success"),
        func.min(case((CallRecord.status == "success", CallRecord.attempt))).label("success_at_attempt"),
    )).group_by(CallRecord.phone).subquery()

    retry_row = db.query(
        func.count().label("total_partners"),
        func.coalesce(func.sum(partner_sub.c.has_success), 0).label("converted"),
        func.avg(partner_sub.c.success_at_attempt).label("avg_to_success"),
        func.coalesce(func.sum(case(
            (and_(partner_sub.c.has_success == 0, partner_sub.c.max_attempt >= 10), 1),
            else_=0
        )), 0).label("exhausted"),
        func.coalesce(func.sum(case(
            (and_(partner_sub.c.has_success == 0, partner_sub.c.max_attempt < 10), 1),
            else_=0
        )), 0).label("pending"),
    ).select_from(partner_sub).one()

    r_total       = int(retry_row.total_partners or 0)
    r_converted   = int(retry_row.converted or 0)
    r_avg         = round(float(retry_row.avg_to_success), 1) if retry_row.avg_to_success else 0.0
    r_exhausted   = int(retry_row.exhausted or 0)
    r_pending     = int(retry_row.pending or 0)
    r_conv_rate   = round((r_converted / r_total * 100) if r_total > 0 else 0.0, 1)

    return {
        "summary": {
            "total_calls":          total_calls,
            "human_needed":         human_needed,
            "avg_attempts":         avg_attempts,
            "avg_duration":         avg_duration,
            "handoff_rate":         handoff_rate,
            "total_hours_saved":    total_hours_saved,
            "partners_contacted":   partners_contacted,
            "connected_calls":      connected_calls,
        },
        "status_distribution":    status_dist,
        "sentiment_distribution": sentiment_dist,
        "calls_over_time":        calls_over_time,
        "duration_over_time":     duration_over_time,
        "attempts_distribution":  attempts_dist,
        "recent_calls":           recent_calls,
        "calls_by_hour":          calls_by_hour,
        "calls_by_dow":           calls_by_dow,
        "retry_intelligence": {
            "conversion_rate":          r_conv_rate,
            "avg_attempts_to_success":  r_avg,
            "exhausted_partners":       r_exhausted,
            "pending_partners":         r_pending,
            "converted_partners":       r_converted,
            "total_partners":           r_total,
        },
    }
