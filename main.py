import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, text, case

from database import engine, get_db, Base
from models import CallRecord
from schemas import CallRecordCreate, CallRecordResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — creating database tables...")
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        conn.execute(text(
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS duration INTEGER NOT NULL DEFAULT 0"
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


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.post("/api/calls", response_model=CallRecordResponse, status_code=201)
async def receive_call(payload: CallRecordCreate, db: Session = Depends(get_db)):
    record = CallRecord(
        phone      = payload.phone,
        status     = payload.status,
        sentiment  = payload.sentiment,
        call_human = payload.call_human,
        summary    = payload.summary,
        attempt    = payload.attempt,
        duration   = payload.duration,
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    logger.info(
        f"New call record: #{record.id} | phone={record.phone} "
        f"| status={record.status} | attempt={record.attempt}"
    )
    return record


@app.get("/api/analytics")
async def get_analytics(db: Session = Depends(get_db)):
    # ── 1 query: all summary stats in one pass ────────────────────────────────
    s = db.query(
        func.count(CallRecord.id).label("total_calls"),
        func.count(case((CallRecord.call_human == True, 1))).label("human_needed"),
        func.avg(CallRecord.attempt).label("avg_attempts"),
        func.avg(CallRecord.duration).label("avg_duration"),
        func.sum(CallRecord.duration + 120).label("total_seconds_saved"),
    ).one()

    total_calls       = s.total_calls or 0
    human_needed      = s.human_needed or 0
    avg_attempts      = round(float(s.avg_attempts), 2) if s.avg_attempts else 0.0
    avg_duration      = round(float(s.avg_duration), 1) if s.avg_duration else 0.0
    total_hours_saved = round(float(s.total_seconds_saved or 0) / 3600, 1)
    handoff_rate      = round((human_needed / total_calls * 100) if total_calls > 0 else 0.0, 1)

    # ── 2 query: status distribution ─────────────────────────────────────────
    status_dist = {
        row.status: row.count
        for row in db.query(
            CallRecord.status,
            func.count(CallRecord.id).label("count")
        ).group_by(CallRecord.status).all()
    }

    # ── 3 query: sentiment distribution ──────────────────────────────────────
    sentiment_dist = {
        row.sentiment: row.count
        for row in db.query(
            CallRecord.sentiment,
            func.count(CallRecord.id).label("count")
        ).group_by(CallRecord.sentiment).all()
    }

    # ── 4 query: calls + avg duration per day (combined) ─────────────────────
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    time_rows = db.query(
        func.date_trunc("day", CallRecord.created_at).label("day"),
        func.count(CallRecord.id).label("count"),
        func.avg(CallRecord.duration).label("avg_duration"),
    ).filter(
        CallRecord.created_at >= thirty_days_ago
    ).group_by("day").order_by("day").all()

    calls_over_time = [
        {"date": row.day.strftime("%Y-%m-%d"), "count": row.count}
        for row in time_rows
    ]
    duration_over_time = [
        {"date": row.day.strftime("%Y-%m-%d"), "avg_duration": round(float(row.avg_duration), 1)}
        for row in time_rows
    ]

    # ── 5 query: contact rate per attempt ─────────────────────────────────────
    contact_rate_rows = db.query(
        CallRecord.attempt,
        func.count(CallRecord.id).label("total"),
        func.count(case((CallRecord.status != "failed", 1))).label("connected"),
    ).group_by(CallRecord.attempt).order_by(CallRecord.attempt).all()
    contact_rate_by_attempt = [
        {
            "attempt":   row.attempt,
            "total":     row.total,
            "connected": row.connected,
            "rate":      round(row.connected / row.total * 100, 1) if row.total > 0 else 0.0,
        }
        for row in contact_rate_rows
    ]

    # ── 6 query: attempts distribution + recent calls (combined fetch) ────────
    attempt_rows = db.query(
        CallRecord.attempt,
        func.count(CallRecord.id).label("count")
    ).group_by(CallRecord.attempt).order_by(CallRecord.attempt).all()
    attempts_dist = {str(row.attempt): row.count for row in attempt_rows}

    recent = db.query(CallRecord).order_by(
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
    hour_rows = db.query(
        func.extract("hour", CallRecord.created_at).label("hour"),
        func.count(CallRecord.id).label("count"),
    ).filter(
        CallRecord.status != "failed"
    ).group_by("hour").order_by("hour").all()
    calls_by_hour = [{"hour": int(row.hour), "count": row.count} for row in hour_rows]

    # ── 8 query: connected calls by day of week ───────────────────────────────
    dow_rows = db.query(
        func.extract("dow", CallRecord.created_at).label("dow"),
        func.count(CallRecord.id).label("count"),
    ).filter(
        CallRecord.status != "failed"
    ).group_by("dow").order_by("dow").all()
    calls_by_dow = [{"dow": int(row.dow), "count": row.count} for row in dow_rows]

    return {
        "summary": {
            "total_calls":        total_calls,
            "human_needed":       human_needed,
            "avg_attempts":       avg_attempts,
            "avg_duration":       avg_duration,
            "handoff_rate":       handoff_rate,
            "total_hours_saved":  total_hours_saved,
        },
        "status_distribution":    status_dist,
        "sentiment_distribution": sentiment_dist,
        "calls_over_time":        calls_over_time,
        "duration_over_time":     duration_over_time,
        "contact_rate_by_attempt": contact_rate_by_attempt,
        "attempts_distribution":  attempts_dist,
        "recent_calls":           recent_calls,
        "calls_by_hour":          calls_by_hour,
        "calls_by_dow":           calls_by_dow,
    }
