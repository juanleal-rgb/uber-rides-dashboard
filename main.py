import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func

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
    total_calls = db.query(func.count(CallRecord.id)).scalar() or 0

    unique_phones = db.query(
        func.count(func.distinct(CallRecord.phone))
    ).scalar() or 0

    human_needed = db.query(
        func.count(CallRecord.id)
    ).filter(CallRecord.call_human == True).scalar() or 0

    avg_attempts_raw = db.query(func.avg(CallRecord.attempt)).scalar()
    avg_attempts = round(float(avg_attempts_raw), 2) if avg_attempts_raw else 0.0

    status_rows = db.query(
        CallRecord.status,
        func.count(CallRecord.id).label("count")
    ).group_by(CallRecord.status).all()
    status_dist = {row.status: row.count for row in status_rows}

    sentiment_rows = db.query(
        CallRecord.sentiment,
        func.count(CallRecord.id).label("count")
    ).group_by(CallRecord.sentiment).all()
    sentiment_dist = {row.sentiment: row.count for row in sentiment_rows}

    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    time_rows = db.query(
        func.date_trunc("day", CallRecord.created_at).label("day"),
        func.count(CallRecord.id).label("count")
    ).filter(
        CallRecord.created_at >= thirty_days_ago
    ).group_by("day").order_by("day").all()
    calls_over_time = [
        {"date": row.day.strftime("%Y-%m-%d"), "count": row.count}
        for row in time_rows
    ]

    phone_rows = db.query(
        CallRecord.phone,
        func.count(CallRecord.id).label("count")
    ).group_by(CallRecord.phone).order_by(
        func.count(CallRecord.id).desc()
    ).limit(10).all()
    top_phones = [{"phone": row.phone, "count": row.count} for row in phone_rows]

    handoff_rate = round(
        (human_needed / total_calls * 100) if total_calls > 0 else 0.0, 1
    )

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
            "created_at": r.created_at.isoformat(),
        }
        for r in recent
    ]

    return {
        "summary": {
            "total_calls":   total_calls,
            "unique_phones": unique_phones,
            "human_needed":  human_needed,
            "avg_attempts":  avg_attempts,
            "handoff_rate":  handoff_rate,
        },
        "status_distribution":    status_dist,
        "sentiment_distribution": sentiment_dist,
        "calls_over_time":        calls_over_time,
        "top_phones":             top_phones,
        "attempts_distribution":  attempts_dist,
        "recent_calls":           recent_calls,
    }
