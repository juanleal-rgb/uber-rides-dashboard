"""
Seed the database with mock call records covering all status/sentiment combinations.
Run locally:  python seed_data.py
Run on Railway: railway run python seed_data.py
"""

import random
from datetime import datetime, timedelta, timezone
from database import SessionLocal, engine
from models import CallRecord
from sqlalchemy import text

# ── Ensure duration column exists before seeding ──────────────────────────────
with engine.connect() as conn:
    conn.execute(text(
        "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS duration INTEGER NOT NULL DEFAULT 0"
    ))
    conn.commit()

# ── Possible values ───────────────────────────────────────────────────────────
STATUSES = [
    "avoid callback",
    "hang up",
    "success",
    "failed",
    "not the right person",
    "not interested",
]

SENTIMENTS = ["Satisfied", "neutral", "upset"]

PHONES = [
    "+34618953592", "+34622134567", "+34699012345", "+34611223344",
    "+34655443322", "+34677889900", "+34644556677", "+34633221100",
    "+34688990011", "+34612345678", "+34698765432", "+34623456789",
]

# Realistic summaries keyed by status
SUMMARIES = {
    "success": [
        "El conductor confirmó su disponibilidad y está listo para comenzar.",
        "Llamada exitosa. El conductor aceptó los términos y completó el proceso.",
        "Se resolvieron todas las dudas. El conductor está activado en la plataforma.",
        "El conductor proporcionó toda la documentación necesaria.",
    ],
    "hang up": [
        "El conductor colgó antes de que se pudiera completar la llamada.",
        "La llamada fue interrumpida sin previo aviso.",
        "El conductor colgó al inicio de la llamada.",
        "No hay contenido en la llamada o la llamada fue al buzón de voz.",
    ],
    "avoid callback": [
        "El conductor solicitó no recibir más llamadas por el momento.",
        "El conductor indicó que contactará él mismo cuando esté disponible.",
        "El conductor pidió que no se le llamara esta semana.",
        "El conductor prefiere ser contactado por correo electrónico.",
    ],
    "failed": [
        "No se pudo completar la llamada por problemas técnicos.",
        "La línea estaba ocupada durante todos los intentos.",
        "El número no estaba disponible en el momento de la llamada.",
        "Error en la conexión. Se intentará de nuevo más tarde.",
    ],
    "not the right person": [
        "La persona que respondió no era el conductor registrado.",
        "Atendió un familiar que indicó que el conductor no estaba disponible.",
        "El número pertenece a otra persona. Se debe verificar el contacto.",
        "Respondió alguien que no conocía la solicitud de Uber.",
    ],
    "not interested": [
        "El conductor indicó que no está interesado en continuar con Uber.",
        "El conductor decidió no seguir adelante con el proceso de activación.",
        "El conductor expresó que encontró otra oportunidad laboral.",
        "El conductor no desea continuar. Se cerrará el expediente.",
    ],
}

# Sentiment probabilities per status (status → {sentiment: weight})
SENTIMENT_WEIGHTS = {
    "success":              {"Satisfied": 0.75, "neutral": 0.20, "upset": 0.05},
    "hang up":              {"Satisfied": 0.05, "neutral": 0.60, "upset": 0.35},
    "avoid callback":       {"Satisfied": 0.10, "neutral": 0.55, "upset": 0.35},
    "failed":               {"Satisfied": 0.05, "neutral": 0.50, "upset": 0.45},
    "not the right person": {"Satisfied": 0.10, "neutral": 0.75, "upset": 0.15},
    "not interested":       {"Satisfied": 0.05, "neutral": 0.30, "upset": 0.65},
}

# Human callback probability per status
HUMAN_WEIGHTS = {
    "success":              0.05,
    "hang up":              0.10,
    "avoid callback":       0.30,
    "failed":               0.20,
    "not the right person": 0.40,
    "not interested":       0.60,
}

# Typical call duration ranges in seconds per status
DURATION_RANGES = {
    "success":              (90,  420),
    "hang up":              (0,   30),
    "avoid callback":       (20,  120),
    "failed":               (0,   15),
    "not the right person": (15,  90),
    "not interested":       (30,  180),
}


def weighted_choice(weights: dict) -> str:
    keys = list(weights.keys())
    probs = list(weights.values())
    return random.choices(keys, weights=probs, k=1)[0]


def random_date(days_back: int = 30) -> datetime:
    now = datetime.now(timezone.utc)
    offset = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return now - offset


def build_record(status: str, attempt: int = 1) -> CallRecord:
    sentiment = weighted_choice(SENTIMENT_WEIGHTS[status])
    call_human = random.random() < HUMAN_WEIGHTS[status]
    lo, hi = DURATION_RANGES[status]
    duration = random.randint(lo, hi)
    phone = random.choice(PHONES)
    summary = random.choice(SUMMARIES[status])

    record = CallRecord(
        phone      = phone,
        status     = status,
        sentiment  = sentiment,
        call_human = call_human,
        summary    = summary,
        attempt    = attempt,
        duration   = duration,
        created_at = random_date(30),
    )
    return record


def seed(n: int = 120):
    db = SessionLocal()
    records = []

    # Guarantee at least 8 records per status
    for status in STATUSES:
        for _ in range(8):
            records.append(build_record(status, attempt=random.randint(1, 3)))

    # Fill the rest randomly
    remaining = n - len(records)
    for _ in range(remaining):
        status = random.choice(STATUSES)
        attempt = random.randint(1, 3)
        records.append(build_record(status, attempt))

    random.shuffle(records)

    try:
        db.bulk_save_objects(records)
        db.commit()
        print(f"✓ Inserted {len(records)} mock call records.")

        # Print distribution summary
        from collections import Counter
        status_counts = Counter(r.status for r in records)
        print("\nStatus distribution:")
        for s, c in sorted(status_counts.items()):
            print(f"  {s:<25} {c}")

        sentiment_counts = Counter(r.sentiment for r in records)
        print("\nSentiment distribution:")
        for s, c in sorted(sentiment_counts.items()):
            print(f"  {s:<15} {c}")

        human_count = sum(1 for r in records if r.call_human)
        print(f"\nHuman callback needed: {human_count} ({human_count/len(records)*100:.1f}%)")
    except Exception as e:
        db.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    seed(n)
