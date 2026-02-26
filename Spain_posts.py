"""
post_calls.py
-------------
Reads analysis.json and POSTs one aggregated record per entry to the calls API.

Sentiment mapping (based on dominant outcome, NOT inferred from hang-ups alone):
  "satisfied"  → success
  "upset"      → not interested, avoid callback
  "neutral"    → everything else (hang up, voicemail, not the right person,
                  wrong flow, callback requested)

Status mapping (dominant outcome priority order):
  success > callback requested > not interested > avoid callback >
  not the right person > wrong flow > hang up > voicemail

call_human → "TRUE" if calls_excluding_voicemail > 0, else "FALSE"
attempt    → sum of ALL result_breakdown values (including voicemails)
duration   → minutes_excluding_voicemail * 60, rounded to int (seconds)
country    → "ES" by default; "PT" if phone starts with +351
"""

import json
import time
import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL   = "https://uber-rides-dashboard-production.up.railway.app/api/calls"   # ← update this
JSON_FILE = "analysis.json"                             # ← path to your file
DELAY_BETWEEN_REQUESTS = 0.2  # seconds, to be polite to the server
# ─────────────────────────────────────────────────────────────────────────────

# Priority order for determining the dominant status of an entry
STATUS_PRIORITY = [
    "success",
    "callback requested",
    "not interested",
    "avoid callback",
    "not the right person",
    "wrong flow",
    "hang up",
    "voicemail",
]

# Sentiment derived from the dominant outcome
SENTIMENT_MAP = {
    "success":              "satisfied",
    "callback requested":   "neutral",
    "not interested":       "upset",
    "avoid callback":       "upset",
    "not the right person": "neutral",
    "wrong flow":           "neutral",
    "hang up":              "neutral",
    "voicemail":            "neutral",
}


def clean_phone(raw: str) -> str:
    """Remove spaces and fix double-plus typos (e.g. '++351...' -> '+351...')."""
    cleaned = raw.replace(" ", "")
    while cleaned.startswith("++"):
        cleaned = cleaned[1:]
    return cleaned


def get_country(phone: str) -> str:
    if phone.startswith("+351"):
        return "PT"
    return "ES"


def dominant_status(result_breakdown: dict) -> str:
    """Return the highest-priority outcome present in result_breakdown."""
    for status in STATUS_PRIORITY:
        if result_breakdown.get(status, 0) > 0:
            return status
    # Fallback: return first key found, or "unknown"
    return next(iter(result_breakdown), "unknown")


def build_summary(result_breakdown: dict) -> str:
    parts = [f"{count} {label}" for label, count in result_breakdown.items() if count > 0]
    return ", ".join(parts) if parts else "no calls recorded"


def build_payload(entry: dict) -> dict:
    phone    = clean_phone(entry["phone"])
    rb       = entry.get("result_breakdown", {})
    dom      = dominant_status(rb)
    duration = int(round(entry.get("minutes_excluding_voicemail", 0.0) * 60))

    return {
        "phone":      phone,
        "status":     dom,
        "sentiment":  SENTIMENT_MAP.get(dom, "neutral"),
        "call_human": "TRUE" if dom == "callback requested" else "FALSE",
        "summary":    build_summary(rb),
        "attempt":    str(sum(rb.values())),
        "duration":   str(duration),
        "country":    get_country(phone),
    }


def post_with_retry(session: requests.Session, payload: dict) -> tuple[bool, int]:
    """POST payload, retry once on failure. Returns (success, http_status)."""
    r = None
    for attempt in range(2):
        try:
            r = session.post(API_URL, json=payload, timeout=10)
            if r.ok:
                return True, r.status_code
            if attempt == 0:
                print(f"           Retrying (HTTP {r.status_code})...")
                time.sleep(1)
        except requests.RequestException as exc:
            if attempt == 0:
                print(f"           Retrying after error: {exc}")
                time.sleep(1)
            else:
                return False, 0
    return False, getattr(r, "status_code", 0)


def main():
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    data    = data[10:]           # ← skip first 10 (already posted)
    total   = len(data)
    success = 0
    failed  = 0

    print(f"Loaded {total} entries from {JSON_FILE}")
    print(f"Posting to {API_URL}\n")

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    for i, entry in enumerate(data, 1):
        payload = build_payload(entry)
        name    = entry.get("partner_name", "unknown")

        print(f"[{i:>4}/{total}] {payload['phone']} -- {name}")
        print(f"           status={payload['status']} | sentiment={payload['sentiment']} | attempt={payload['attempt']} | duration={payload['duration']}s")

        ok, status_code = post_with_retry(session, payload)

        if ok:
            print(f"           OK HTTP {status_code}")
            success += 1
        else:
            print(f"           SKIPPED after retry failure")
            failed += 1

        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\n{'--'*25}")
    print(f"Succeeded : {success}/{total}")
    print(f"Failed    : {failed}/{total}")


if __name__ == "__main__":
    main()