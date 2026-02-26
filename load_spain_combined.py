"""
load_spain_combined.py
----------------------
Loads ALL 767 individual Spain calls from results.json into the Railway DB.
Enriches with analysis.json data (minutes_excluding_voicemail per partner).

Each call in results.json becomes 1 row in call_records with:
  - Real timestamps (Nov 2025 – Jan 2026)
  - Individual duration per call
  - Attempt number = Nth call to same phone (sorted by timestamp)
  - Status mapped from HappyRobot classification
  - Sentiment derived from status
  - country = "ES"

Replaces any previously posted Spain aggregated records (the 10 from analysis.json).
"""

import json
import time
import requests
from collections import defaultdict
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL   = "https://uber-rides-dashboard-production.up.railway.app/api/calls"
DELAY     = 0.15  # seconds between requests
# ─────────────────────────────────────────────────────────────────────────────

# UUID-based keys in results.json → short names
KEY_PARTNER  = "019aa6c0-ee74-7df4-b3c9-ec883c90c557.data.partner_name"
KEY_PHONE    = "019b0c77-d32c-7ba0-b4b8-330cf4ffa55b.to_number"
KEY_DURATION = "019b0c78-0b08-7a01-b7d4-884e6a229ada.duration"
KEY_STATUS   = "019b0c78-5a08-75cc-a580-be0799253dee.response.classification"

# Sentiment derived from call status (same logic as Spain_posts.py)
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
    cleaned = raw.replace(" ", "")
    while cleaned.startswith("++"):
        cleaned = cleaned[1:]
    return cleaned


def parse_results(results_data: list) -> list:
    """Parse results.json into a flat list of call dicts, sorted by timestamp."""
    calls = []
    for entry in results_data:
        data = entry.get("data", {})
        phone    = clean_phone(data.get(KEY_PHONE, ""))
        partner  = data.get(KEY_PARTNER, "unknown")
        duration = int(data.get(KEY_DURATION, 0))
        status   = data.get(KEY_STATUS, "unknown")
        ts       = entry.get("timestamp", entry.get("completed_at", ""))

        if not phone:
            continue

        calls.append({
            "phone":    phone,
            "partner":  partner,
            "duration": duration,
            "status":   status,
            "timestamp": ts,
        })

    # Sort by timestamp so we can assign attempt numbers
    calls.sort(key=lambda c: c["timestamp"])
    return calls


def assign_attempts(calls: list) -> list:
    """Assign attempt number = Nth call to same phone (chronological order)."""
    phone_counter = defaultdict(int)
    for call in calls:
        phone_counter[call["phone"]] += 1
        call["attempt"] = phone_counter[call["phone"]]
    return calls


def build_payload(call: dict) -> dict:
    status    = call["status"]
    sentiment = SENTIMENT_MAP.get(status, "neutral")
    payload = {
        "phone":      call["phone"],
        "status":     status,
        "sentiment":  sentiment,
        "call_human": "TRUE" if status == "callback requested" else "FALSE",
        "summary":    f"{call['partner']} — {status}",
        "attempt":    str(call["attempt"]),
        "duration":   str(call["duration"]),
        "country":    "ES",
    }
    if call.get("timestamp"):
        payload["created_at"] = call["timestamp"]
    return payload


def post_with_retry(session: requests.Session, payload: dict) -> tuple:
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
    # Load both data sources
    with open("results.json", "r", encoding="utf-8") as f:
        results_data = json.load(f)
    with open("analysis.json", "r", encoding="utf-8") as f:
        analysis_data = json.load(f)

    # Parse and assign attempts
    calls = parse_results(results_data)
    calls = assign_attempts(calls)

    # Build enrichment map from analysis.json (phone → partner data)
    enrichment = {}
    for entry in analysis_data:
        phone = clean_phone(entry["phone"])
        enrichment[phone] = {
            "minutes_excl_vm": entry.get("minutes_excluding_voicemail", 0),
            "calls_excl_vm":   entry.get("calls_excluding_voicemail", 0),
        }

    total   = len(calls)
    success = 0
    failed  = 0

    print(f"Loaded {total} individual calls from results.json")
    print(f"Enrichment data for {len(enrichment)} partners from analysis.json")
    print(f"Posting to {API_URL}\n")

    # Print stats
    from collections import Counter
    status_counts = Counter(c["status"] for c in calls)
    print("Status distribution:")
    for s, count in status_counts.most_common():
        print(f"  {s:<25} {count}")
    print()

    unique_phones = len(set(c["phone"] for c in calls))
    print(f"Unique phones: {unique_phones}")
    print(f"Date range: {calls[0]['timestamp'][:10]} to {calls[-1]['timestamp'][:10]}")
    print()

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    for i, call in enumerate(calls, 1):
        payload = build_payload(call)
        print(f"[{i:>4}/{total}] {payload['phone']} -- {call['partner'][:30]}")
        print(f"           status={payload['status']} | attempt={payload['attempt']} | duration={payload['duration']}s | {call['timestamp'][:19]}")

        ok, status_code = post_with_retry(session, payload)

        if ok:
            print(f"           OK HTTP {status_code}")
            success += 1
        else:
            print(f"           SKIPPED after retry failure")
            failed += 1

        time.sleep(DELAY)

    print(f"\n{'--'*25}")
    print(f"Succeeded : {success}/{total}")
    print(f"Failed    : {failed}/{total}")
    print(f"\nKPI preview:")
    print(f"  Total call attempts: {total}")
    print(f"  Partners contacted:  {unique_phones}")
    connected = sum(1 for c in calls if c["status"] != "voicemail")
    print(f"  Connected calls:     {connected}")


if __name__ == "__main__":
    main()
