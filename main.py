import asyncio
import io
import csv
import json
import logging, sys
import os
from datetime import datetime, timezone
from typing import Optional
import dateutil.parser

import aiohttp

# Counters for Bubble
new_records_count = 0
updated_records_count = 0

# Counters for Back4App
new_records_back4app_count = 0
updated_records_back4app_count = 0

# Setup logging
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Try to setup Google Cloud Logging if available
try:
    from google.cloud import logging as cloud_logging
    cloud_logging_client = cloud_logging.Client()
    cloud_logging_client.setup_logging()
    logger.info("Google Cloud Logging initialized successfully")
except Exception as e:
    logger.warning(f"Google Cloud Logging not available: {e}")

# Configuration
API_BASE_URL = os.environ["BUBBLE_API_BASE_URL"]
API_TOKEN = os.environ["BUBBLE_API_TOKEN"]
TABLE_NAME = os.environ["BUBBLE_TABLE_NAME"]
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", 25))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 25))
RETRY_TOTAL = int(os.environ.get("RETRY_TOTAL", 3))
BACKOFF_FACTOR = float(os.environ.get("BACKOFF_FACTOR", 1))
STATUS_FORCELIST = {429, 500, 502, 503, 504}

# Back4App Configuration
BACK4APP_API_BASE_URL = os.environ.get("BACK4APP_API_BASE_URL", "https://parseapi.back4app.com/classes")
BACK4APP_APP_ID = os.environ.get("BACK4APP_APP_ID", "mK60GEj1uzfoICD3dFxW75KZ5K77bbBoaWeeENeK")
BACK4APP_MASTER_KEY = os.environ.get("BACK4APP_MASTER_KEY", "ZDYmU9PLUhJRhTscXJGBFlU8wThrKY6Q0alTtZu2")
BACK4APP_TABLE_NAME = "API_Connector_Users"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
}

BACK4APP_HEADERS = {
    "X-Parse-Application-Id": BACK4APP_APP_ID,
    "X-Parse-Master-Key": BACK4APP_MASTER_KEY,
    "Content-Type": "application/json",
}

# Helper Functions
def sanitize_phone(phone: str) -> str:
    digits = ''.join(filter(str.isdigit, phone or ''))
    return digits[-10:] if len(digits) > 10 else digits

def parse_number(s: str) -> Optional[float]:
    try:
        return float(s)
    except (TypeError, ValueError):
        return None

def parse_csv_date(s: str) -> Optional[datetime]:
    if not s or not s.strip():
        return None
    try:
        dt = dateutil.parser.parse(s)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        logger.warning(f"Couldn’t parse DateEnrolled '{s}': {e}")
        return None

def parse_record_date(s: str) -> datetime:
    dt_aware = datetime.fromisoformat(s.replace("Z", "+00:00"))
    dt_utc = dt_aware.astimezone(timezone.utc)
    return dt_utc

def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_payload(row: dict) -> dict:
    phone = sanitize_phone(row.get("Phone", ""))
    enrolled = parse_csv_date(row.get("DateEnrolled", ""))
    logged = parse_csv_date(row.get("LastLoggedIn", ""))
    completed = parse_csv_date(row.get("PLE DateCompleted", ""))

    ple_complete = parse_number(row.get("% PLE Complete", ""))
    prep_complete = parse_number(row.get("% Prep Complete", ""))
    sim_complete = parse_number(row.get("% Sim Complete", ""))

    raw_email = row.get("EmailAddress", "")
    email = raw_email.lower().strip()

    payload = {
        "pre_licensing_email": email,
        "first_name": row.get("FirstName"),
        "last_name": row.get("LastName"),
        "phone": phone,
        "imo": row.get("Department"),
        **({"date_enrolled": to_utc_iso(enrolled)} if enrolled else {}),
        **({"pre_licensing_course_last_login": to_utc_iso(logged)} if logged else {}),
        "time_spent_in_course": row.get("TimeSpent"),
        **({"percentage_ple_complete": ple_complete} if ple_complete is not None else {}),
        **({"percentage_prep_complete": prep_complete} if prep_complete is not None else {}),
        **({"percentage_sim_complete": sim_complete} if sim_complete is not None else {}),
        **({"ple_date_completed": to_utc_iso(completed)} if completed else {}),
        "pre_licensing_course": row.get("Course"),
        "hiring_manager": row.get("HiringManager"),
        "prepared_to_pass": row.get("Prepared to Pass"),
    }
    return {k: v for k, v in payload.items() if v is not None}

def to_back4app_payload(row: dict) -> dict:
    """Map CSV row to Back4App API_Connector_Users format"""
    phone = sanitize_phone(row.get("Phone", ""))
    enrolled = parse_csv_date(row.get("DateEnrolled", ""))
    logged = parse_csv_date(row.get("LastLoggedIn", ""))
    completed = parse_csv_date(row.get("PLE DateCompleted", ""))

    ple_complete = parse_number(row.get("% PLE Complete", ""))
    prep_complete = parse_number(row.get("% Prep Complete", ""))
    sim_complete = parse_number(row.get("% Sim Complete", ""))

    raw_email = row.get("EmailAddress", "")
    email = raw_email.lower().strip()

    payload = {
        "first_name_text": row.get("FirstName"),
        "last_name_text": row.get("LastName"),
        "pre_licensing_email_text": email,
        "phone_text": phone,
        "imo_text": row.get("Department"),
        "hiring_manager_text": row.get("HiringManager"),
        "pre_licensing_course_text": row.get("Course"),
        "prepared_to_pass_text": row.get("Prepared to Pass"),
        "time_spent_text": row.get("TimeSpent"),
        **({"date_enrolled_date": to_utc_iso(enrolled)} if enrolled else {}),
        **({"pre_licensing_course_last_login_date": to_utc_iso(logged)} if logged else {}),
        **({"ple_date_completed_date": to_utc_iso(completed)} if completed else {}),
        **({"ple_complete_number": ple_complete} if ple_complete is not None else {}),
        **({"percentage_prep_complete_number": prep_complete} if prep_complete is not None else {}),
        **({"percentage_sim_complete_number": sim_complete} if sim_complete is not None else {}),
    }
    return {k: v for k, v in payload.items() if v is not None}

# Retryable Request
async def request_with_retries(session, method, url, **kwargs):
    for attempt in range(1, RETRY_TOTAL + 1):
        try:
            async with session.request(method, url, headers=HEADERS, **kwargs) as resp:
                if resp.status in STATUS_FORCELIST:
                    raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status)
                return await resp.json() if method.upper() == "GET" else None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request {method} {url} failed (attempt {attempt}): {e}")
            if attempt == RETRY_TOTAL:
                raise
            await asyncio.sleep(BACKOFF_FACTOR * (2 ** (attempt - 1)))

# Retryable Request for Back4App
async def request_with_retries_back4app(session, method, url, **kwargs):
    for attempt in range(1, RETRY_TOTAL + 1):
        try:
            async with session.request(method, url, headers=BACK4APP_HEADERS, **kwargs) as resp:
                if resp.status in STATUS_FORCELIST:
                    raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status)
                return await resp.json() if method.upper() == "GET" else None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Back4App Request {method} {url} failed (attempt {attempt}): {e}")
            if attempt == RETRY_TOTAL:
                raise
            await asyncio.sleep(BACKOFF_FACTOR * (2 ** (attempt - 1)))

# Async API Calls
async def get_records_by_emails(session: aiohttp.ClientSession, emails: list[str]) -> dict:
    url = f"{API_BASE_URL}/{TABLE_NAME}"
    normalized_emails = [email.lower().strip() for email in emails]
    constraints = [{"key": "UserPreLicensingEMAIL", "constraint_type": "in", "value": normalized_emails}]
    params = {"constraints": json.dumps(constraints), "cursor": 0, "limit": 100}
    results = []
    
    while True:
        data = await request_with_retries(session, 'GET', url, params=params) or {}
        response = data.get("response", {})
        results.extend(response.get("results", []))
        remaining = response.get("remaining", 0)
        if remaining == 0:
            break
        params["cursor"] += params["limit"]
    
    # logger.info(f"Found {len(results)} existing records")
    return {r.get("pre_licensing_email", "").lower().strip(): r for r in results}

async def create_record(session: aiohttp.ClientSession, payload: dict):
    url = f"{API_BASE_URL}/{TABLE_NAME}"
    logger.info(f"[CREATE] Sending POST to {url} with payload: {json.dumps(payload)}")
    try:
        async with session.post(url, headers=HEADERS, json=payload) as resp:
            body = await resp.text()
            logger.info(f"[CREATE RESPONSE] status={resp.status}, body={body}")
            resp.raise_for_status()
            try:
                data = json.loads(body)
                logger.debug(f"[CREATE DATA] {data}")
            except Exception:
                logger.debug("[CREATE DATA] Response not JSON")
        logger.info(f"[CREATE SUCCESS] {payload.get('pre_licensing_email')}")
    except Exception as e:
        logger.error(f"[CREATE FAILED] {payload.get('UserPreLicensingEMAIL')} → {e}")
        raise

async def update_record(session: aiohttp.ClientSession, record_id: str, payload: dict, email: str):
    url = f"{API_BASE_URL}/{TABLE_NAME}/{record_id}"
    logger.info(f"[UPDATE] Sending PATCH to {url} with payload: {json.dumps(payload)}")
    try:
        async with session.patch(url, headers=HEADERS, json=payload) as resp:
            body = await resp.text()
            logger.info(f"[UPDATE RESPONSE] status={resp.status}, body={body}")
            resp.raise_for_status()
            try:
                data = json.loads(body)
                logger.debug(f"[UPDATE DATA] {data}")
            except Exception:
                logger.debug("[UPDATE DATA] Response not JSON")
        logger.info(f"[UPDATE SUCCESS] {email}")
    except Exception as e:
        logger.error(f"[UPDATE FAILED] {email} → {e}")
        raise

# Back4App API Functions
async def get_records_by_emails_back4app(session: aiohttp.ClientSession, emails: list[str]) -> dict:
    url = f"{BACK4APP_API_BASE_URL}/{BACK4APP_TABLE_NAME}"
    normalized_emails = [email.lower().strip() for email in emails]
    constraints = [{"key": "pre_licensing_email_text", "constraint_type": "in", "value": normalized_emails}]
    params = {"where": json.dumps({"$or": [{"pre_licensing_email_text": email} for email in normalized_emails]}), "limit": 1000}
    results = []
    
    while True:
        data = await request_with_retries_back4app(session, 'GET', url, params=params) or {}
        response = data.get("results", [])
        results.extend(response)
        if len(response) < 1000:  # Back4App doesn't have remaining field
            break
        # For pagination, we'd need to implement skip/limit logic if needed
    
    return {r.get("pre_licensing_email_text", "").lower().strip(): r for r in results}

async def create_record_back4app(session: aiohttp.ClientSession, payload: dict):
    url = f"{BACK4APP_API_BASE_URL}/{BACK4APP_TABLE_NAME}"
    logger.info(f"[BACK4APP CREATE] Sending POST to {url} with payload: {json.dumps(payload)}")
    try:
        async with session.post(url, headers=BACK4APP_HEADERS, json=payload) as resp:
            body = await resp.text()
            logger.info(f"[BACK4APP CREATE RESPONSE] status={resp.status}, body={body}")
            resp.raise_for_status()
            try:
                data = json.loads(body)
                logger.debug(f"[BACK4APP CREATE DATA] {data}")
            except Exception:
                logger.debug("[BACK4APP CREATE DATA] Response not JSON")
        logger.info(f"[BACK4APP CREATE SUCCESS] {payload.get('pre_licensing_email_text')}")
    except Exception as e:
        logger.error(f"[BACK4APP CREATE FAILED] {payload.get('pre_licensing_email_text')} → {e}")
        raise

async def update_record_back4app(session: aiohttp.ClientSession, record_id: str, payload: dict, email: str):
    url = f"{BACK4APP_API_BASE_URL}/{BACK4APP_TABLE_NAME}/{record_id}"
    logger.info(f"[BACK4APP UPDATE] Sending PUT to {url} with payload: {json.dumps(payload)}")
    try:
        async with session.put(url, headers=BACK4APP_HEADERS, json=payload) as resp:
            body = await resp.text()
            logger.info(f"[BACK4APP UPDATE RESPONSE] status={resp.status}, body={body}")
            resp.raise_for_status()
            try:
                data = json.loads(body)
                logger.debug(f"[BACK4APP UPDATE DATA] {data}")
            except Exception:
                logger.debug("[BACK4APP UPDATE DATA] Response not JSON")
        logger.info(f"[BACK4APP UPDATE SUCCESS] {email}")
    except Exception as e:
        logger.error(f"[BACK4APP UPDATE FAILED] {email} → {e}")
        raise

async def handle_row(row, bubble_map, back4app_map, session, sem):
    global new_records_count, updated_records_count, new_records_back4app_count, updated_records_back4app_count
    
    # Process Bubble
    bubble_payload = to_payload(row)
    bubble_email = bubble_payload.get("pre_licensing_email")
    
    # Process Back4App
    back4app_payload = to_back4app_payload(row)
    back4app_email = back4app_payload.get("pre_licensing_email_text")
    
    async with sem:
        # Process Bubble
        bubble_existing = bubble_map.get(bubble_email) if bubble_email else None
        
        # Parse DB "last login" if present
        bubble_db_val = bubble_existing.get("pre_licensing_course_last_login") if bubble_existing else None
        bubble_db_dt = None
        if bubble_db_val:
            bubble_db_dt = parse_record_date(bubble_db_val)

        # Parse CSV "last login" if present
        bubble_csv_val = bubble_payload.get("pre_licensing_course_last_login")
        bubble_csv_dt = None
        if bubble_csv_val:
            bubble_csv_dt = (
                datetime.fromisoformat(bubble_csv_val.replace("Z", "+00:00"))
                        .astimezone(timezone.utc)
            )
        
        # Process Bubble record
        if bubble_existing:
            if bubble_csv_dt and (bubble_db_dt is None or bubble_csv_dt > bubble_db_dt):
                update_fields = [
                    "pre_licensing_course_last_login",
                    "time_spent_in_course",
                    "percentage_ple_complete",
                    "ple_date_completed",
                    "pre_licensing_course",
                    "hiring_manager",
                    "percentage_prep_complete",
                    "percentage_sim_complete",
                    "prepared_to_pass",
                    "date_enrolled"
                ]
                upd = {k: bubble_payload[k] for k in update_fields if k in bubble_payload and bubble_payload[k] not in (None, "")}
                rid = bubble_existing.get("_id") or bubble_existing.get("id")
                await update_record(session, rid, upd, str(bubble_email))
                updated_records_count += 1
                logger.info(f"[BUBBLE UPDATED] {bubble_email} — changes: {upd}")
            else:
                logger.debug(f"[BUBBLE SKIPPED] {bubble_email} — CSV={bubble_csv_dt}, DB={bubble_db_dt}")
        else:
            await create_record(session, bubble_payload)
            new_records_count += 1
            logger.info(f"[BUBBLE CREATED] {bubble_email}")
        
        # Process Back4App
        back4app_existing = back4app_map.get(back4app_email) if back4app_email else None
        
        # Parse DB "last login" if present
        back4app_db_val = back4app_existing.get("pre_licensing_course_last_login_date") if back4app_existing else None
        back4app_db_dt = None
        if back4app_db_val:
            back4app_db_dt = parse_record_date(back4app_db_val)

        # Parse CSV "last login" if present
        back4app_csv_val = back4app_payload.get("pre_licensing_course_last_login_date")
        back4app_csv_dt = None
        if back4app_csv_val:
            back4app_csv_dt = (
                datetime.fromisoformat(back4app_csv_val.replace("Z", "+00:00"))
                        .astimezone(timezone.utc)
            )
        
        # Process Back4App record
        if back4app_existing:
            if back4app_csv_dt and (back4app_db_dt is None or back4app_csv_dt > back4app_db_dt):
                update_fields = [
                    "pre_licensing_course_last_login_date",
                    "time_spent_text",
                    "ple_complete_number",
                    "ple_date_completed_date",
                    "pre_licensing_course_text",
                    "hiring_manager_text",
                    "percentage_prep_complete_number",
                    "percentage_sim_complete_number",
                    "prepared_to_pass_text",
                    "date_enrolled_date"
                ]
                upd = {k: back4app_payload[k] for k in update_fields if k in back4app_payload and back4app_payload[k] not in (None, "")}
                rid = back4app_existing.get("objectId")
                await update_record_back4app(session, rid, upd, str(back4app_email))
                updated_records_back4app_count += 1
                logger.info(f"[BACK4APP UPDATED] {back4app_email} — changes: {upd}")
            else:
                logger.debug(f"[BACK4APP SKIPPED] {back4app_email} — CSV={back4app_csv_dt}, DB={back4app_db_dt}")
        else:
            await create_record_back4app(session, back4app_payload)
            new_records_back4app_count += 1
            logger.info(f"[BACK4APP CREATED] {back4app_email}")

async def process_chunk(chunk, session, sem):
    # Extract emails for both platforms
    bubble_emails = [r.get("EmailAddress", "").lower().strip() for r in chunk if r.get("EmailAddress")]
    back4app_emails = [r.get("EmailAddress", "").lower().strip() for r in chunk if r.get("EmailAddress")]
    
    # Fetch existing records from both platforms
    bubble_map = await get_records_by_emails(session, bubble_emails)
    back4app_map = await get_records_by_emails_back4app(session, back4app_emails)
    
    # Process each row for both platforms
    tasks = [handle_row(row, bubble_map, back4app_map, session, sem) for row in chunk]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Task failed: {result}")

async def main_async(rows):
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        for idx in range(0, len(rows), CHUNK_SIZE):
            chunk = rows[idx : idx + CHUNK_SIZE]
            await process_chunk(chunk, session, sem)

# Google Cloud Function Entry Point

async def fetch_csv_from_url(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            raw = await resp.read()

        try:
            return raw.decode('utf-8')
        except UnicodeDecodeError:
            try:
                return raw.decode('cp1252')
            except UnicodeDecodeError:
                # last resort: keep all bytes, replacing invalid sequences
                return raw.decode('utf-8', errors='replace')
            
    except aiohttp.ClientError as e:
        logger.error(f"Failed to fetch CSV from {url}: {e}")
        raise

def main(request):
    try:
        # Check for the 'bubble' header with value 'X'
        if request.headers.get('bubble') != 'eafe2749ca27a1c37ccf000431c2d083':
            logger.error("Unauthorized request: missing or invalid 'bubble' header")
            return "Unauthorized: missing or invalid 'bubble' header", 401
        
        # Expect JSON payload with 'csvfile' key
        content_type = request.headers.get('Content-Type', '')
        if 'application/json' not in content_type.lower():
            return "Content-Type must be application/json", 400
        
        request_json = request.get_json()
        if not isinstance(request_json, dict) or 'csvfile' not in request_json:
            return "Request body must be JSON with a 'csvfile' key", 400
        
        csv_url = request_json['csvfile']
        if not isinstance(csv_url, str) or not csv_url.strip():
            return "Invalid or empty URL", 400

        # Fetch CSV content from URL
        async def process_csv():
            async with aiohttp.ClientSession() as session:
                content = await fetch_csv_from_url(session, csv_url)
                text_stream = io.StringIO(content)
                reader = csv.DictReader(text_stream)
                rows = [row for row in reader if any(row.values())]
                logger.info(f"Starting processing of {len(rows)} rows")
                await main_async(rows)
        
        asyncio.run(process_csv())

        logger.info(f"Processing completed — Bubble: {new_records_count} new, "
                    f"{updated_records_count} updated. Back4App: {new_records_back4app_count} new, "
                    f"{updated_records_back4app_count} updated.")
        
        return "Processing completed", 200
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return "Internal server error", 500

# Cloud Run entry point
if __name__ == "__main__":
    try:
        from functions_framework import create_app
        logger.info("Starting Cloud Run server...")
        app = create_app(main)
        port = int(os.environ.get("PORT", 8080))
        logger.info(f"Server will listen on port {port}")
        app.run(host="0.0.0.0", port=port, debug=False)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)