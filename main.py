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
from flask import Flask, request, jsonify, send_file, send_from_directory

# Counters for Bubble
new_records_count = 0
updated_records_count = 0

# Counters for Back4App
new_records_back4app_count = 0
updated_records_back4app_count = 0

# Setup logging
def setup_logging():
    """Configure application logging for Cloud Run"""
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler for stdout (Cloud Run requirement)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create detailed formatter for application logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    # Create application logger
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(logging.INFO)
    
    return app_logger

# Initialize logger
logger = setup_logging()

# Initialize Flask app
app = Flask(__name__)

# Configure Flask logging
app.logger.setLevel(logging.INFO)

# Add request logging middleware
@app.before_request
def log_request_info():
    logger.info(f"Request: {request.method} {request.url} from {request.remote_addr}")

@app.after_request
def log_response_info(response):
    logger.info(f"Response: {response.status_code} for {request.method} {request.url}")
    return response

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
BACK4APP_CSV_TABLE_NAME = "Prelicensingcsv"

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

def parse_record_date(s) -> datetime:
    # Se for um dicionário do Back4App com formato de data
    if isinstance(s, dict) and "__type" in s and s["__type"] == "Date":
        s = s["iso"]
    
    # Se for string, processar normalmente
    if isinstance(s, str) and s.strip():
        try:
            dt_aware = datetime.fromisoformat(s.replace("Z", "+00:00"))
            dt_utc = dt_aware.astimezone(timezone.utc)
            return dt_utc
        except ValueError as e:
            # Tentar parsing alternativo com dateutil
            try:
                import dateutil.parser
                dt = dateutil.parser.parse(s)
                return dt.astimezone(timezone.utc)
            except Exception:
                raise ValueError(f"Erro ao parsear data: {s} - {e}")
    
    # Se não for nem dict nem string válida, lançar erro
    raise ValueError(f"Tipo de data não suportado: {type(s)} - {s}")

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
        "UserPreLicensingEMAIL": email,  # ✅ Campo correto do Bubble
        # Removido first_name - campo não reconhecido pela API do Bubble
        # Removido last_name - campo não reconhecido pela API do Bubble
        # Removido phone - campo não reconhecido pela API do Bubble
        # Removido imo - campo não reconhecido pela API do Bubble
        **({"date_enrolled": to_utc_iso(enrolled)} if enrolled else {}),
        # Removido pre_licensing_course_last_login - campo não reconhecido pela API do Bubble
        # Removido time_spent_in_course - campo não reconhecido pela API do Bubble
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
        "first_name_text": row.get("FirstName") or "",
        "last_name_text": row.get("LastName") or "",
        "pre_licensing_email_text": email,
        "phone_text": phone or "",
        "imo_custom_imo": row.get("Department") or "",
        "hiring_manager_text": row.get("HiringManager") or "",
        "pre_licensing_course_text": row.get("Course") or "",
        "prepared_to_pass_text": row.get("Prepared to Pass") or "",
        "time_spent_text": row.get("TimeSpent") or "",
        **({"date_enrolled_date": {"__type": "Date", "iso": to_utc_iso(enrolled)}} if enrolled else {}),
        **({"pre_licensing_course_last_login_date": {"__type": "Date", "iso": to_utc_iso(logged)}} if logged else {}),
        **({"ple_date_completed_date": {"__type": "Date", "iso": to_utc_iso(completed)}} if completed else {}),
        **({"ple_complete_number": ple_complete} if ple_complete is not None else {}),
        **({"percentage_prep_complete_number": prep_complete} if prep_complete is not None else {}),
        **({"percentage_sim_complete_number": sim_complete} if sim_complete is not None else {}),
    }
    # Retornar todos os campos, mesmo os vazios, para garantir dados completos
    return payload

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
    return {r.get("UserPreLicensingEMAIL", "").lower().strip(): r for r in results}

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
        logger.info(f"[CREATE SUCCESS] {payload.get('UserPreLicensingEMAIL')}")
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

# CSV Files Management Functions
async def save_csv_file(session: aiohttp.ClientSession, csv_url: str, csv_content: str, filename: str) -> str:
    """Save CSV file to Prelicensingcsv table and return the objectId"""
    url = f"{BACK4APP_API_BASE_URL}/{BACK4APP_CSV_TABLE_NAME}"
    
    # For large files, save only URL and queue for processing
    if len(csv_content) > 100000:  # 100KB threshold
        logger.info(f"[CSV QUEUE] Large file detected, queuing for background processing: {filename}")
        payload = {
            "filename": filename,
            "csv_url": csv_url,
            "csv_content": "",  # Empty - will be processed in background
            "file_size": 0,
            "total_records": 0,
            "processed_records": 0,
            "processing_status": "queued",
            "source_email": "google_apps_script",
            "imo": "",
            "queue_priority": 1,
            "created_at": {"__type": "Date", "iso": to_utc_iso(datetime.now())}
        }
    else:
        # Small files - process immediately
        imo = ""
        if csv_content:
            lines = csv_content.split('\n')
            if len(lines) > 1:
                try:
                    reader = csv.DictReader(io.StringIO(csv_content))
                    first_row = next(reader, None)
                    if first_row:
                        imo = first_row.get("Department", "")
                except Exception as e:
                    logger.warning(f"Could not extract IMO from CSV: {e}")
        
        payload = {
            "filename": filename,
            "csv_url": csv_url,
            "csv_content": csv_content,
            "file_size": len(csv_content),
            "total_records": csv_content.count('\n') - 1 if csv_content else 0,
            "processed_records": 0,
            "processing_status": "pending",
            "source_email": "google_apps_script",
            "imo": imo
        }
    
    logger.info(f"[CSV SAVE] Saving CSV file: {filename}")
    try:
        async with session.post(url, headers=BACK4APP_HEADERS, json=payload) as resp:
            body = await resp.text()
            logger.info(f"[CSV SAVE RESPONSE] status={resp.status}, body={body}")
            resp.raise_for_status()
            data = json.loads(body)
            csv_file_id = data.get("objectId")
            logger.info(f"[CSV SAVE SUCCESS] CSV file saved with ID: {csv_file_id}")
            return csv_file_id
    except Exception as e:
        logger.error(f"[CSV SAVE FAILED] {filename} → {e}")
        raise

async def update_csv_file_status(session: aiohttp.ClientSession, csv_file_id: str, status: str, processed_records: int = 0, error_message: str = ""):
    """Update CSV file processing status"""
    url = f"{BACK4APP_API_BASE_URL}/{BACK4APP_CSV_TABLE_NAME}/{csv_file_id}"
    
    payload = {
        "processing_status": status,
        "processed_records": processed_records
    }
    
    if error_message:
        payload["error_message"] = error_message
    
    if status == "completed":
        payload["processed_at"] = {"__type": "Date", "iso": to_utc_iso(datetime.now())}
    
    logger.info(f"[CSV UPDATE] Updating CSV file {csv_file_id} status to {status}")
    try:
        async with session.put(url, headers=BACK4APP_HEADERS, json=payload) as resp:
            body = await resp.text()
            logger.info(f"[CSV UPDATE RESPONSE] status={resp.status}, body={body}")
            resp.raise_for_status()
            logger.info(f"[CSV UPDATE SUCCESS] CSV file {csv_file_id} updated")
    except Exception as e:
        logger.error(f"[CSV UPDATE FAILED] {csv_file_id} → {e}")
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
    
    # Process Back4App FIRST (independente do Bubble)
    back4app_payload = to_back4app_payload(row)
    back4app_email = back4app_payload.get("pre_licensing_email_text")
    
    async with sem:
        # Process Back4App FIRST
        try:
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
                # Sempre atualizar se houver dados válidos (removida lógica de comparação de data)
                update_fields = [
                    "first_name_text",
                    "last_name_text",
                    "pre_licensing_email_text",
                    "phone_text",
                    "imo_custom_imo",
                    "hiring_manager_text",
                    "pre_licensing_course_text",
                    "prepared_to_pass_text",
                    "time_spent_text",
                    "date_enrolled_date",
                    "pre_licensing_course_last_login_date",
                    "ple_date_completed_date",
                    "ple_complete_number",
                    "percentage_prep_complete_number",
                    "percentage_sim_complete_number"
                ]
                upd = {k: back4app_payload[k] for k in update_fields if k in back4app_payload}
                if upd:  # Só atualizar se houver campos para atualizar
                    rid = back4app_existing.get("objectId")
                    await update_record_back4app(session, rid, upd, str(back4app_email))
                    updated_records_back4app_count += 1
                    logger.info(f"[BACK4APP UPDATED] {back4app_email} — changes: {upd}")
                else:
                    logger.debug(f"[BACK4APP SKIPPED] {back4app_email} — no changes needed")
            else:
                await create_record_back4app(session, back4app_payload)
                new_records_back4app_count += 1
                logger.info(f"[BACK4APP CREATED] {back4app_email}")
        except Exception as e:
            logger.error(f"[BACK4APP ERROR] {back4app_email} — {e}")
            # Continue processing even if Back4App fails
        
        # Process Bubble SECOND (independente do Back4App)
        try:
            bubble_payload = to_payload(row)
            bubble_email = bubble_payload.get("UserPreLicensingEMAIL")
            
            bubble_existing = bubble_map.get(bubble_email) if bubble_email else None
            
            # Parse DB "last login" if present (campo removido do Bubble)
            bubble_db_val = None  # Campo não existe mais no Bubble
            bubble_db_dt = None

            # Parse CSV "last login" if present (campo removido do Bubble)
            bubble_csv_val = None  # Campo não existe mais no Bubble
            bubble_csv_dt = None
            
            # Process Bubble record
            if bubble_existing:
                # Sempre atualizar se houver dados válidos (removida lógica de comparação de data)
                update_fields = [
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
                if upd:  # Só atualizar se houver campos para atualizar
                    rid = bubble_existing.get("_id") or bubble_existing.get("id")
                    await update_record(session, rid, upd, str(bubble_email))
                    updated_records_count += 1
                    logger.info(f"[BUBBLE UPDATED] {bubble_email} — changes: {upd}")
                else:
                    logger.debug(f"[BUBBLE SKIPPED] {bubble_email} — no changes needed")
            else:
                await create_record(session, bubble_payload)
                new_records_count += 1
                logger.info(f"[BUBBLE CREATED] {bubble_email}")
        except Exception as e:
            logger.error(f"[BUBBLE ERROR] {bubble_email if 'bubble_email' in locals() else 'unknown'} — {e}")
            # Continue processing even if Bubble fails

async def process_chunk(chunk, session, sem):
    # Extract emails for both platforms
    bubble_emails = [r.get("EmailAddress", "").lower().strip() for r in chunk if r.get("EmailAddress")]
    back4app_emails = [r.get("EmailAddress", "").lower().strip() for r in chunk if r.get("EmailAddress")]
    
    # Fetch existing records from both platforms (independente)
    bubble_map = {}
    back4app_map = {}
    
    try:
        bubble_map = await get_records_by_emails(session, bubble_emails)
        logger.info(f"[BUBBLE] Loaded {len(bubble_map)} existing records")
    except Exception as e:
        logger.error(f"[BUBBLE] Failed to load existing records: {e}")
        # Continue without Bubble data
    
    try:
        back4app_map = await get_records_by_emails_back4app(session, back4app_emails)
        logger.info(f"[BACK4APP] Loaded {len(back4app_map)} existing records")
    except Exception as e:
        logger.error(f"[BACK4APP] Failed to load existing records: {e}")
        # Continue without Back4App data
    
    # Process each row for both platforms
    tasks = [handle_row(row, bubble_map, back4app_map, session, sem) for row in chunk]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Task failed: {result}")
            # Continue processing other rows

async def main_async(rows, csv_url: str = "", csv_filename: str = ""):
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT)
    csv_file_id = None
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Step 1: Save CSV file to CSV_Files table if URL provided
        if csv_url and csv_filename:
            try:
                csv_content = await fetch_csv_from_url(session, csv_url)
                csv_file_id = await save_csv_file(session, csv_url, csv_content, csv_filename)
                await update_csv_file_status(session, csv_file_id, "processing")
            except Exception as e:
                logger.error(f"[CSV SAVE ERROR] Failed to save CSV file: {e}")
                if csv_file_id:
                    await update_csv_file_status(session, csv_file_id, "error", error_message=str(e))
                return
        
        # Step 2: Process rows in chunks
        total_processed = 0
        try:
            for idx in range(0, len(rows), CHUNK_SIZE):
                chunk = rows[idx : idx + CHUNK_SIZE]
                await process_chunk(chunk, session, sem)
                total_processed += len(chunk)
                
                # Update progress
                if csv_file_id:
                    await update_csv_file_status(session, csv_file_id, "processing", total_processed)
            
            # Mark as completed
            if csv_file_id:
                await update_csv_file_status(session, csv_file_id, "completed", total_processed)
                
        except Exception as e:
            logger.error(f"[PROCESSING ERROR] Failed to process rows: {e}")
            if csv_file_id:
                await update_csv_file_status(session, csv_file_id, "error", total_processed, str(e))
            raise

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

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    return jsonify({"status": "healthy", "message": "Service is running"}), 200

# Static file routes for frontend
@app.route('/', methods=['GET'])
def serve_frontend():
    """Serve the main frontend page"""
    logger.info("Serving frontend page")
    return send_file('index.html')

@app.route('/script.js')
def serve_script():
    """Serve JavaScript file"""
    logger.info("Serving JavaScript file")
    return send_file('script.js', mimetype='application/javascript')

@app.route('/styles.css')
def serve_styles():
    """Serve CSS file"""
    logger.info("Serving CSS file")
    return send_file('styles.css', mimetype='text/css')

@app.route('/', methods=['POST'])
def process_csv_endpoint():
    """Main CSV processing endpoint"""
    logger.info("CSV processing endpoint called")
    try:
        # Reset counters
        global new_records_count, updated_records_count, new_records_back4app_count, updated_records_back4app_count
        new_records_count = 0
        updated_records_count = 0
        new_records_back4app_count = 0
        updated_records_back4app_count = 0
        
        # Check for the 'bubble' header with value 'X'
        if request.headers.get('bubble') != 'eafe2749ca27a1c37ccf000431c2d083':
            logger.error("Unauthorized request: missing or invalid 'bubble' header")
            return jsonify({"error": "Unauthorized: missing or invalid 'bubble' header"}), 401
        
        # Expect JSON payload with 'csvfile' key
        content_type = request.headers.get('Content-Type', '')
        if 'application/json' not in content_type.lower():
            return jsonify({"error": "Content-Type must be application/json"}), 400
        
        request_json = request.get_json()
        if not isinstance(request_json, dict) or 'csvfile' not in request_json:
            return jsonify({"error": "Request body must be JSON with a 'csvfile' key"}), 400
        
        csv_url = request_json['csvfile']
        if not isinstance(csv_url, str) or not csv_url.strip():
            return jsonify({"error": "Invalid or empty URL"}), 400

        logger.info(f"Processing CSV from URL: {csv_url}")

        # Fetch CSV content from URL
        async def process_csv():
            async with aiohttp.ClientSession() as session:
                logger.info("Fetching CSV content from URL")
                content = await fetch_csv_from_url(session, csv_url)
                text_stream = io.StringIO(content)
                reader = csv.DictReader(text_stream)
                rows = [row for row in reader if any(row.values())]
                
                # Extract filename from URL or use default
                filename = csv_url.split('/')[-1] if '/' in csv_url else f"csv_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                
                logger.info(f"Starting processing of {len(rows)} rows from {filename}")
                await main_async(rows, csv_url, filename)
        
        asyncio.run(process_csv())

        logger.info(f"Processing completed — Bubble: {new_records_count} new, "
                    f"{updated_records_count} updated. Back4App: {new_records_back4app_count} new, "
                    f"{updated_records_back4app_count} updated.")
        
        return jsonify({
            "message": "Processing completed",
            "bubble": {"new": new_records_count, "updated": updated_records_count},
            "back4app": {"new": new_records_back4app_count, "updated": updated_records_back4app_count}
        }), 200
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

# Cloud Run entry point
if __name__ == "__main__":
    try:
        logger.info("=" * 50)
        logger.info("Starting CSV Processor Service")
        logger.info("=" * 50)
        logger.info("Configuration loaded:")
        logger.info(f"  - Bubble API: {API_BASE_URL}")
        logger.info(f"  - Table: {TABLE_NAME}")
        logger.info(f"  - Back4App API: {BACK4APP_API_BASE_URL}")
        logger.info(f"  - Max Concurrent: {MAX_CONCURRENT}")
        logger.info(f"  - Chunk Size: {CHUNK_SIZE}")
        logger.info("=" * 50)
        
        port = int(os.environ.get("PORT", 8080))
        logger.info(f"Starting Flask server on port {port}")
        app.run(host="0.0.0.0", port=port, debug=False)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)