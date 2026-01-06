"""
RIS Scraper API Service

Provides RESTful API endpoints to query scraped address data.

Endpoints:
    GET /              - Health check
    GET /records       - Query house number records
    GET /records/{id}  - Get single record by ID
    GET /stats         - Get statistics
    GET /alerts        - Query alerts

Logs & Monitoring:
    Grafana + Loki     - http://localhost:3000/d/ris-scraper-logs
"""

import os
import time
import logging
from datetime import datetime
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import json

# Setup API logging with Loki support
try:
    from loki_logger import get_loki_handler
    LOKI_AVAILABLE = True
except ImportError:
    LOKI_AVAILABLE = False

# Configure logger
api_logger = logging.getLogger("ris_api")
api_logger.setLevel(logging.INFO)
if not api_logger.handlers:
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    api_logger.addHandler(console_handler)
    # Loki handler
    if LOKI_AVAILABLE:
        loki_handler = get_loki_handler(job_name="api")
        if loki_handler:
            api_logger.addHandler(loki_handler)

# Custom JSON response with UTF-8 encoding for Chinese characters
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"

    def render(self, content) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,  # Allow Chinese characters
            indent=2
        ).encode("utf-8")

# Load environment variables
load_dotenv()

# =============================================================================
# Configuration
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://a1000yun@localhost:5432/ris_scraper")

# =============================================================================
# Pydantic Models (Response Schemas)
# =============================================================================

class RecordResponse(BaseModel):
    """Single house number record response."""
    id: int
    city: str
    district: str
    full_address: Optional[str] = None
    village: Optional[str] = None          # 里/村
    neighborhood: Optional[str] = None     # 鄰
    road: Optional[str] = None
    section: Optional[str] = None
    lane: Optional[str] = None
    alley: Optional[str] = None
    number: Optional[str] = None
    floor: Optional[str] = None
    floor_dash: Optional[str] = None
    assignment_type: str
    assignment_date: Optional[str] = None
    assignment_date_roc: Optional[str] = None
    created_at: Optional[str] = None


class RecordsListResponse(BaseModel):
    """List of records with pagination info."""
    total: int
    page: int
    page_size: int
    records: List[RecordResponse]


class StatsResponse(BaseModel):
    """Statistics response."""
    total_records: int
    by_district: dict
    by_date: dict
    last_updated: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    database: str
    timestamp: str


# =============================================================================
# Database Helper
# =============================================================================

def get_db_connection():
    """
    Create and return a database connection.

    Returns:
        psycopg2 connection object with RealDictCursor
    """
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def log_api_query(request: Request, endpoint: str, city: str = None,
                  district: str = None, results_count: int = 0,
                  response_time_ms: float = 0, status_code: int = 200,
                  error_message: str = None):
    """
    Log API query to database for monitoring.

    This implements the requirement: "Log API queries"
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO api_query_logs
            (endpoint, method, city, district, results_count,
             response_time_ms, status_code, error_message, client_ip, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            endpoint,
            request.method,
            city,
            district,
            results_count,
            response_time_ms,
            status_code,
            error_message,
            request.client.host if request.client else None,
            request.headers.get("user-agent")
        ))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to log API query: {e}")


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="RIS Scraper API",
    description="API service for querying Taiwan house number registration data",
    version="1.0.0",
    docs_url="/docs",      # Swagger UI
    redoc_url="/redoc",    # ReDoc
    default_response_class=UTF8JSONResponse  # UTF-8 for Chinese
)

# CORS middleware (allow cross-origin requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.

    Returns API status and database connection status.
    """
    db_status = "disconnected"

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "running",
        "database": db_status,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/records", response_model=RecordsListResponse)
async def get_records(
    request: Request,
    city: Optional[str] = Query(None, description="Filter by city (e.g., 臺北市)"),
    district: Optional[str] = Query(None, description="Filter by district (e.g., 大安區)"),
    assignment_type: Optional[str] = Query(None, description="Filter by type (e.g., 門牌初編)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Records per page")
):
    """
    Query house number records with optional filters.

    This is the main endpoint for querying scraped data.
    Supports filtering by city, district, type, and date range.
    """
    start_time = time.time()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Build query with filters
        query = "SELECT * FROM house_number_records WHERE 1=1"
        count_query = "SELECT COUNT(*) FROM house_number_records WHERE 1=1"
        params = []

        if city:
            query += " AND city = %s"
            count_query += " AND city = %s"
            params.append(city)

        if district:
            query += " AND district = %s"
            count_query += " AND district = %s"
            params.append(district)

        if assignment_type:
            query += " AND assignment_type = %s"
            count_query += " AND assignment_type = %s"
            params.append(assignment_type)

        if start_date:
            query += " AND assignment_date >= %s"
            count_query += " AND assignment_date >= %s"
            params.append(start_date)

        if end_date:
            query += " AND assignment_date <= %s"
            count_query += " AND assignment_date <= %s"
            params.append(end_date)

        # Get total count
        cursor.execute(count_query, params)
        total = cursor.fetchone()["count"]

        # Add pagination
        offset = (page - 1) * page_size
        query += " ORDER BY id LIMIT %s OFFSET %s"
        params.extend([page_size, offset])

        # Execute query
        cursor.execute(query, params)
        rows = cursor.fetchall()

        # Format response
        records = []
        for row in rows:
            records.append({
                "id": row["id"],
                "city": row["city"],
                "district": row["district"],
                "full_address": row.get("full_address"),
                "village": row.get("village"),
                "neighborhood": row.get("neighborhood"),
                "road": row.get("road"),
                "section": row.get("section"),
                "lane": row.get("lane"),
                "alley": row.get("alley"),
                "number": row.get("number"),
                "floor": row.get("floor"),
                "floor_dash": row.get("floor_dash"),
                "assignment_type": row["assignment_type"],
                "assignment_date": str(row["assignment_date"]) if row.get("assignment_date") else None,
                "assignment_date_roc": row.get("assignment_date_roc"),
                "created_at": str(row["created_at"]) if row.get("created_at") else None
            })

        cursor.close()
        conn.close()

        response_time = (time.time() - start_time) * 1000

        # Log to Loki
        api_logger.info(f"GET /records city={city} district={district} results={len(records)} time={response_time:.1f}ms")

        # Log the query
        log_api_query(
            request=request,
            endpoint="/records",
            city=city,
            district=district,
            results_count=len(records),
            response_time_ms=response_time,
            status_code=200
        )

        # Send alert if query returns empty 
        if total == 0 and (city or district):
            alert_service.api_empty_result(
                city=city or "全部",
                district=district or "全部",
                metadata={
                    "city": city,
                    "district": district,
                    "assignment_type": assignment_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "client_ip": request.client.host if request.client else None
                }
            )

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "records": records
        }

    except Exception as e:
        response_time = (time.time() - start_time) * 1000
        log_api_query(
            request=request,
            endpoint="/records",
            city=city,
            district=district,
            results_count=0,
            response_time_ms=response_time,
            status_code=500,
            error_message=str(e)
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/records/{record_id}", response_model=RecordResponse)
async def get_record_by_id(record_id: int, request: Request):
    """
    Get a single record by ID.
    """
    start_time = time.time()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM house_number_records WHERE id = %s", (record_id,))
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail=f"Record {record_id} not found")

        response_time = (time.time() - start_time) * 1000
        log_api_query(
            request=request,
            endpoint=f"/records/{record_id}",
            results_count=1,
            response_time_ms=response_time,
            status_code=200
        )

        return {
            "id": row["id"],
            "city": row["city"],
            "district": row["district"],
            "full_address": row.get("full_address"),
            "village": row.get("village"),
            "neighborhood": row.get("neighborhood"),
            "road": row.get("road"),
            "section": row.get("section"),
            "lane": row.get("lane"),
            "alley": row.get("alley"),
            "number": row.get("number"),
            "floor": row.get("floor"),
            "floor_dash": row.get("floor_dash"),
            "assignment_type": row["assignment_type"],
            "assignment_date": str(row["assignment_date"]) if row.get("assignment_date") else None,
            "assignment_date_roc": row.get("assignment_date_roc"),
            "created_at": str(row["created_at"]) if row.get("created_at") else None
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", response_model=StatsResponse)
async def get_statistics(request: Request):
    """
    Get statistics about the scraped data.

    Returns:
        - Total record count
        - Records by district
        - Records by date
        - Last update time
    """
    start_time = time.time()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Total count
        cursor.execute("SELECT COUNT(*) FROM house_number_records")
        total = cursor.fetchone()["count"]

        # By district
        cursor.execute("""
            SELECT district, COUNT(*) as count
            FROM house_number_records
            GROUP BY district
            ORDER BY count DESC
        """)
        by_district = {row["district"]: row["count"] for row in cursor.fetchall()}

        # By date
        cursor.execute("""
            SELECT assignment_date, COUNT(*) as count
            FROM house_number_records
            WHERE assignment_date IS NOT NULL
            GROUP BY assignment_date
            ORDER BY assignment_date DESC
            LIMIT 10
        """)
        by_date = {str(row["assignment_date"]): row["count"] for row in cursor.fetchall()}

        # Last updated
        cursor.execute("SELECT MAX(created_at) FROM house_number_records")
        last_updated = cursor.fetchone()["max"]

        cursor.close()
        conn.close()

        response_time = (time.time() - start_time) * 1000
        log_api_query(
            request=request,
            endpoint="/stats",
            results_count=1,
            response_time_ms=response_time,
            status_code=200
        )

        return {
            "total_records": total,
            "by_district": by_district,
            "by_date": by_date,
            "last_updated": str(last_updated) if last_updated else None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Alert Endpoints
# =============================================================================

# Import alert service
from alert_service import alert_service, AlertType, AlertSeverity


class AlertResponse(BaseModel):
    """Alert record response."""
    id: int
    alert_type: str
    severity: str
    title: str
    message: str
    sent_at: Optional[str] = None
    status: Optional[str] = None


class AlertsListResponse(BaseModel):
    """List of alerts response."""
    total: int
    alerts: List[AlertResponse]


class AlertStatsResponse(BaseModel):
    """Alert statistics response."""
    total: int
    by_type: dict
    by_severity: dict
    last_24_hours: int


@app.get("/alerts", response_model=AlertsListResponse)
async def get_alerts(
    request: Request,
    alert_type: Optional[str] = Query(None, description="Filter by type"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    limit: int = Query(50, ge=1, le=200, description="Max alerts to return")
):
    """
    Get alert notifications.

    This endpoint returns recorded alerts for monitoring.
    """
    try:
        alerts = alert_service.get_alerts(
            limit=limit,
            alert_type=alert_type,
            severity=severity
        )

        return {
            "total": len(alerts),
            "alerts": [
                {
                    "id": a["id"],
                    "alert_type": a["alert_type"],
                    "severity": a["severity"],
                    "title": a["title"],
                    "message": a["message"],
                    "sent_at": str(a["sent_at"]) if a.get("sent_at") else None,
                    "status": a.get("status")
                }
                for a in alerts
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/alerts/stats", response_model=AlertStatsResponse)
async def get_alert_stats(request: Request):
    """
    Get alert statistics.

    Returns counts by type, severity, and recent alerts.
    """
    try:
        stats = alert_service.get_alert_stats()

        return {
            "total": stats.get("total", 0),
            "by_type": stats.get("by_type", {}),
            "by_severity": stats.get("by_severity", {}),
            "last_24_hours": stats.get("last_24_hours", 0)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("RIS Scraper API Server")
    print("=" * 60)
    print("Starting server at http://localhost:8000")
    print("")
    print("API Documentation: http://localhost:8000/docs")
    print("Logs (Grafana):    http://localhost:3000/d/ris-scraper-logs")
    print("=" * 60)

    uvicorn.run(app, host="0.0.0.0", port=8000)
