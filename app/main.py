from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.requests import Request
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import database functions (PostgreSQL-based)
from database import get_brands_list, close_pool, get_connection, get_events_list
from matching import get_matches_for_brand, get_matches_for_event


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    close_pool()
    print("Database connection pool closed.")


app = FastAPI(title="Sponsorship Matching System", lifespan=lifespan)

# Paths relative to project root (parent of app/)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
static_path = os.path.join(BASE_DIR, "static")
templates_path = os.path.join(BASE_DIR, "templates")

# Mount static files first so /static/* is served
if os.path.isdir(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")
else:
    print("Warning: static directory not found at", static_path)

templates = Jinja2Templates(directory=templates_path)


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the main HTML page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health():
    """Health check: verifies database connectivity."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")


@app.get("/api/brands")
async def get_brands():
    """Get all brands (brands) for dropdown."""
    try:
        brands = get_brands_list()
        return {"brands": brands}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/api/events")
async def get_events():
    """Get all events for dropdown."""
    try:
        events = get_events_list()
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/api/brands/{brand_id}/matches")
async def get_brand_matches(brand_id: int):
    """Get matched events for a specific brand (brand)."""
    try:
        result = get_matches_for_brand(brand_id)
        if result is None:
            result = {"brand_org_id": brand_id, "brand_name": "Unknown", "matches": []}
        result["brand_name"] = result.get("brand_name", "Unknown")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/events/{event_id}/matches")
async def get_event_matches(event_id: int):
    """Get matched brands for a specific event."""
    try:
        result = get_matches_for_event(event_id)
        if result is None:
            result = {"event_org_id": event_id, "event_name": "Unknown", "matches": []}
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

