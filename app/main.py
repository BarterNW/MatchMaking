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

# Mount static files
# static_path = os.path.join(os.path.dirname(__file__), "..", "static")
# if os.path.exists(static_path):
#     app.mount("/static", StaticFiles(directory=static_path), name="static")

# # Templates
# templates_path = os.path.join(os.path.dirname(__file__), "..", "templates")
# templates = Jinja2Templates(directory=templates_path)


# @app.get("/", response_class=HTMLResponse)
# async def read_root(request: Request):
#     """Serve the main HTML page."""
#     return templates.TemplateResponse("index.html", {"request": request})


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
async def get_sponsors():
    """Get all brands (sponsors) for dropdown."""
    try:
        sponsors = get_brands_list()
        return {"sponsors": sponsors}
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

@app.get("/Api/v1/brands/search/{sponsor_id}")
async def get_sponsor_matches(sponsor_id: int):
    """Get matched events for a specific sponsor (brand)."""
    try:
        result = get_matches_for_brand(sponsor_id)
        result["sponsor_name"] = result.get("brand_name", "Unknown")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/Api/v1/events/search/{event_id}")
async def get_event_matches(event_id: int):
    """Get matched brands for a specific event."""
    try:
        result = get_matches_for_event(event_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
