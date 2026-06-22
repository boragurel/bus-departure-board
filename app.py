import asyncio
import logging
import logging.handlers
import os
import json
import time
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI, Request
from pathlib import Path
from fastapi.responses import FileResponse, StreamingResponse

BASE_DIR = Path(__file__).parent

# ===========================================
# LOGGING
# ===========================================

logger = logging.getLogger("bus-board")
logger.setLevel(logging.INFO)

# Rotating file: 1MB max, keep 3 old files
fh = logging.handlers.RotatingFileHandler(
    BASE_DIR / "bus-board.log",
    maxBytes=1_000_000,
    backupCount=3,
)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(fh)

# Also log to stdout (captured by journalctl)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(sh)

# ===========================================
# CONFIGURATION
# ===========================================

TFL_BASE = "https://api.tfl.gov.uk"
POLL_INTERVAL = 5  # seconds
COUNTDOWN_THRESHOLD = 30  # seconds
BST = timezone(timedelta(hours=1))

TFL_API_KEY = os.environ["TFL_API_KEY"]

SECTIONS = [
    {
        "title": "Towards Work / School",
        "stops": [
            {
                "name": "Carshalton Beeches Station",
                "naptanIds": ["490001050S"],
                "showRoute": True,
                "maxArrivals": 2,
            },
            {
                "name": "Cambridge Rd / Banstead Rd",
                "naptanIds": ["490020254N"],
                "showRoute": True,
                "maxArrivals": 2,
            },
            {
                "name": "Downside Road",
                "naptanIds": ["490006181S"],
                "showRoute": True,
                "maxArrivals": 2,
            },
            {
                "name": "Wales Avenue",
                "naptanIds": ["490014129N"],
                "showRoute": True,
                "maxArrivals": 2,
            },
        ],
    },
    {
        "title": "Towards Home",
        "stops": [
            {
                "name": "Royal Marsden Hospital",
                "naptanIds": ["490011767L", "490011768E"],
                "showRoute": True,
                "maxArrivals": 4,
            },
            {
                "name": "Sutton / Marshall's Road",
                "naptanIds": ["490013061E"],
                "showRoute": True,
                "maxArrivals": 2,
                "filterLines": ["154"],
            },
            {
                "name": "Park Lane / Fairfield Halls",
                "naptanIds": ["490006706ZZ"],
                "showRoute": True,
                "maxArrivals": 2,
                "filterLines": ["154"],
            },
        ],
    },
]


# ===========================================
# STATE
# ===========================================

app = FastAPI()
connected_clients: set = set()
latest_data: dict | None = None
last_good_arrivals: dict = {}   # stop_name -> arrivals list
empty_count: dict = {}          # stop_name -> consecutive empty poll count


# ===========================================
# TFL FETCHING
# ===========================================

async def fetch_stop_arrivals(client: httpx.AsyncClient, naptan_id: str) -> list:
    """Fetch arrivals for a single NAPTAN."""
    try:
        r = await client.get(
            f"{TFL_BASE}/StopPoint/{naptan_id}/Arrivals",
            params={"app_key": TFL_API_KEY},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        return r.json()
    except Exception as e:
        logger.warning(f"Stop {naptan_id} fetch failed: {e}")
        return []


async def fetch_vehicle_arrivals(client: httpx.AsyncClient, vehicle_id: str) -> list:
    """Fetch all remaining stops for a vehicle."""
    try:
        r = await client.get(
            f"{TFL_BASE}/Vehicle/{vehicle_id}/Arrivals",
            params={"app_key": TFL_API_KEY},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        return r.json()
    except Exception as e:
        logger.warning(f"Vehicle {vehicle_id} fetch failed: {e}")
        return []


def get_tracking_info(vehicle_stops: list, naptan_ids: list) -> str | None:
    """Calculate how far a bus is from our stop."""
    sorted_stops = sorted(vehicle_stops, key=lambda s: s["timeToStation"])
    our_index = next(
        (i for i, s in enumerate(sorted_stops) if s["naptanId"] in naptan_ids),
        -1,
    )
    if our_index == -1:
        return None
    if our_index == 0:
        if sorted_stops[0]["timeToStation"] <= COUNTDOWN_THRESHOLD:
            return "arriving"
        return "next stop"
    if our_index == 1:
        return "1 stop away"
    return f"{our_index} stops away"


async def poll_tfl() -> dict:
    """Fetch all stops and assemble the full board data."""
    fetched_at = int(time.time() * 1000)  # ms timestamp for JS compatibility
    results = []

    async with httpx.AsyncClient() as client:
        for section in SECTIONS:
            section_result = {"title": section["title"], "stops": []}

            for stop in section["stops"]:
                try:
                    # Fetch arrivals from all NAPTANs in parallel
                    naptan_results = await asyncio.gather(
                        *[fetch_stop_arrivals(client, nid) for nid in stop["naptanIds"]]
                    )
                    all_arrivals = [a for sublist in naptan_results for a in sublist]

                    # Filter by line if specified
                    filter_lines = stop.get("filterLines")
                    if filter_lines:
                        all_arrivals = [
                            a for a in all_arrivals if a["lineName"] in filter_lines
                        ]

                    # Deduplicate by vehicleId, keeping lowest timeToStation
                    best_by_vehicle = {}
                    for a in all_arrivals:
                        vid = a["vehicleId"]
                        if vid not in best_by_vehicle or a["timeToStation"] < best_by_vehicle[vid]["timeToStation"]:
                            best_by_vehicle[vid] = a
                    unique = list(best_by_vehicle.values())

                    # Sort by displayed minute then vehicleId for stable ordering
                    def sort_key(a):
                        displayed_min = (a["timeToStation"] + 30) // 60
                        return (displayed_min, a["vehicleId"])
                    unique.sort(key=sort_key)
                    top = unique[: stop["maxArrivals"]]

                    # Persistence: keep last good data for up to 3 empty polls
                    stop_name = stop["name"]
                    if top:
                        last_good_arrivals[stop_name] = top
                        empty_count[stop_name] = 0
                    else:
                        empty_count[stop_name] = empty_count.get(stop_name, 0) + 1
                        if empty_count[stop_name] < 3 and stop_name in last_good_arrivals:
                            top = last_good_arrivals[stop_name]

                    # Fetch vehicle tracking in parallel
                    if top:
                        vehicle_results = await asyncio.gather(
                            *[fetch_vehicle_arrivals(client, bus["vehicleId"]) for bus in top]
                        )
                        for bus, v_stops in zip(top, vehicle_results):
                            bus["tracking"] = get_tracking_info(v_stops, stop["naptanIds"])

                    section_result["stops"].append(
                        {
                            "stop": {
                                "name": stop["name"],
                                "showRoute": stop["showRoute"],
                            },
                            "arrivals": [
                                {
                                    "lineName": bus["lineName"],
                                    "destinationName": bus["destinationName"],
                                    "vehicleId": bus["vehicleId"],
                                    "timeToStation": bus["timeToStation"],
                                    "tracking": bus.get("tracking"),
                                }
                                for bus in top
                            ],
                        }
                    )
                except Exception as e:
                    logger.error(f"Stop {stop['name']} failed: {e}")
                    section_result["stops"].append(
                        {
                            "stop": {
                                "name": stop["name"],
                                "showRoute": stop["showRoute"],
                            },
                            "arrivals": [],
                        }
                    )

            results.append(section_result)

    # Section order: work first before noon BST, home first after noon
    now_bst = datetime.now(BST)
    if now_bst.hour >= 12:
        results = list(reversed(results))

    return {"fetchedAt": fetched_at, "sections": results}


# ===========================================
# BACKGROUND POLLER
# ===========================================

async def poller():
    """Poll TfL every POLL_INTERVAL seconds, but only when clients are connected."""
    global latest_data
    was_polling = False
    while True:
        if connected_clients:
            if not was_polling:
                logger.info(f"Polling started ({len(connected_clients)} client(s))")
                was_polling = True
            try:
                start = time.time()
                latest_data = await poll_tfl()
                elapsed = time.time() - start
                total_buses = sum(
                    len(s["arrivals"])
                    for sec in latest_data["sections"]
                    for s in sec["stops"]
                )
                logger.info(f"Poll OK: {elapsed:.1f}s, {total_buses} buses, {len(connected_clients)} client(s)")
            except Exception as e:
                logger.error(f"Poll failed: {e}")
        else:
            if was_polling:
                logger.info("Polling stopped (no clients)")
                was_polling = False
        await asyncio.sleep(POLL_INTERVAL)


@app.on_event("startup")
async def start_poller():
    logger.info("Bus board server starting")
    asyncio.create_task(poller())


# ===========================================
# SSE ENDPOINT
# ===========================================

@app.get("/events")
async def sse(request: Request):
    client_id = id(request)

    async def event_stream():
        connected_clients.add(client_id)
        logger.info(f"Client connected ({len(connected_clients)} total)")
        last_sent = None
        try:
            while True:
                if await request.is_disconnected():
                    break
                if latest_data is not None and latest_data is not last_sent:
                    payload = json.dumps(latest_data)
                    yield f"data: {payload}\n\n"
                    last_sent = latest_data
                await asyncio.sleep(0.5)
        finally:
            connected_clients.discard(client_id)
            logger.info(f"Client disconnected ({len(connected_clients)} remaining)")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ===========================================
# STATIC PAGE
# ===========================================

@app.get("/")
@app.get("/bus-board.html")
async def serve_board():
    return FileResponse(BASE_DIR / "bus-board.html", media_type="text/html")


# ===========================================
# HEALTH CHECK
# ===========================================

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "clients": len(connected_clients),
        "has_data": latest_data is not None,
        "last_fetch": latest_data["fetchedAt"] if latest_data else None,
    }
