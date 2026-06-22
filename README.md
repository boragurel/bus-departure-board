# Bus Departure Board

A live bus departure board for south London (Carshalton/Sutton area), showing real-time TfL bus arrivals.

Live at **https://bus.thegurels.uk**

## What it does

Polls the TfL Unified API every 5 seconds and pushes arrival data to connected browsers via Server-Sent Events. Seven stops across two sections (towards work and towards home), with section order swapping at noon.

The board only polls TfL when someone is viewing the page, so it uses no API quota when idle.

## Architecture

```
browser <--SSE-- FastAPI/Uvicorn --poll--> TfL API
                      ^
                      |
           Cloudflare Tunnel (HTTPS)
```

A single Python process (FastAPI + Uvicorn) handles everything: serving the HTML page, polling TfL, and streaming updates to clients. Cloudflare Tunnel provides HTTPS with no public ports, no certificates to manage, and no reverse proxy.

## Setup

Requires Python 3.10+ and a TfL API key (free from https://api-portal.tfl.gov.uk/).

```bash
git clone https://github.com/boragurel/bus-departure-board.git
cd bus-departure-board
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file with your TfL API key:

```
TFL_API_KEY=your_key_here
```

Run:

```bash
export $(cat .env)
uvicorn app:app --host 0.0.0.0 --port 8000
```

Then open http://localhost:8000 in your browser.

## Stops

| Stop | Routes |
|------|--------|
| Carshalton Beeches Station | All |
| Cambridge Rd / Banstead Rd | All |
| Downside Road | All |
| Wales Avenue | All |
| Royal Marsden Hospital | All |
| Sutton / Marshall's Road | 154 |
| Park Lane / Fairfield Halls | 154 |

The stop configuration is in `app.py` in the `SECTIONS` list. Edit NaPTAN IDs and line filters to customise for your own stops.

## Licence

GPLv2
