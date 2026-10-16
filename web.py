#!/usr/bin/env python3
"""
web.py - the Social Hustle dashboard.

FastAPI + one static HTML file (no build step, no JS framework).
  python web.py            # serves http://localhost:8000

Endpoints:
  GET  /                   client list + this week's drafts
  POST /runs/{slug}        start ContentRun for a client -> workflow id
  GET  /runs/{wid}         drafts + "what worked last week" (Temporal queries)
  POST /runs/{wid}/approve signal approval for the ticked drafts -> publish
"""
import asyncio
import json
import os
import re
import sys
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from temporalio.client import Client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import social_hustle as sh

HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
PORT = int(os.getenv("PORT", "8000"))
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = FastAPI(title="Social Hustle")
app.mount("/static", StaticFiles(directory=STATIC), name="static")

_client = None


async def client():
    global _client
    if _client is None:
        _client = await Client.connect(HOST)
    return _client


def _slug(path):
    return os.path.basename(os.path.dirname(path))


def clients():
    out = []
    for d in sorted(os.listdir("clients")):
        p = os.path.join("clients", d, "brand.json")
        if os.path.isdir(os.path.join("clients", d)) and os.path.exists(p):
            b = json.load(open(p))
            b["slug"] = d
            out.append(b)
    return out


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(open(os.path.join(STATIC, "index.html")).read())


@app.get("/api/clients")
async def api_clients():
    return JSONResponse(clients())


@app.post("/api/runs/{slug}")
async def api_run(slug: str):
    p = os.path.join("clients", slug, "brand.json")
    if not os.path.exists(p):
        raise HTTPException(404, f"unknown client {slug}")
    brand = sh._load_brand(p)
    c = await client()
    wid = f"{brand['name'].lower().replace(' ', '-')}-{datetime.now():%Y%m%d-%H%M}"
    await c.start_workflow(sh.ContentRun.run, brand, id=wid, task_queue=sh.QUEUE,
                           memo={"client": sh._slug(brand)})
    return JSONResponse({"workflow_id": wid})


@app.get("/api/runs/{wid}")
async def api_drafts(wid: str):
    c = await client()
    h = c.get_workflow_handle(wid)
    try:
        drafts = await h.query("drafts")
        learn = await h.query("learn")
    except Exception as e:
        raise HTTPException(502, f"workflow query failed: {e}")
    return JSONResponse({"drafts": drafts, "learn": learn})


@app.post("/api/runs/{wid}/approve")
async def api_approve(wid: str, indexes: list[int]):
    c = await client()
    h = c.get_workflow_handle(wid)
    await h.signal("approve", indexes)
    return JSONResponse({"ok": True})


@app.get("/api/runs")
async def api_runs():
    """Recent runs for the dashboard, newest first.

    The Temporal query is derived from the actual clients/*/ slugs, so any
    client's runs show up (not just the ones hardcoded into the query). Each
    run carries a `client` memo set at start; we read it back to tag the run
    with the slug even if the workflow id was renamed later.
    """
    c = await client()
    slugs = [d for d in os.listdir("clients")
             if os.path.isdir(os.path.join("clients", d))
             and os.path.exists(os.path.join("clients", d, "brand.json"))]
    out = []
    for slug in slugs:
        try:
            async for w in c.list_workflows(query=f"WorkflowId = '{slug}-*'"):
                d = await w.describe()
                try:
                    run_slug = await w.memo_value("client", default=None)
                except Exception:
                    run_slug = None
                out.append({"workflow_id": w.id,
                            "slug": run_slug or slug,
                            "status": str(d.status).split(".")[-1].lower()})
        except Exception:
            pass
    out.sort(key=lambda r: r["workflow_id"], reverse=True)
    return JSONResponse(out)


def _sanitize(name):
    return re.sub(r"[^a-z0-9-]", "", name.lower().replace(" ", "-"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)