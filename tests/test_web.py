"""web.py endpoint tests — offline. The Temporal client is a fake: api_runs
must query per-slug (not a hardcoded pattern) without talking to a server."""
import asyncio
import json

import pytest
from fastapi import HTTPException

import web


class FakeDesc:
    def __init__(self, status_name, memo):
        class S:  # stand-in for the protobuf IntEnum
            name = status_name
        self.status = S()
        self._memo = memo

    async def memo_value(self, key, default=None):
        return self._memo.get(key, default)


class FakeExec:
    def __init__(self, wid, desc):
        self.id = wid
        self._desc = desc

    async def describe(self):
        return self._desc


class FakeClient:
    """Records every query so we can assert the per-slug derivation."""

    def __init__(self, runs):
        self.runs = runs      # list of FakeExec
        self.queries = []

    async def list_workflows(self, query=None):
        self.queries.append(query)
        pattern = query.split("'")[1]           # e.g. "brew-lane-*"
        prefix = pattern.rstrip("*")            # e.g. "brew-lane-"
        for r in self.runs:
            if r.id.startswith(prefix):
                yield r


@pytest.fixture
def two_clients(workdir):
    (workdir / "clients" / "other-shop").mkdir()
    b = json.loads((workdir / "clients" / "brew-lane" / "brand.json").read_text())
    b["name"] = "Other Shop"
    (workdir / "clients" / "other-shop" / "brand.json").write_text(json.dumps(b))
    return ["brew-lane", "other-shop"]


class Req:
    headers = {}


def test_api_runs_queries_per_client_slug(workdir, two_clients, monkeypatch):
    runs = [
        FakeExec("brew-lane-20261016-0800",
                 FakeDesc("RUNNING", {"client": "brew-lane"})),
        FakeExec("other-shop-20261016-0801",
                 FakeDesc("COMPLETED", {"client": "other-shop"})),
    ]
    fc = FakeClient(runs)
    monkeypatch.setattr(web, "client", lambda: asyncio.sleep(0, result=fc))

    resp = asyncio.run(web.api_runs(Req()))
    # jsonable responses carry a .body; accept either dict or Response
    import json as _json
    body = resp.body if hasattr(resp, "body") else resp
    out = _json.loads(body) if isinstance(body, (bytes, bytearray)) else body

    assert {r["workflow_id"] for r in out} == {
        "brew-lane-20261016-0800", "other-shop-20261016-0801"}
    assert {r["slug"] for r in out} == {"brew-lane", "other-shop"}
    assert [r["status"] for r in out if r["slug"] == "brew-lane"] == ["running"]
    # One query per slug, derived from the clients dir — no hardcoded pattern.
    assert sorted(fc.queries) == sorted(
        [f"WorkflowId = '{s}-*'" for s in two_clients])


def test_api_runs_lists_run_without_memo(workdir, two_clients, monkeypatch):
    """Old runs started before the client memo exist — still listed, tagged
    with the slug the query found them under."""
    runs = [FakeExec("brew-lane-20260101-0900", FakeDesc("FAILED", {}))]
    fc = FakeClient(runs)
    monkeypatch.setattr(web, "client", lambda: asyncio.sleep(0, result=fc))

    body = asyncio.run(web.api_runs(Req())).body
    out = json.loads(body) if isinstance(body, (bytes, bytearray)) else body
    assert out == [{"workflow_id": "brew-lane-20260101-0900",
                    "slug": "brew-lane", "status": "failed"}]


# ---------------------------------------------------------------- auth
def test_api_runs_unauthenticated_when_token_set(workdir, two_clients, monkeypatch):
    """With DASH_TOKEN set, a request without the header is rejected with 401."""
    runs = [FakeExec("brew-lane-20261016-0800", FakeDesc("RUNNING", {"client": "brew-lane"}))]
    fc = FakeClient(runs)
    monkeypatch.setattr(web, "client", lambda: asyncio.sleep(0, result=fc))
    monkeypatch.setattr(web, "DASH_TOKEN", "secret")

    # web.py raises HTTPException(401) — the FastAPI layer turns that into a
    # 401 response, so the handler itself raises. Drive it through the same
    # path the other auth tests use.
    with pytest.raises(HTTPException) as ei:
        asyncio.run(web.api_runs(Req()))
    assert ei.value.status_code == 401


def test_api_runs_authenticated_with_token(workdir, two_clients, monkeypatch):
    """The right header passes the check and the run is listed."""
    runs = [FakeExec("brew-lane-20261016-0800", FakeDesc("RUNNING", {"client": "brew-lane"}))]
    fc = FakeClient(runs)
    monkeypatch.setattr(web, "client", lambda: asyncio.sleep(0, result=fc))
    monkeypatch.setattr(web, "DASH_TOKEN", "secret")

    class Req:
        headers = {"X-Dash-Token": "secret"}

    async def call():
        return await web.api_runs(Req())

    resp = asyncio.run(call())
    body = resp.body if hasattr(resp, "body") else resp
    import json as _json
    out = _json.loads(body) if isinstance(body, (bytes, bytearray)) else body
    assert [r["workflow_id"] for r in out] == ["brew-lane-20261016-0800"]


def test_api_runs_no_token_when_unset(workdir, two_clients, monkeypatch):
    """No DASH_TOKEN -> auth disabled, everything passes through."""
    runs = [FakeExec("brew-lane-20261016-0800", FakeDesc("RUNNING", {"client": "brew-lane"}))]
    fc = FakeClient(runs)
    monkeypatch.setattr(web, "client", lambda: asyncio.sleep(0, result=fc))
    monkeypatch.setattr(web, "DASH_TOKEN", "")

    class Req:
        headers = {}

    async def call():
        return await web.api_runs(Req())

    resp = asyncio.run(call())
    body = resp.body if hasattr(resp, "body") else resp
    import json as _json
    out = _json.loads(body) if isinstance(body, (bytes, bytearray)) else body
    assert [r["workflow_id"] for r in out] == ["brew-lane-20261016-0800"]


# ---------------------------------------------------------------- bind guard
def test_main_refuses_non_loopback_without_token(monkeypatch):
    """A non-loopback bind with no DASH_TOKEN must refuse to start."""
    monkeypatch.setattr(web, "DASH_TOKEN", "")
    monkeypatch.setenv("HOST", "0.0.0.0")
    monkeypatch.setattr(web.uvicorn, "run", lambda *a, **k: None)
    with pytest.raises(SystemExit) as ei:
        web.main()
    assert "DASH_TOKEN" in str(ei.value)


def test_main_allows_non_loopback_with_token(monkeypatch):
    """With DASH_TOKEN set, a non-loopback bind is allowed."""
    monkeypatch.setattr(web, "DASH_TOKEN", "secret")
    monkeypatch.setenv("HOST", "0.0.0.0")
    calls = {}
    monkeypatch.setattr(web.uvicorn, "run",
                        lambda app, host, port: calls.update(host=host, port=port))
    web.main()
    assert calls == {"host": "0.0.0.0", "port": 8000}


def test_main_defaults_to_loopback(monkeypatch):
    """No HOST override -> 127.0.0.1, even without a token."""
    monkeypatch.setattr(web, "DASH_TOKEN", "")
    monkeypatch.delenv("HOST", raising=False)
    calls = {}
    monkeypatch.setattr(web.uvicorn, "run",
                        lambda app, host, port: calls.update(host=host, port=port))
    web.main()
    assert calls == {"host": "127.0.0.1", "port": 8000}
