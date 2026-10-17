"""Tests for the LLM-driven activities: analyze, draft, learn.

No network: MOCK_LLM=1 gives canned schema-valid JSON, and the activity
runner (temporalio.testing.ActivityEnvironment) executes activities outside
a real worker so no Temporal server is needed.
"""
import asyncio
import json

import pytest
from temporalio.testing import ActivityEnvironment

import social_hustle as sh


def _run(fn, *args, **kwargs):
    """Run an activity in the in-process test environment (no worker/server).
    Async activities return a coroutine; drive it with a tiny event loop."""
    return asyncio.run(ActivityEnvironment().run(fn, *args, **kwargs))


# ---------------------------------------------------------------- analyze
def test_analyze_returns_top_3_sorted_by_score(brand):
    items = [{"title": "Coffee prices are climbing", "link": "https://example.com/a"},
             {"title": "New study says specialty beans keep their flavor longer",
              "link": "https://example.com/b"},
             {"title": "Local roasters band together on a sustainability pledge",
              "link": "https://example.com/c"}]
    picks = _run(sh.analyze, brand, items)
    assert len(picks) == 3
    scores = [p["score"] for p in picks]
    assert scores == sorted(scores, reverse=True)
    for p in picks:
        assert set(p) == {"title", "link", "why_it_fits", "score"}
        assert isinstance(p["score"], int)


def test_analyze_skips_learn_section_when_no_summary(brand, workdir, caplog):
    """A cold start (no performance data) must not inject a null summary into
    the prompt — the section is simply absent."""
    captured = {}

    real = sh.llm_json

    def fake(prompt, schema):
        captured["prompt"] = prompt
        return real(prompt, schema)

    sh.llm_json = fake
    try:
        _run(sh.analyze, brand, [{"title": "t", "link": "l"}],
             learn={"summary": None, "reason": "no data"})
    finally:
        sh.llm_json = real
    assert "What worked last week" not in captured["prompt"]


def test_analyze_includes_learn_section_when_summary_present(brand):
    captured = {}
    real = sh.llm_json

    def fake(prompt, schema):
        captured["prompt"] = prompt
        return real(prompt, schema)

    sh.llm_json = fake
    try:
        _run(sh.analyze, brand, [{"title": "t", "link": "l"}],
             learn={"summary": "Short questions won."})
    finally:
        sh.llm_json = real
    assert "What worked last week: Short questions won." in captured["prompt"]


# ---------------------------------------------------------------- draft
def test_draft_returns_posts_with_text_and_source(brand):
    picks = [{"title": "Coffee prices are climbing", "link": "https://example.com/a",
              "why_it_fits": "home brewers care", "score": 9}]
    posts = _run(sh.draft, brand, picks)
    assert len(posts) == 2
    for p in posts:
        assert set(p) == {"text", "source"}
        assert len(p["text"]) <= 300
        assert p["source"].startswith("https://")


def test_draft_respects_voice_and_never(brand):
    captured = {}
    real = sh.llm_json

    def fake(prompt, schema):
        captured["prompt"] = prompt
        return real(prompt, schema)

    sh.llm_json = fake
    try:
        _run(sh.draft, brand, [{"title": "t", "link": "l", "why_it_fits": "w", "score": 8}])
    finally:
        sh.llm_json = real
    assert brand["voice"] in captured["prompt"]
    assert brand["avoid"] in captured["prompt"]


# ---------------------------------------------------------------- learn
def test_learn_graceful_when_no_history(brand, workdir):
    res = _run(sh.learn, brand)
    assert res["summary"] is None
    assert res["reason"]
    assert "0" in res["reason"]


def test_learn_graceful_when_fewer_than_min_posted(brand, workdir):
    hist = workdir / "clients" / brand["slug"] / "history.jsonl"
    hist.parent.mkdir(parents=True, exist_ok=True)
    with open(hist, "w") as f:
        # Only pending entries — nothing actually went out.
        for i in range(3):
            f.write(json.dumps({"text": f"post {i}", "status": "pending"}) + "\n")
    res = _run(sh.learn, brand)
    assert res["summary"] is None
    assert res["reason"]


def test_learn_graceful_on_network_failure(brand, workdir, monkeypatch):
    """Even with enough posted items, a Bluesky outage must not fail the run."""
    hist = workdir / "clients" / brand["slug"] / "history.jsonl"
    hist.parent.mkdir(parents=True, exist_ok=True)
    with open(hist, "w") as f:
        for i in range(sh.MIN_POSTED):
            f.write(json.dumps({"text": f"post {i}", "status": "posted",
                                "result": "at://x"}) + "\n")
    monkeypatch.setattr(sh, "_get", lambda *a, **k: (_ for _ in ()).throw(
        ConnectionError("bluesky down")))
    res = _run(sh.learn, brand)
    assert res["summary"] is None
    assert res["reason"]


def test_learn_produces_summary_when_data_is_there(brand, workdir, monkeypatch):
    """Happy path: enough posted items and reachable Bluesky -> a real summary."""
    hist = workdir / "clients" / brand["slug"] / "history.jsonl"
    hist.parent.mkdir(parents=True, exist_ok=True)
    with open(hist, "w") as f:
        for i in range(sh.MIN_POSTED):
            f.write(json.dumps({"text": f"post {i}", "status": "posted",
                                "result": "at://x"}) + "\n")

    def fake_get(url, timeout=30):
        return json.dumps({"likes": [{"count": 7}]}).encode()

    monkeypatch.setattr(sh, "_get", fake_get)
    res = _run(sh.learn, brand)
    assert res["summary"]
    assert res["reason"] is None
    assert {p["likes"] for p in res["posts"]} == {7}


# ---------------------------------------------------------------- json repair
def test_parse_with_repair_valid():
    raw = '{"picks": [{"title": "x", "link": "u", "why_it_fits": "y", "score": 9}]}'
    assert sh._parse_with_repair(raw)["picks"][0]["score"] == 9


def test_parse_with_repair_strips_fences():
    raw = '```json\n{"picks": [{"title": "x"}]}\n```'
    assert sh._parse_with_repair(raw)["picks"][0]["title"] == "x"


def test_parse_with_repair_truncated_mid_string():
    """gemma3:4b sometimes cuts its response off mid-string; the closing
    braces are still present, so walking back to the last valid cut point
    recovers a complete object."""
    good = ('{"picks": [{"title": "Coffee prices are climbing here is what that '
            'means for your brew", "link": "https://example.com/a", "why_it_fits": '
            '"This is a consistently relevant topic for our audience. We can '
            'position Brew Lane as a trusted source for understanding the factors '
            'driving price increases traceability, climate change impacts on crops, '
            'and we can use this to showcase our values.", "score": 7}]}')
    trunc = good[:1575]
    assert trunc.endswith("}]}")  # the closing containers are still present
    out = sh._parse_with_repair(trunc)
    assert out["picks"][0]["score"] == 7
    assert out["picks"][0]["title"].startswith("Coffee prices")


def test_parse_with_repair_unrepairable_raises():
    with pytest.raises(ValueError):
        sh._parse_with_repair('{"picks": [{"title": "x"')