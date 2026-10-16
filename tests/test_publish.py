"""Publish idempotency. No network: dry-run writes the client's outbox, and the
history journal (pending -> posted) drives the "already sent?" decision."""
import asyncio
import hashlib
import json

import pytest
from temporalio.testing import ActivityEnvironment

import social_hustle as sh


def _run(fn, *args, **kwargs):
    return asyncio.run(ActivityEnvironment().run(fn, *args, **kwargs))


def _digest(brand, text):
    return hashlib.sha256(f"{sh._slug(brand)}:{text}".encode()).hexdigest()[:16]


def _history(workdir, brand):
    p = workdir / "clients" / brand["slug"] / "history.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []


def _outbox(workdir, brand):
    p = workdir / "clients" / brand["slug"] / "outbox.jsonl"
    return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []


def test_publish_writes_pending_then_posted(brand, workdir):
    """The journal entry lands BEFORE the network call, so a crash in between
    still leaves evidence the post was attempted."""
    text = "Fresh beans, same-day ship."
    res = _run(sh.publish, brand, text)
    assert "dry-run" in res
    rows = _history(workdir, brand)
    assert [r["status"] for r in rows] == ["pending", "posted"]
    assert all(r["digest"] == _digest(brand, text) for r in rows)
    assert len(_outbox(workdir, brand)) == 1


def test_publish_never_double_posts_on_retry(brand, workdir):
    """Temporal retries activities at-least-once. Once a digest is "posted",
    every later call is a no-op and the outbox keeps one entry."""
    text = "Why we roast in-house."
    first = _run(sh.publish, brand, text)
    second = _run(sh.publish, brand, text)
    assert "dry-run" in first
    assert second.startswith("already posted")
    assert len(_outbox(workdir, brand)) == 1
    assert len([r for r in _history(workdir, brand) if r["status"] == "posted"]) == 1


@pytest.fixture
def live_bsky(monkeypatch):
    """Pretend we have real Bluesky creds and the post is already out there."""
    monkeypatch.setenv("BSKY_HANDLE", "brewlane.bsky.social")
    monkeypatch.setenv("BSKY_APP_PASSWORD", "xxxx-xxxx")
    return monkeypatch


def test_publish_pending_then_retry_finds_it_on_bluesky(brand, workdir, live_bsky):
    """The crash path. Attempt 1 writes "pending" and dies before the journal
    can record success. Retry sees the lone "pending" row, asks Bluesky, finds
    the exact text already published, and does NOT send it a second time."""
    text = "Cold brew, 18 hours, no bitterness."
    digest = _digest(brand, text)

    # Attempt 1: record only the pending marker, then "crash" before posting.
    hist = workdir / "clients" / brand["slug"] / "history.jsonl"
    hist.parent.mkdir(parents=True, exist_ok=True)
    hist.write_text(json.dumps({"ts": 1.0, "slug": brand["slug"], "digest": digest,
                                "text": text, "status": "pending"}) + "\n")

    # Bluesky says it's already out there.
    live_bsky.setattr(sh, "_get", lambda url, timeout=30: json.dumps(
        {"posts": [{"record": {"text": text}}]}).encode())
    posted = []
    live_bsky.setattr(sh, "_post",
                      lambda *a, **k: posted.append(a) or "at://should/not/happen")

    res = _run(sh.publish, brand, text)

    assert res == f"already posted ({digest})"
    assert posted == []                       # never re-sent
    assert not _outbox(workdir, brand)        # nothing queued
    rows = _history(workdir, brand)
    assert [r["status"] for r in rows] == ["pending", "posted"]
    assert rows[-1]["result"] == "found-on-bluesky"


def test_publish_pending_retry_with_bluesky_down_uses_one_more_attempt(
        brand, workdir, live_bsky):
    """If the search can't be reached, publish must not guess. It retries the
    post; the digest journal still records exactly one "posted" line, so the
    next retry short-circuits."""
    text = "Ethiopia Yirgacheffe, notes of jasmine."
    digest = _digest(brand, text)
    hist = workdir / "clients" / brand["slug"] / "history.jsonl"
    hist.parent.mkdir(parents=True, exist_ok=True)
    hist.write_text(json.dumps({"ts": 1.0, "slug": brand["slug"], "digest": digest,
                                "text": text, "status": "pending"}) + "\n")

    live_bsky.setattr(sh, "_get", lambda url, timeout=30: (_ for _ in ()).throw(
        ConnectionError("bluesky unreachable")))
    live_bsky.setattr(sh, "_post", lambda t, s: f"at://{s}/{abs(hash(t)) % 999}")

    res = _run(sh.publish, brand, text)
    assert res.startswith("at://")
    rows = _history(workdir, brand)
    assert [r["status"] for r in rows] == ["pending", "posted"]
    # Third call short-circuits on the posted record.
    assert _run(sh.publish, brand, text) == f"already posted ({digest})"


def test_publish_outbox_dedups_equal_text_without_journal(brand, workdir):
    """Belt-and-braces: even if history were lost, _post itself refuses to queue
    the same text twice for the same client."""
    text = "Cupping tonight, 7pm, walk in."
    box = workdir / "clients" / brand["slug"] / "outbox.jsonl"
    box.parent.mkdir(parents=True, exist_ok=True)
    box.write_text(json.dumps({"ts": 1.0, "slug": brand["slug"], "text": text}) + "\n")
    assert sh._post(text, brand["slug"]) == "dry-run: already in outbox"
    assert len(box.read_text().splitlines()) == 1


def test_publish_two_clients_do_not_share_history_or_outbox(brand, workdir):
    """Per-client isolation: identical copy under a different slug posts once
    each, and the digests differ because the slug is in the hash."""
    text = "Same copy, two brands."
    other = dict(brand, slug="other-shop", name="Other Shop")
    (workdir / "clients" / "other-shop").mkdir(parents=True, exist_ok=True)

    _run(sh.publish, brand, text)
    _run(sh.publish, other, text)

    assert len(_outbox(workdir, brand)) == 1
    assert len(_outbox(workdir, other)) == 1
    assert _digest(brand, text) != _digest(other, text)
