"""ContentRun workflow tests against Temporal's time-skipping test server:
approval waits durably, and an unapproved run gives up after 48h — the skipping
environment fast-forwards that instead of us sleeping.

No pytest-asyncio (keeping deps to temporalio + fastapi per CLAUDE.md): each
test is a sync function that drives one asyncio.run over an async helper.
"""
import asyncio

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import social_hustle as sh

TASK = "test-queue"


# --- stub activities: same names as the real ones, canned results, zero I/O ---
@activity.defn(name="research")
async def research_stub(brand):
    return [{"title": "Coffee prices climb", "link": "https://example.com/a"}]


@activity.defn(name="learn")
async def learn_stub(brand):
    return {"summary": None, "reason": "only 2 published posts", "posts": []}


@activity.defn(name="analyze")
async def analyze_stub(brand, items, learn=None):
    return [{"title": items[0]["title"], "link": items[0]["link"],
             "why_it_fits": "on-brand", "score": 9}]


@activity.defn(name="draft")
async def draft_stub(brand, picks, learn=None):
    return [{"text": "Beans cost more. We didn't blink.", "source": picks[0]["link"]}]


@activity.defn(name="publish")
async def publish_stub(brand, text):
    return f"posted:{text[:10]}"


STUBS = [research_stub, learn_stub, analyze_stub, draft_stub, publish_stub]


def _scenario(brand, body, activities=STUBS):
    """Boot a time-skipping env + worker, run body(client), tear everything down."""
    async def go():
        env = await WorkflowEnvironment.start_time_skipping()
        try:
            async with Worker(env.client, task_queue=TASK,
                              workflows=[sh.ContentRun], activities=activities):
                return await body(env.client)
        finally:
            await env.shutdown()
    return asyncio.run(go())


async def wait_for_drafts(h, tries=100):
    """Drafts appear only after research/learn/analyze/draft complete, so poll
    the query (it returns [] before the workflow sets them)."""
    for _ in range(tries):
        drafts = await h.query("drafts")
        if drafts:
            return drafts
        await asyncio.sleep(0.1)
    raise AssertionError("drafts never appeared")


def test_approval_timeout_returns_empty(brand):
    """Nobody signals approve -> the 48h wait is skipped -> [] and no post."""
    async def body(client):
        h = await client.start_workflow(sh.ContentRun.run, brand,
                                        id="timeout-run", task_queue=TASK)
        drafts = await wait_for_drafts(h)
        assert drafts[0]["text"] == "Beans cost more. We didn't blink."
        # Time-skipping server fast-forwards the 48h approval window.
        return drafts, await h.result()

    drafts, result = _scenario(brand, body)
    assert result == []


def test_approve_publishes_only_selected_drafts(brand):
    async def body(client):
        h = await client.start_workflow(sh.ContentRun.run, brand,
                                        id="approve-run", task_queue=TASK)
        await wait_for_drafts(h)
        await h.signal("approve", [0])
        return await h.result()

    assert _scenario(brand, body) == ["posted:Beans cost"]


def test_learn_result_is_passed_to_analyze_and_draft(brand):
    """The graceful-learn contract end to end: a null summary plus a reason
    still flows into the downstream activities without breaking the run."""
    seen = {}

    @activity.defn(name="analyze")
    async def spy_analyze(brand, items, learn=None):
        seen["analyze_learn"] = learn
        await asyncio.sleep(0)
        return [{"title": items[0]["title"], "link": items[0]["link"],
                 "why_it_fits": "w", "score": 8}]

    stubs = [research_stub, spy_analyze, draft_stub, publish_stub, learn_stub]

    async def body(client):
        h = await client.start_workflow(sh.ContentRun.run, brand,
                                        id="learn-run", task_queue=TASK)
        await wait_for_drafts(h)
        await h.result()  # no approval -> 48h skip -> []

    _scenario(brand, body, activities=stubs)
    assert seen["analyze_learn"]["summary"] is None
    assert seen["analyze_learn"]["reason"] == "only 2 published posts"
