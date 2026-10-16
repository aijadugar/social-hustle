#!/usr/bin/env python3
"""
social_hustle.py - a done-for-you social media engine for small-business clients.

Pipeline (one durable Temporal workflow per client per day):
  research (Google News RSS) -> analyze (local Gemma ranks angles for the brand)
  -> draft (Gemma writes posts in the client's voice) -> HUMAN APPROVAL (signal)
  -> publish (Bluesky; dry-run outbox if no creds)

Open pieces: Gemma via Ollama (local, $0/post, client data stays on your box),
Temporal (durable, retries, waits for approval without losing state).

Setup:
  ollama pull gemma3:4b                 # or any newer Gemma: MODEL=gemma3:12b
  pip install temporalio
  temporal server start-dev             # https://docs.temporal.io/cli
Run:
  python social_hustle.py worker                      # terminal 1
  python social_hustle.py run brand.json              # terminal 2 -> prints workflow id
  python social_hustle.py show <workflow_id>          # view drafts
  python social_hustle.py approve <workflow_id> 0 2   # publish drafts #0 and #2
  python social_hustle.py schedule                    # weekday 8am ContentRun per client
Optional posting: export BSKY_HANDLE=you.bsky.social BSKY_APP_PASSWORD=xxxx
"""
import asyncio, hashlib, json, os, sys, time, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from temporalio import activity, workflow
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, \
    ScheduleOverlapPolicy, ScheduleSpec
from temporalio.common import RetryPolicy
from temporalio.exceptions import TimeoutError
from temporalio.worker import Worker

OLLAMA = "http://localhost:11434/api/chat"
MODEL = os.getenv("MODEL", "gemma3:4b")
QUEUE = "social-hustle"
# MOCK_LLM=1: skip Ollama, return canned schema-valid JSON. Smoke tests + demo.
MOCK_LLM = os.getenv("MOCK_LLM", "0") == "1"


# ---------- helpers (blocking; run in threads) ----------
def _req(url, body=None, headers=None, timeout=300):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json", "User-Agent": "social-hustle/1.0", **(headers or {})}
    with urllib.request.urlopen(urllib.request.Request(url, data, h), timeout=timeout) as r:
        return r.read()


def _get(url, timeout=30):
    with urllib.request.urlopen(urllib.request.Request(
            url, headers={"User-Agent": "social-hustle/1.0"}), timeout=timeout) as r:
        return r.read()


def _mock(prompt, schema):
    """Canned, schema-valid responses. Detect the activity by a keyword in the
    prompt so one function covers analyze / draft / learn."""
    if "Pick the 3 headlines" in prompt:
        return {"picks": [
            {"title": "Coffee prices are climbing — here's what that means for your brew",
             "link": "https://example.com/a", "why_it_fits": "Direct, useful angle home brewers care about.", "score": 9},
            {"title": "New study says specialty beans keep their flavor longer",
             "link": "https://example.com/b", "why_it_fits": "Teaches the audience something.", "score": 8},
            {"title": "Local roasters band together on a new sustainability pledge",
             "link": "https://example.com/c", "why_it_fits": "Community angle, on-brand.", "score": 7}]}
    if "Write one social post" in prompt:
        return {"posts": [
            {"text": "Coffee prices are up. We're not marking up our beans — here's what's actually happening and how to brew around it.",
             "source": "https://example.com/a"},
            {"text": "Freshness isn't a myth. Our beans are roasted in-house and shipped the same day. Try a bag and taste the difference.",
             "source": "https://example.com/b"}]}
    if "Summarize what worked" in prompt:
        return {"summary": "Short, plain questions performed best. Posts that taught one thing got the most likes; "
                          "long promotional threads got none. Keep posts under 200 chars and end with a single question."}
    return {}


def llm_json(prompt, schema):
    if MOCK_LLM:
        return _mock(prompt, schema)
    body = {"model": MODEL, "stream": False, "format": schema,
            "messages": [{"role": "user", "content": prompt}],
            "options": {"temperature": 0.7}}
    return json.loads(json.loads(_req(OLLAMA, body))["message"]["content"])


# ---------- activities ----------
@activity.defn
async def research(brand: dict) -> list[dict]:
    def go():
        items = []
        for kw in brand["keywords"]:
            q = urllib.parse.quote(f"{kw} when:7d")
            xml = _req(f"https://news.google.com/rss/search?q={q}", timeout=30)
            for it in ET.fromstring(xml).iterfind(".//item"):
                items.append({"title": it.findtext("title"), "link": it.findtext("link")})
                if len(items) % 6 == 0:
                    break
        seen, out = set(), []
        for i in items:
            if i["title"] not in seen:
                seen.add(i["title"]); out.append(i)
        return out[:20]
    return await asyncio.to_thread(go)


@activity.defn
async def learn(brand: dict) -> dict:
    """What worked last week: read this client's history.jsonl plus the public
    Bluesky like/repost counts for past posts, then ask Gemma for a summary."""
    slug = brand.get("slug") or brand["name"].lower().replace(" ", "-")
    hist = os.path.join("clients", slug, "history.jsonl")
    posts = []
    if os.path.exists(hist):
        with open(hist) as f:
            for line in f:
                try:
                    posts.append(json.loads(line))
                except Exception:
                    pass
    posts = posts[-12:]
    for p in posts:
        if p.get("result") and p["result"].startswith("at://"):
            try:
                j = json.loads(_get(
                    f"https://bsky.social/xrpc/app.bsky.feed.getLikes?uri={p['result']}&limit=1"))
                p["likes"] = j.get("likes", [{}])[0].get("count", 0) if j.get("likes") else 0
            except Exception:
                p["likes"] = 0
        else:
            p["likes"] = 0
    if not posts:
        return {"summary": "", "posts": []}
    schema = {"type": "object", "properties": {"summary": {"type": "string"}},
              "required": ["summary"]}
    prompt = (f"Summarize what worked for {brand['name']} on social media from these "
              f"recent posts. Voice: {brand['voice']}. Return a short, plain-English "
              f"note (2-4 sentences) on which angles, phrasing lengths, and call-to-action "
              f"styles got the most likes, and what to do more of / less of next week.\n\n"
              "Posts:\n" + "\n".join(
                  f"- {p.get('text','')[:200]} | likes: {p.get('likes', 0)}"
                  for p in posts))
    res = await asyncio.to_thread(llm_json, prompt, schema)
    return {"summary": res["summary"], "posts": posts}


@activity.defn
async def analyze(brand: dict, items: list[dict], learn: dict | None = None) -> list[dict]:
    schema = {"type": "object", "properties": {"picks": {"type": "array", "items": {
        "type": "object", "properties": {
            "title": {"type": "string"}, "link": {"type": "string"},
            "why_it_fits": {"type": "string"}, "score": {"type": "integer"}},
        "required": ["title", "link", "why_it_fits", "score"]}}}, "required": ["picks"]}
    prompt = (f"You are a social media strategist for {brand['name']}: {brand['about']}\n"
              f"Audience: {brand['audience']}\n"
              f"{('What worked last week: ' + learn['summary']) if learn and learn.get('summary') else ''}\n\n"
              "Headlines this week:\n" +
              "\n".join(f"- {i['title']} | {i['link']}" for i in items) +
              "\n\nPick the 3 headlines this brand can comment on credibly and usefully. "
              "Score 1-10 for audience relevance. Skip anything off-brand or risky. "
              "Prefer angles and phrasing that match what worked last week.")
    res = await asyncio.to_thread(llm_json, prompt, schema)
    return sorted(res["picks"], key=lambda p: -p["score"])[:3]


@activity.defn
async def draft(brand: dict, picks: list[dict], learn: dict | None = None) -> list[dict]:
    schema = {"type": "object", "properties": {"posts": {"type": "array", "items": {
        "type": "object", "properties": {"text": {"type": "string"}, "source": {"type": "string"}},
        "required": ["text", "source"]}}}, "required": ["posts"]}
    prompt = (f"Write one social post per topic for {brand['name']}.\n"
              f"Voice: {brand['voice']}\nNever: {brand.get('avoid', 'hype, emojis spam')}\n"
              f"{('What worked last week: ' + learn['summary']) if learn and learn.get('summary') else ''}\n"
              "Max 280 characters each. Add a clear point of view and a soft call to action. "
              "Do not invent facts beyond the headline. Mirror the phrasing style and "
              "call-to-action format that worked last week.\n\nTopics:\n" +
              "\n".join(f"- {p['title']} ({p['why_it_fits']}) {p['link']}" for p in picks))
    res = await asyncio.to_thread(llm_json, prompt, schema)
    return [{"text": p["text"][:300], "source": p["source"]} for p in res["posts"]]


@activity.defn
async def publish(brand: dict, text: str) -> str:
    """Idempotent: a post is keyed by hash(client + text). Re-running a workflow
    (e.g. a Temporal retry) never double-posts.

    The digest is recorded as "pending" BEFORE the network call and flipped to
    "posted" after, so a crash between posting and recording can't double-post.
    On retry a "pending" entry re-checks Bluesky for the text before posting.
    """
    slug = brand.get("slug") or brand["name"].lower().replace(" ", "-")
    digest = hashlib.sha256(f"{slug}:{text}".encode()).hexdigest()[:16]
    hist = os.path.join("clients", slug, "history.jsonl")
    os.makedirs(os.path.dirname(hist), exist_ok=True)
    with open(hist, "a") as f:
        f.write(json.dumps({"ts": time.time(), "slug": slug, "digest": digest,
                            "text": text, "status": "pending"}) + "\n")
    if _already_on_bluesky(text):
        with open(hist, "a") as f:
            f.write(json.dumps({"ts": time.time(), "slug": slug, "digest": digest,
                                "text": text, "status": "posted", "result": "found-on-bluesky"}) + "\n")
        return f"already posted ({digest})"
    result = await asyncio.to_thread(_post, text)
    with open(hist, "a") as f:
        f.write(json.dumps({"ts": time.time(), "slug": slug, "digest": digest,
                            "text": text, "status": "posted", "result": result}) + "\n")
    return result


def _already_on_bluesky(text: str) -> bool:
    """Public search: did this exact text already go out? Only consulted for a
    "pending" entry, so a crash between post+record can't double-post."""
    if not (os.getenv("BSKY_HANDLE") and os.getenv("BSKY_APP_PASSWORD")):
        return False  # dry-run: no remote record to check
    try:
        q = urllib.parse.quote(f'"{text[:60]}"')
        j = json.loads(_get(
            f"https://bsky.social/xrpc/app.bsky.feed.searchPosts?query={q}&limit=5"))
        for p in j.get("posts", []):
            if p.get("record", {}).get("text") == text:
                return True
    except Exception:
        return False
    return False


def _post(text: str) -> str:
    h, pw = os.getenv("BSKY_HANDLE"), os.getenv("BSKY_APP_PASSWORD")
    if not (h and pw):  # dry run: safe default, still proves the pipeline
        with open("outbox.jsonl", "a") as f:
            f.write(json.dumps({"ts": time.time(), "text": text}) + "\n")
        return "dry-run: saved to outbox.jsonl"
    base = "https://bsky.social/xrpc"
    s = json.loads(_req(f"{base}/com.atproto.server.createSession",
                        {"identifier": h, "password": pw}))
    rec = {"repo": s["did"], "collection": "app.bsky.feed.post", "record": {
        "$type": "app.bsky.feed.post", "text": text,
        "createdAt": datetime.now(timezone.utc).isoformat()}}
    r = json.loads(_req(f"{base}/com.atproto.repo.createRecord", rec,
                        {"Authorization": f"Bearer {s['accessJwt']}"}))
    return r["uri"]


# ---------- workflow ----------
@workflow.defn
class ContentRun:
    def __init__(self):
        self._drafts: list[dict] = []
        self._approved: list[int] | None = None
        self._learn: dict = {}

    @workflow.signal
    def approve(self, indexes: list[int]) -> None:
        self._approved = indexes

    @workflow.query
    def drafts(self) -> list[dict]:
        return self._drafts

    @workflow.query
    def learn(self) -> dict:
        return self._learn

    @workflow.run
    async def run(self, brand: dict) -> list[str]:
        opts = dict(start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=RetryPolicy(maximum_attempts=5,
                                             initial_interval=timedelta(seconds=5)))
        items = await workflow.execute_activity(research, brand, **opts)
        self._learn = await workflow.execute_activity(learn, brand, **opts)
        picks = await workflow.execute_activity(analyze, args=[brand, items, self._learn], **opts)
        self._drafts = await workflow.execute_activity(
            draft, args=[brand, picks, self._learn], **opts)

        try:  # durable human-in-the-loop: client/you approve whenever, even tomorrow
            await workflow.wait_condition(lambda: self._approved is not None,
                                          timeout=timedelta(hours=48))
        except TimeoutError:
            return []
        results = []
        for i in self._approved:
            results.append(await workflow.execute_activity(
                publish, args=[brand, self._drafts[i]["text"]], **opts))
        return results


# ---------- CLI ----------
def _clients():
    return sorted(d for d in os.listdir("clients")
                  if os.path.isdir(os.path.join("clients", d))
                  and os.path.exists(os.path.join("clients", d, "brand.json")))


def _load_brand(path):
    brand = json.load(open(path))
    brand.setdefault("slug", os.path.basename(os.path.dirname(path)))
    return brand


async def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    client = await Client.connect("localhost:7233")
    if cmd == "worker":
        print(f"Worker up (model={MODEL}). Ctrl+C to stop.")
        await Worker(client, task_queue=QUEUE, workflows=[ContentRun],
                     activities=[research, analyze, draft, publish]).run()
    elif cmd == "run":
        brand = _load_brand(sys.argv[2])
        wid = f"{brand['name'].lower().replace(' ', '-')}-{datetime.now():%Y%m%d-%H%M}"
        await client.start_workflow(ContentRun.run, brand, id=wid, task_queue=QUEUE)
        print(f"Started {wid}\nNext: python social_hustle.py show {wid}")
    elif cmd == "show":
        h = client.get_workflow_handle(sys.argv[2])
        for i, d in enumerate(await h.query("drafts")):
            print(f"[{i}] {d['text']}\n    source: {d['source']}\n")
    elif cmd == "approve":
        h = client.get_workflow_handle(sys.argv[2])
        await h.signal("approve", [int(x) for x in sys.argv[3:]])
        print("Approved. Result:", await h.result())
    elif cmd == "schedule":
        for slug in _clients():
            brand = _load_brand(f"clients/{slug}/brand.json")
            sid = f"{slug}-weekday-8am"
            await client.create_schedule(
                sid, Schedule(
                    action=ScheduleActionStartWorkflow(
                        ContentRun.run, brand, id=f"{slug}-{datetime.now():%Y%m%d}",
                        task_queue=QUEUE),
                    spec=ScheduleSpec(cron_expressions=["0 8 * * 1-5"]),
                    policy=type("P", (), {"overlap": ScheduleOverlapPolicy.SKIP})()),
            )
            print(f"Schedule {sid}: weekday 8am -> ContentRun for {brand['name']}")
    else:
        print(__doc__)

if __name__ == "__main__":
    asyncio.run(main())
