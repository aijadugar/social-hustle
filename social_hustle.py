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
import asyncio, hashlib, json, logging, os, sys, time, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("social_hustle")

from temporalio import activity, workflow
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, \
    ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec
from temporalio.common import RetryPolicy
from temporalio.worker import Worker

OLLAMA = "http://localhost:11434/api/chat"
MODEL = os.getenv("MODEL", "gemma3:4b")
QUEUE = "social-hustle"
# MOCK_LLM=1: skip Ollama, return canned schema-valid JSON. Smoke tests + demo.
MOCK_LLM = os.getenv("MOCK_LLM", "0") == "1"
# Per-call timeout for the Ollama structured-output call. Gemma on a slow box can
# take a while for the first response; raise it if you see "timed out".
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "600"))


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
    # Structured output on a cold/slow box can exceed a single timeout, gemma3:4b
    # sometimes cuts its response off mid-string, and Ollama can 404 while
    # reloading the model. Retry with growing backoff before giving up — a
    # transient failure shouldn't kill the run.
    for attempt in range(5):
        try:
            raw = json.loads(_req(OLLAMA, body, timeout=LLM_TIMEOUT))["message"]["content"]
            return _parse_with_repair(raw)
        except Exception as e:
            if attempt == 4:
                raise
            delay = 3 * (2 ** attempt)      # 3s, 6s, 12s, 24s
            log.warning("llm_json attempt %d failed: %s (retrying in %ds)",
                        attempt + 1, e, delay)
            time.sleep(delay)


def _strip_fences(raw):
    """Remove a markdown ```json ... ``` wrapper if present."""
    stripped = raw.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    return stripped


def _repair_truncated_json(text):
    """Close a JSON value that a truncated model response cut off mid-way.

    gemma3:4b sometimes returns a response cut off mid-string, e.g.
    'Unterminated string starting at: line 17'. Rather than failing the whole
    activity, scan once tracking string state and the container stack, then
    append the missing closing quote (if cut mid-string) and the missing ]/}
    brackets. Trailing commas are dropped first.
    """
    out, stack, i, n = [], [], 0, len(text)
    in_string = False
    while i < n:
        c = text[i]
        if in_string:
            out.append(c)
            if c == "\\" and i + 1 < n:          # keep escapes intact
                out.append(text[i + 1]); i += 2; continue
            if c == '"':
                in_string = False
            i += 1; continue
        if c == '"':
            in_string = True; out.append(c); i += 1; continue
        if c in "[{":
            stack.append(c); out.append(c); i += 1; continue
        if c in "]}":
            if stack and ((c == "]" and stack[-1] == "[") or (c == "}" and stack[-1] == "{")):
                stack.pop()
            out.append(c); i += 1; continue
        out.append(c); i += 1
    s = "".join(out).rstrip()
    # Drop trailing commas before a closing bracket: {"a":1,} / [1,2,]
    cleaned, j, m = [], 0, len(s)
    while j < m:
        if s[j] == "," and j + 1 < m and s[j + 1] in "]}":
            j += 1; continue
        cleaned.append(s[j]); j += 1
    s = "".join(cleaned).rstrip()
    if in_string:
        if s.endswith("\\"):                    # dangling escape -> close it first
            s += '"'
        s += '"'                                # close the unterminated string
    for opener in reversed(stack):              # close open containers, innermost first
        s += "]" if opener == "[" else "}"
    return s


def _parse_with_repair(raw):
    """Parse the model's JSON, repairing truncation if needed."""
    text = _strip_fences(raw)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    repaired = _repair_truncated_json(text)
    try:
        log.warning("llm_json: repaired truncated response (%d -> %d chars)",
                    len(text), len(repaired))
        return json.loads(repaired)
    except json.JSONDecodeError:
        raise ValueError("no valid JSON in model response")


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


# How long to wait on the public Bluesky like-count lookups. Short: this is a
# nice-to-have signal, never worth stalling the whole run.
LEARN_NET_TIMEOUT = float(os.getenv("LEARN_NET_TIMEOUT", "5"))
# A published post counts toward the "enough data" bar only if it actually went
# out (or is a confirmed dry-run record). Pending entries don't count.
MIN_POSTED = 5


@activity.defn
async def learn(brand: dict) -> dict:
    """What worked last week: read this client's history.jsonl plus the public
    Bluesky like/repost counts for past posts, then ask Gemma for a summary.

    Graceful: on any network error, empty history, or fewer than MIN_POSTED
    published items, returns a null summary with a reason. The analyze/draft
    prompts skip the "what worked" section when the summary is null, so a cold
    start just means no performance signal — never a failed workflow.
    """
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
    posted = [p for p in posts if p.get("status") == "posted"]
    if len(posted) < MIN_POSTED:
        reason = (f"only {len(posted)} published post(s) on record "
                  f"(need {MIN_POSTED})")
        log.warning("learn[%s]: %s — skipping performance summary", slug, reason)
        return {"summary": None, "reason": reason, "posts": posts}

    recent = posted[-12:]
    checked = failed = 0
    for p in recent:
        if p.get("result") and p["result"].startswith("at://"):
            checked += 1
            likes = await _bluesky_likes(p["result"])
            if likes is None:
                failed += 1
                p["likes"] = None
            else:
                p["likes"] = likes
        else:
            p["likes"] = None  # dry-run record: no public engagement to read
    if checked and failed == checked:
        # Every like-count failed — there is no signal to summarize.
        reason = "could not reach Bluesky to read engagement"
        log.warning("learn[%s]: %s — skipping performance summary", slug, reason)
        return {"summary": None, "reason": reason, "posts": posts}

    schema = {"type": "object", "properties": {"summary": {"type": "string"}},
              "required": ["summary"]}
    prompt = (f"Summarize what worked for {brand['name']} on social media from these "
              f"recent posts. Voice: {brand['voice']}. Return a short, plain-English "
              f"note (2-4 sentences) on which angles, phrasing lengths, and call-to-action "
              f"styles got the most likes, and what to do more of / less of next week.\n\n"
              "Posts:\n" + "\n".join(
                  f"- {p.get('text','')[:200]} | likes: {p['likes'] if p['likes'] is not None else 'n/a'}"
                  for p in recent))
    res = await asyncio.to_thread(llm_json, prompt, schema)
    return {"summary": res["summary"], "reason": None, "posts": posted}


async def _bluesky_likes(uri: str) -> int | None:
    """Public like count for one post. One retry, short timeout, never raises:
    returns None only if the lookup itself failed (as opposed to 0 likes, which
    is real data). This signal is optional and must never fail the workflow."""
    url = f"https://bsky.social/xrpc/app.bsky.feed.getLikes?uri={uri}&limit=1"
    for attempt in range(2):
        try:
            j = json.loads(await asyncio.to_thread(_get, url, timeout=LEARN_NET_TIMEOUT))
            likes = j.get("likes", [{}])[0].get("count", 0) if j.get("likes") else 0
            return int(likes or 0)
        except Exception as e:
            log.warning("bluesky likes failed for %s (attempt %d): %s",
                        uri, attempt + 1, e)
    return None


def _normalize_picks(picks):
    """Keep only picks with the fields the downstream code reads.

    Ollama's structured output isn't strictly enforced, and a repaired
    truncated response can leave a pick missing a field. Rather than crash the
    activity on a KeyError, drop the malformed pick and default missing scores
    to 0 so a partial response still produces a usable run.
    """
    out = []
    for p in picks:
        if not isinstance(p, dict) or not p.get("title") or not p.get("link"):
            continue
        try:
            score = int(p.get("score", 0))
        except (TypeError, ValueError):
            score = 0
        out.append({"title": str(p["title"]), "link": str(p["link"]),
                    "why_it_fits": str(p.get("why_it_fits", "")), "score": score})
    return out


@activity.defn
async def analyze(brand: dict, items: list[dict], learn: dict | None = None) -> list[dict]:
    schema = {"type": "object", "properties": {"picks": {"type": "array", "items": {
        "type": "object", "properties": {
            "title": {"type": "string"}, "link": {"type": "string"},
            "why_it_fits": {"type": "string"}, "score": {"type": "integer"}},
        "required": ["title", "link", "why_it_fits", "score"]}}}, "required": ["picks"]}
    prompt = (f"You are a social media strategist for {brand['name']}: {brand['about']}\n"
              f"Audience: {brand['audience']}\n"
              f"{_worked_section(learn)}\n\n"
              "Headlines this week:\n" +
              "\n".join(f"- {i['title']} | {i['link']}" for i in items) +
              "\n\nPick the 3 headlines this brand can comment on credibly and usefully. "
              "Score 1-10 for audience relevance. Skip anything off-brand or risky. "
              "Prefer angles and phrasing that match what worked last week.")
    res = await asyncio.to_thread(llm_json, prompt, schema)
    return sorted(_normalize_picks(res.get("picks", [])),
                  key=lambda p: -p["score"])[:3]


@activity.defn
async def draft(brand: dict, picks: list[dict], learn: dict | None = None) -> list[dict]:
    schema = {"type": "object", "properties": {"posts": {"type": "array", "items": {
        "type": "object", "properties": {"text": {"type": "string"}, "source": {"type": "string"}},
        "required": ["text", "source"]}}}, "required": ["posts"]}
    mirror = ("Mirror the phrasing style and call-to-action format that worked "
              "last week." if learn and learn.get("summary") else
              "Mirror the brand's own voice and avoid format.")
    prompt = (f"Write one social post per topic for {brand['name']}.\n"
              f"Voice: {brand['voice']}\nNever: {brand.get('avoid', 'hype, emojis spam')}\n"
              f"{_worked_section(learn)}\n"
              "Max 280 characters each. Add a clear point of view and a soft call to action. "
              "Do not invent facts beyond the headline. " + mirror + "\n\nTopics:\n" +
              "\n".join(f"- {p['title']} ({p['why_it_fits']}) {p['link']}" for p in picks))
    res = await asyncio.to_thread(llm_json, prompt, schema)
    out = []
    for p in res.get("posts", []):
        if not isinstance(p, dict) or not p.get("text") or not p.get("source"):
            continue
        out.append({"text": str(p["text"])[:300], "source": str(p["source"])})
    return out


@activity.defn
async def publish(brand: dict, text: str) -> str:
    """Idempotent: a post is keyed by hash(client + text). Re-running a workflow
    (e.g. a Temporal retry) never double-posts.

    The digest is recorded as "pending" BEFORE the network call and flipped to
    "posted" after, so a crash between posting and recording can't double-post.
    On retry: a digest with a "posted" record is skipped outright; a lone
    "pending" record re-checks Bluesky (or, in dry-run, the client's own
    outbox) before anything is sent again.
    """
    slug = _slug(brand)
    digest = hashlib.sha256(f"{slug}:{text}".encode()).hexdigest()[:16]
    hist = os.path.join("clients", slug, "history.jsonl")
    os.makedirs(os.path.dirname(hist), exist_ok=True)
    records = _history_digests(hist, digest)
    if any(r.get("status") == "posted" for r in records):
        return f"already posted ({digest})"
    if not records:  # first attempt: write the pending marker before any network call
        _append(hist, {"ts": time.time(), "slug": slug, "digest": digest,
                       "text": text, "status": "pending"})
    if _already_on_bluesky(text):
        _append(hist, {"ts": time.time(), "slug": slug, "digest": digest,
                       "text": text, "status": "posted", "result": "found-on-bluesky"})
        return f"already posted ({digest})"
    result = await asyncio.to_thread(_post, text, slug)
    _append(hist, {"ts": time.time(), "slug": slug, "digest": digest,
                   "text": text, "status": "posted", "result": result})
    return result


def _append(path, record: dict) -> None:
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def _history_digests(hist: str, digest: str) -> list[dict]:
    """All history lines for one digest (client+text hash)."""
    out = []
    if os.path.exists(hist):
        with open(hist) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("digest") == digest:
                    out.append(r)
    return out


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


def _post(text: str, slug: str) -> str:
    h, pw = os.getenv("BSKY_HANDLE"), os.getenv("BSKY_APP_PASSWORD")
    if not (h and pw):  # dry run: safe default, still proves the pipeline
        box = os.path.join("clients", slug, "outbox.jsonl")
        os.makedirs(os.path.dirname(box), exist_ok=True)
        if os.path.exists(box):  # retry after a crash: same text already queued
            with open(box) as f:
                for line in f:
                    try:
                        if json.loads(line).get("text") == text:
                            return "dry-run: already in outbox"
                    except Exception:
                        pass
        with open(box, "a") as f:
            f.write(json.dumps({"ts": time.time(), "slug": slug, "text": text}) + "\n")
        return "dry-run: saved to clients/{slug}/outbox.jsonl"
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
        except asyncio.TimeoutError:
            pass  # state below is the source of truth
        if self._approved is None:  # never approved within 48h: end clean, post nothing
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


def _slug(brand: dict) -> str:
    return brand.get("slug") or brand["name"].lower().replace(" ", "-")


def _worked_section(learn: dict | None) -> str:
    """The 'what worked last week' block for the analyze/draft prompts. Empty
    when learn has no summary (cold start, not enough data, network failure),
    so those prompts skip the section entirely instead of echoing a null."""
    if learn and learn.get("summary"):
        return f"What worked last week: {learn['summary']}"
    return ""


async def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    client = await Client.connect("localhost:7233")
    if cmd == "worker":
        print(f"Worker up (model={MODEL}). Ctrl+C to stop.")
        await Worker(client, task_queue=QUEUE, workflows=[ContentRun],
                     activities=[research, learn, analyze, draft, publish]).run()
    elif cmd == "run":
        brand = _load_brand(sys.argv[2])
        wid = f"{_slug(brand)}-{datetime.now():%Y%m%d-%H%M}"
        await client.start_workflow(ContentRun.run, brand, id=wid, task_queue=QUEUE,
                                    memo={"client": _slug(brand)})
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
                        task_queue=QUEUE, memo={"client": slug}),
                    spec=ScheduleSpec(cron_expressions=["0 8 * * 1-5"]),
                    policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
                ),
            )
            print(f"Schedule {sid}: weekday 8am -> ContentRun for {brand['name']}")
    else:
        print(__doc__)

if __name__ == "__main__":
    asyncio.run(main())
