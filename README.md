# Social Hustle

A done-for-you social media engine for small-business clients. It researches
the week's news, ranks angles with a local LLM, drafts posts in the client's
own voice, and waits for human approval before posting — all on a durable
Temporal workflow so retries and approvals never lose state.

Open-source AI is the core: Gemma runs locally via Ollama, so client data
stays on your box and every post costs $0. No closed APIs.

```
research (Google News RSS) -> analyze (Gemma ranks angles)
  -> draft (Gemma writes in the client's voice) -> HUMAN APPROVAL
  -> publish (Bluesky; dry-run outbox if no creds)
```

## Setup

```bash
pip install temporalio fastapi uvicorn
ollama pull gemma3:4b            # or gemma3:12b / gemma3:1b
```

## Run (4 terminals)

```bash
# terminal 1 — the LLM
ollama serve

# terminal 2 — the workflow engine (http://localhost:7233, UI at :8233)
temporal server start-dev

# terminal 3 — the worker that runs the workflow
python social_hustle.py worker

# terminal 4 — start a run for a client, or run the dashboard instead
python social_hustle.py run clients/brew-lane/brand.json
# -> Started brew-lane-20261017-1101
# -> Next: python social_hustle.py show brew-lane-20261017-1101
```

Watch progress in the Temporal UI at **http://localhost:8233** — you'll see the
`research` → `analyze` → `draft` activities light up, then the workflow waits
on your approval signal.

## Approve and publish

```bash
python social_hustle.py show brew-lane-20261017-1101   # review the drafts
python social_hustle.py approve brew-lane-20261017-1101 0 2   # publish #0 and #2
```

Approval is a durable signal, so you can approve hours (or days) later — the
workflow never loses state. Drafts you don't tick are simply not posted.

## Dashboard

```bash
python web.py          # http://localhost:8000 — list clients, start runs, approve
```

The dashboard is the full UI for terminal 4. It lists your clients, starts a
run with one click ("Generate this week's posts"), polls drafts as they
appear, and publishes the ones you tick. Same endpoints as the CLI, so you can
mix the two.

## Faster model

`gemma3:4b` is accurate but slow (~2 min/call on a modest box, ~4 min per
full run). Pull a smaller model to speed it up:

```bash
ollama pull gemma3:1b
MODEL=gemma3:1b python social_hustle.py worker
```

`gemma3:1b` is ~10× faster and still good enough for picking angles and
writing short posts. Bump it back up to `gemma3:4b` (or `gemma3:12b`) when
you want quality over speed.

FastAPI + one static HTML file, no build step, no JS framework.

## Schedule

```bash
python social_hustle.py schedule        # weekday 8am ContentRun per client
```

## Bluesky posting

Set creds to actually post; leave them unset for a **dry run** that writes to
`clients/<slug>/outbox.jsonl` instead:

```bash
export BSKY_HANDLE=you.bsky.social
export BSKY_APP_PASSWORD=xxxxxxx       # bsky.app settings -> app passwords
```

Posting is idempotent: a post is keyed by `hash(client + text)`, so a Temporal
retry never double-posts.

## Configuration

| Env var | Default | Effect |
|---|---|---|
| `MODEL` | `gemma3:4b` | Ollama model to use |
| `MOCK_LLM` | `0` | `1` skips Ollama, uses canned responses (smoke tests, demos) |
| `LLM_TIMEOUT` | `600` | per-call Ollama timeout in seconds |
| `BSKY_HANDLE` / `BSKY_APP_PASSWORD` | — | Bluesky creds; unset = dry-run |
| `DASH_TOKEN` | — | dashboard auth; unset = auth disabled |
| `TEMPORAL_HOST` | `localhost:7233` | where web.py connects |
| `PORT` / `HOST` | `8000` / `127.0.0.1` | web.py bind |
| `LOG_LEVEL` | `INFO` | log verbosity |

## Tests

```bash
pytest -q          # 30 passed — Ollama, HTTP, and Temporal are all mocked
```

## End-to-end smoke

```bash
python smoke.py    # MOCK_LLM=1 by default; real Ollama: MODEL=gemma3:4b python smoke.py
```

## Layout

```
social_hustle.py   workflow ContentRun + activities (research/analyze/draft/publish) + CLI
web.py             FastAPI dashboard
smoke.py           full end-to-end run through Temporal's in-process test env
static/            dashboard HTML + CSS
tests/             pytest suite (network-free)
clients/<slug>/    per-client brand.json, outbox.jsonl, history.jsonl
```

Per-client data lives in `clients/<slug>/` and is never mixed between clients.