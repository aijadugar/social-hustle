# Social Hustle - project context

Done-for-you social media engine for small-business clients. Hackathon entry:
open-source AI must be the core. Sellable product, not a demo.

## Stack (do not swap without asking)
- LLM: Gemma via local Ollama (`MODEL` env, default gemma3:4b). No closed APIs.
- Orchestration: Temporal (Python SDK, `temporal server start-dev` on :7233).
- Posting: Bluesky (AT Protocol). Dry-run to outbox.jsonl when no creds.
- Web UI (to build): FastAPI + one static HTML file, no build step, no JS framework.

## Files
- social_hustle.py: workflow ContentRun, activities (research, analyze, draft, publish), CLI
- brand.json: per-client profile (name, about, audience, voice, avoid, keywords)

## Rules
- Keep it small: stdlib + temporalio + fastapi/uvicorn only. No new deps without a reason.
- Every LLM call uses Ollama structured output (JSON schema). Validate before use.
- Human approval stays in the loop. Never auto-publish.
- Publishing must be idempotent (don't double-post on Temporal retries).
- Per-client data lives in clients/<slug>/ (brand.json, outbox, history). Never mix clients.

## Commands
- Worker: `python social_hustle.py worker`
- Run:    `python social_hustle.py run clients/<slug>/brand.json`
- Tests:  `pytest -q` (mock Ollama and HTTP; no network in tests)

## Definition of done for any task
1. Runs end to end on a fresh clone following the README.
2. Tests pass. 3. README updated. 4. Small commit with a clear message.
