#!/usr/bin/env python3
"""End-to-end smoke test for Social Hustle.

No Temporal CLI needed: WorkflowEnvironment.start_local() downloads and runs
the dev server in-process. The worker and web.py are launched as subprocesses
against that server, then we drive a real run through generate -> drafts ->
approve -> publish (dry-run) and print each step's output.

  python smoke.py            # MOCK_LLM=1 canned responses
  MODEL=gemma3:4b python smoke.py   # real Ollama (needs ollama serve + model)
"""
import asyncio, os, signal, socket, subprocess, sys, time, urllib.request

os.environ.setdefault("MOCK_LLM", "1")
os.environ.setdefault("BSKY_HANDLE", "")
os.environ.setdefault("BSKY_APP_PASSWORD", "")

from temporalio.testing import WorkflowEnvironment
import social_hustle as sh

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def wait_http(url, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                return r.read()
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"timeout waiting for {url}")


async def main():
    env = await WorkflowEnvironment.start_local()
    addr = env.client.service_client.config.target_host
    print(f"Temporal dev server: {addr}")
    try:
        web_port = free_port()
        worker = subprocess.Popen(
            [sys.executable, "social_hustle.py", "worker"],
            env={**os.environ, "TEMPORAL_HOST": addr})
        web = subprocess.Popen(
            [sys.executable, "web.py"],
            env={**os.environ, "TEMPORAL_HOST": addr, "PORT": str(web_port)})
        try:
            wait_http(f"http://127.0.0.1:{web_port}/api/clients")
            print(f"web dashboard: http://127.0.0.1:{web_port}/")
            await _drive(env.client, web_port)
        finally:
            for p in (web, worker):
                p.send_signal(signal.SIGTERM)
                try: p.wait(timeout=10)
                except Exception: p.kill()
    finally:
        await env.shutdown()


async def wait_for_drafts(h, timeout=180):
    """Drafts appear only after research/learn/analyze/draft complete, so poll
    the query (it returns [] before the workflow sets them)."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            drafts = await h.query("drafts")
        except Exception as e:
            print(f"  (query retry: {e})")
            drafts = []
        if drafts:
            return drafts
        await asyncio.sleep(1)
    raise RuntimeError("drafts never appeared")


async def _drive(client, web_port):
    slug = "brew-lane"
    print("\n=== 1. start run ===")
    h = await client.start_workflow(
        sh.ContentRun.run, sh._load_brand(f"clients/{slug}/brand.json"),
        id=f"{slug}-smoke", task_queue=sh.QUEUE, memo={"client": slug})
    print(f"workflow_id: {h.id}")

    print("\n=== 2. drafts (query) ===")
    drafts = await wait_for_drafts(h)
    learn = await h.query("learn")
    for i, d in enumerate(drafts):
        print(f"[{i}] {d['text']}\n    source: {d['source']}")
    print(f"learn: {learn}")

    print("\n=== 3. approve [0] ===")
    await h.signal("approve", [0])
    res = await h.result()
    print(f"result: {res}")

    print("\n=== 4. web: list runs ===")
    print(wait_http(f"http://127.0.0.1:{web_port}/api/runs").decode())

    print("\n=== 5. web: drafts by id ===")
    print(wait_http(f"http://127.0.0.1:{web_port}/api/runs/{h.id}").decode())

    print("\n=== 6. outbox (dry-run) ===")
    box = f"clients/{slug}/outbox.jsonl"
    if os.path.exists(box):
        print(open(box).read())
    else:
        print(f"(no {box})")

    print("\nSMOKE OK")


if __name__ == "__main__":
    asyncio.run(main())