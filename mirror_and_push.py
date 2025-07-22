#!/usr/bin/env python3
import os
import json
import requests

# ─── CONFIG ──────────────────────────────────────────────────────────────────
PEGASUS_URL       = "https://qservices.pegasusgateway.com/installations/api/v1/installation"
BEARER_TOKEN      = os.getenv("PEGASUS_TOKEN")     # set this in your env
ZAPIER_HOOK_URL   = os.getenv("ZAPIER_HOOK_URL")  # your Catch Hook URL
MAX_BYTES         = 5 * 1024 * 1024               # ~5 MiB per chunk
# ────────────────────────────────────────────────────────────────────────────

def fetch_full_data():
    resp = requests.get(
        PEGASUS_URL,
        headers={"Authorization": f"Bearer {BEARER_TOKEN}"}, 
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()

def chunk_by_size(items, max_bytes=MAX_BYTES):
    """Yield lists of items whose JSON dump stays under max_bytes."""
    chunk = []
    size = 0
    for itm in items:
        b = json.dumps(itm, separators=(",",":"), ensure_ascii=False).encode("utf-8")
        if size + len(b) > max_bytes:
            yield chunk
            chunk, size = [], 0
        chunk.append(itm)
        size += len(b)
    if chunk:
        yield chunk

def push_chunks(chunks):
    for i, ch in enumerate(chunks, start=1):
        payload = {"batch_number": i, "items": ch}
        r = requests.post(ZAPIER_HOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        print(f"→ pushed batch {i} ({len(ch)} items)")

def main():
    data = fetch_full_data()
    print(f"Fetched {len(data)} total items")
    chunks = list(chunk_by_size(data))
    push_chunks(chunks)

if __name__ == "__main__":
    main()
