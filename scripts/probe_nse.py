"""Throwaway Step-0 probe for NSE internal endpoints. Not part of the package.

Usage: python scripts/probe_nse.py <step> [args]
Writes raw responses to $PROBE_OUT (default ./probe_out). Polite: >=1s between requests.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import httpx

BASE = "https://www.nseindia.com"
OUT = Path(os.environ.get("PROBE_OUT", "probe_out"))
OUT.mkdir(parents=True, exist_ok=True)
MIN_GAP_S = 1.0

FULL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{BASE}/",
}

_last = 0.0


def polite_get(client: httpx.Client, url: str, **kw) -> httpx.Response:
    global _last
    wait = MIN_GAP_S - (time.monotonic() - _last)
    if wait > 0:
        time.sleep(wait)
    try:
        r = client.get(url, **kw)
    finally:
        _last = time.monotonic()
    return r


def new_client(headers: dict[str, str] | None = None) -> httpx.Client:
    return httpx.Client(
        headers=FULL_HEADERS if headers is None else headers,
        http2=True,
        timeout=30,
        follow_redirects=True,
    )


def prime(client: httpx.Client, symbol: str = "BHARTIARTL") -> httpx.Response:
    r = polite_get(client, f"{BASE}/get-quote/equity/{symbol}/", headers={"Accept": "text/html,*/*"})
    print(f"prime {r.status_code} len={len(r.content)} cookies={sorted(client.cookies.keys())}")
    return r


def save(name: str, r: httpx.Response) -> None:
    (OUT / name).write_bytes(r.content)


def summarize(label: str, r: httpx.Response) -> None:
    ctype = r.headers.get("content-type", "")
    body = r.text[:300].replace("\n", " ")
    print(f"[{label}] {r.status_code} {ctype} len={len(r.content)} :: {body}")


def step_page(symbol: str = "BHARTIARTL") -> None:
    """Prime, save quote-page HTML, list script bundles."""
    with new_client() as c:
        r = prime(c, symbol)
        save("quote_page.html", r)
        scripts = sorted(set(re.findall(r'src="([^"]+\.js[^"]*)"', r.text)))
        print("\n".join(scripts))


def step_bundles(*urls: str) -> None:
    """Download given JS bundles and grep for functionName / api paths."""
    with new_client() as c:
        prime(c)
        for u in urls:
            full = u if u.startswith("http") else BASE + u
            r = polite_get(c, full)
            name = re.sub(r"[^A-Za-z0-9_.-]", "_", full.split("/")[-1].split("?")[0])[:80]
            save(f"bundle_{name}", r)
            print(f"{r.status_code} {full} len={len(r.content)}")


def step_get(path: str, params_json: str = "{}", label: str = "x") -> None:
    """Prime then GET one API path with JSON-encoded params."""
    params = json.loads(params_json)
    with new_client() as c:
        prime(c)
        r = polite_get(c, BASE + path, params=params)
        summarize(label, r)
        save(f"api_{label}.json", r)


def step_batch(spec_file: str) -> None:
    """Run many GETs in one primed session. spec: [[label, path, params], ...]"""
    spec = json.loads(Path(spec_file).read_text())
    with new_client() as c:
        prime(c)
        for label, path, params in spec:
            r = polite_get(c, BASE + path, params=params)
            summarize(label, r)
            save(f"api_{label}.json", r)


def step_headers() -> None:
    """Find the minimal header set: drop headers one at a time."""
    target = (f"{BASE}/api/corporate-announcements",
              {"index": "equities", "symbol": "BHARTIARTL"})
    variants: dict[str, dict[str, str]] = {"full": dict(FULL_HEADERS)}
    for k in FULL_HEADERS:
        variants[f"no_{k}"] = {h: v for h, v in FULL_HEADERS.items() if h != k}
    variants["ua_only"] = {"User-Agent": FULL_HEADERS["User-Agent"]}
    variants["none"] = {"User-Agent": "python-httpx"}
    for name, hdrs in variants.items():
        if name == "full":
            continue
        with new_client(hdrs) as c:
            try:
                r = polite_get(c, target[0], params=target[1])
                print(f"{name:20s} unprimed -> {r.status_code} len={len(r.content)}")
            except httpx.HTTPError as e:
                print(f"{name:20s} unprimed -> {type(e).__name__}: {e}")


if __name__ == "__main__":
    step, *args = sys.argv[1:]
    globals()[f"step_{step}"](*args)
