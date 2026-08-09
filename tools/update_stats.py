#!/usr/bin/env python3
"""Refresh the live-market numbers hardcoded in the Wingman landing page.

Fetches pools.trade's public tRPC API (via curl — python-UA requests get 403'd),
computes the two hero stats + the FAQ launch-rate, and string-replaces them in
the target HTML file. Refuses to write if the fetched data fails sanity checks,
so a broken API can never blank the page.

Usage:  python3 tools/update_stats.py [path/to/landing.html]
Exit:   0 = updated (or already current), 1 = fetch/sanity failure (file untouched)
"""
import json, re, subprocess, sys, datetime, urllib.parse

API = "https://pools.trade/api/trpc/curve.listLaunchesDeep"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"

def fetch(sort_by):
    # tRPC v11 GET quirk: batch=1 + {"0":{...}} plain JSON (no "json" wrapper),
    # sortBy is REQUIRED. The non-batch form silently ignores the input.
    inp = urllib.parse.quote(json.dumps({"0": {"limit": 500, "sortBy": sort_by}}), safe="")
    out = subprocess.run(
        ["curl", "-s", "--max-time", "60", "-A", UA, f"{API}?batch=1&input={inp}"],
        capture_output=True, text=True, check=True).stdout
    data = json.loads(out)

    def find(o):
        if isinstance(o, list) and o and isinstance(o[0], dict) and "createdAt" in o[0]:
            return o
        if isinstance(o, dict):
            for v in o.values():
                r = find(v)
                if r is not None:
                    return r
        if isinstance(o, list):
            for v in o:
                r = find(v)
                if r is not None:
                    return r
        return None

    launches = find(data)
    if not launches:
        raise RuntimeError(f"no launch records in response ({out[:200]!r})")
    return launches

def parse_ts(t):
    if isinstance(t, (int, float)):
        return datetime.datetime.fromtimestamp(t / 1000 if t > 1e12 else t, datetime.timezone.utc)
    return datetime.datetime.fromisoformat(str(t).replace("Z", "+00:00"))

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "landing.html"

    top = fetch("volume")
    vol = round(sum(float((x.get("poolStats") or {}).get("volume24hUsd") or 0) for x in top))

    recent = fetch("recency")
    ts = [parse_ts(x["createdAt"]) for x in recent]
    span = abs((ts[0] - ts[-1]).total_seconds())
    rate = len(recent) / span * 86400
    rate_floor = int(rate // 100 * 100)          # 1,433/day -> "1,400+"

    # sanity: a broken/changed API must never write garbage into the page
    if not (100_000 <= vol <= 10_000_000_000):
        raise RuntimeError(f"volume {vol} outside sane range")
    if not (50 <= rate <= 100_000):
        raise RuntimeError(f"launch rate {rate:.0f}/day outside sane range")

    src = open(path).read()
    n0 = src

    src = re.sub(r'data-count="\d+" data-pre="\$" data-thousands="1">\$[\d,]+</div>',
                 f'data-count="{vol}" data-pre="$" data-thousands="1">${vol:,}</div>', src, count=1)
    src = re.sub(r'data-count="\d+" data-thousands="1" data-suf="\+">[\d,]+\+</div>',
                 f'data-count="{rate_floor}" data-thousands="1" data-suf="+">{rate_floor:,}+</div>', src, count=1)
    src = re.sub(r"around [\d,]+ launches a day",
                 f"around {rate_floor:,} launches a day", src, count=1)

    if src == n0:
        print(f"already current: vol=${vol:,} rate={rate_floor:,}+/day")
        return
    open(path, "w").write(src)
    print(f"updated: vol=${vol:,} rate={rate_floor:,}+/day (measured {rate:.0f}/day)")

if __name__ == "__main__":
    main()
