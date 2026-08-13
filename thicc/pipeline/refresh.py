#!/usr/bin/env python3
"""THICC pipeline — pools.trade locked-liquidity tracker (Robinhood Chain, 4663).

Stdlib only. All HTTP via curl subprocess (RHC RPC / pools.trade 403 python UAs;
curl is proven from both local and GitHub Actions runners).

Data model (thicc/data/):
  state.json       pipeline cursor + legacy position map (committed; not read by UI)
  flywheel.json    per-token on-chain event history + aggregates + global tape
  series.json      per-token depth/liq snapshots (accrues from first run)
  leaderboard.json the UI's primary fetch: ranked tokens + windows + ecosystem
  digest.txt       human-readable daily digest (content engine)

Event sources (verified on-chain 2026-08-13, see methodology):
  Claimed(uint256 idx tokenId, uint256 c0, uint256 c1, PoolKey poolKey)
    topic0 a13569ec... — emitted by every BaseClaimRecipient subclass:
      compounder 0xf9526dd3 -> flywheel compound  (kind 'c')
      FEEB vaults d35e9ca7/587d2fdd -> creator claim (kind 'r')
      buyback&burn a1ba4cc1 -> burn-machine claim  (kind 'b')
      vesting ef451b29 / unknown -> other          (kind 'o')
    poolKey.currency1 (data word 3) = the token address. currency0 = native ETH.
  FeesCollected(uint256 idx tokenId, address idx tokenOrCaller, uint256 a0, uint256 a1)
    topic0 1df95d00... — new splitters index the TOKEN; the legacy locker
    (0x7198c32a, FRONG era) indexes the CALLER, so legacy rows resolve token
    via POSM.getPoolAndPositionInfo(tokenId). Legacy flywheel = 60% of ETH side
    + 100% of token side (fee routing verified on-chain 2026-08-03).
"""
import json, os, subprocess, sys, time, math
from decimal import Decimal, getcontext

getcontext().prec = 80

RPC = "https://rpc.mainnet.chain.robinhood.com"
API = "https://pools.trade/api/trpc/"

TOPIC_CLAIMED = "0xa13569eccad8e9d7eed1a66b1944fe2a6ff946072c636c0a8c5af0ac6c24be5b"
TOPIC_FEES    = "0x1df95d0058852523ea13aa1809224af942bd446ce86d3e431bf193c2bec269f1"

COMPOUNDER = "0xf9526dd3361fe0ba6b7a99533ed471d3e808e99a"
VAULTS     = {"0xd35e9ca72f64c7f93be30fad67524323396b36d7",
              "0x587d2fdddf14f6f84022b51e8c3a473eb88c4544"}
BUYBACK    = "0xa1ba4cc12654d2b188e3ba77dc86c75ca47f1a4e"
LOCKER_OLD = "0x7198c32a497c09497e04c86cf8f77a244a9e4b8f"
POSM       = "0x58daec3116aae6d93017baaea7749052e8a04fa7"
STATEVIEW  = "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b"
SEL_POOL_INFO  = "0x7ba03aad"  # getPoolAndPositionInfo(uint256)
SEL_GET_SLOT0  = "0xc815641c"  # getSlot0(bytes32)
SEL_GET_LIQ    = "0xfa6793d5"  # getLiquidity(bytes32)

GENESIS_BLOCK = 22754669       # legacy locker deploy, 2026-07-29 (pre-FRONG)
CONFIRM_LAG   = 12
CHUNK_MAX     = 500_000
CHUNK_MIN     = 20_000
LEGACY_ETH_RECYCLE_NUM, LEGACY_ETH_RECYCLE_DEN = 6, 10  # 60% of ETH side relocked

# instant-launch curve position bounds (verified constants; strategy source)
Q96 = 2**96
SQRT_PU = int((Decimal("1.0001") ** 198050).sqrt() * Q96)
SQRT_PL = int((Decimal("1.0001") ** Decimal(-160100)).sqrt() * Q96)

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data")

SORTS = ["volume", "trending", "recency", "linked-x"]

# dust pruning: fold per-token history into an aggregate once a token has been
# quiet this long AND its lifetime totals are below this floor. Bounds
# flywheel.json to ACTIVE tokens (the chain mints ~3k dust launches/day);
# ecosystem totals stay exact because the aggregate keeps every wei.
PRUNE_AGE_S = 10 * 86400
PRUNE_ETH_WEI = 5 * 10**15   # 0.005 ETH lifetime across fly+creator+burn
TIERS = [(500_000, "ABSOLUTE UNIT"), (100_000, "THICC"), (10_000, "THICK"), (0, "SLIM")]

def log(*a):
    print("[thicc]", *a, file=sys.stderr, flush=True)

# ---------------------------------------------------------------- HTTP / RPC

def curl(url, post=None, timeout=60, retries=3):
    cmd = ["curl", "-sS", "-m", str(timeout), "-H", "User-Agent: Mozilla/5.0 (thicc)", url]
    if post is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(post)]
    last = None
    for i in range(retries):
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            try:
                return json.loads(r.stdout)
            except json.JSONDecodeError:
                last = f"bad json: {r.stdout[:200]}"
        else:
            last = r.stderr.strip()[:200] or f"exit {r.returncode}"
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"curl failed for {url[:120]}: {last}")

_last_rpc = [0.0]
# pacing between RPC hits; overridable for heavy local runs (Actions default is fine:
# fresh runner IPs + small incremental windows)
RPC_GAP = float(os.environ.get("THICC_RPC_GAP", "0.45"))
RATE_ERRORS = ("Too Many Requests", "429")

def _rpc_send(payload, timeout, rate_attempts=12):
    """One JSON-RPC POST with pacing + rate-limit-aware retry (wait, never shrink)."""
    for attempt in range(rate_attempts):
        wait = _last_rpc[0] + RPC_GAP - time.time()
        if wait > 0:
            time.sleep(wait)
        _last_rpc[0] = time.time()
        try:
            d = curl(RPC, payload, timeout, retries=1)
        except RuntimeError as e:
            if attempt == rate_attempts - 1:
                raise
            time.sleep(min(60, 5 * (attempt + 1)))
            continue
        msg = ""
        if isinstance(d, dict) and "error" in d:
            msg = d["error"].get("message", "")
        elif isinstance(d, list):
            errs = [x["error"].get("message", "") for x in d if isinstance(x, dict) and "error" in x]
            msg = errs[0] if errs else ""
        if any(s in msg for s in RATE_ERRORS):
            back = min(120, 10 * (attempt + 1))
            log(f"rate limited, backing off {back}s")
            time.sleep(back)
            continue
        return d
    raise RuntimeError("rpc: rate limited after retries")

def rpc(method, params, timeout=60):
    d = _rpc_send({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout)
    if "error" in d:
        raise RuntimeError(f"rpc {method}: {d['error'].get('message','')[:200]}")
    return d["result"]

_batch_mode = ["full"]  # sticky per-run: 'full' -> 'ten' -> 'single'

def rpc_batch(calls, timeout=90):
    """calls: list of (method, params). Returns results in order; raises on any error.
    The RHC limiter weighs batch size (single calls pass when batches don't), so probe
    each batch size with only 2 attempts, downshift on rejection, and REMEMBER the
    working mode for the rest of the run — re-probing dead levels costs minutes each."""
    if os.environ.get("THICC_SINGLES"):
        _batch_mode[0] = "single"
    if _batch_mode[0] == "full":
        try:
            return _rpc_batch_once(calls, timeout, rate_attempts=2)
        except RuntimeError as e:
            if "rate limited" not in str(e):
                raise
            _batch_mode[0] = "ten"
            log(f"batch({len(calls)}) rate-rejected, run downshifts to 10")
    if _batch_mode[0] == "ten":
        try:
            out = []
            for i in range(0, len(calls), 10):
                out.extend(_rpc_batch_once(calls[i:i + 10], timeout, rate_attempts=2))
            return out
        except RuntimeError as e:
            if "rate limited" not in str(e):
                raise
            _batch_mode[0] = "single"
            log("10-batches rate-rejected, run downshifts to singles")
    return [rpc(m, p, timeout) for m, p in calls]

def _rpc_batch_once(calls, timeout, rate_attempts=12):
    payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]
    d = _rpc_send(payload, timeout, rate_attempts=rate_attempts)
    if isinstance(d, dict):
        raise RuntimeError(f"rpc batch: {json.dumps(d)[:200]}")
    out = [None] * len(calls)
    for item in d:
        if "error" in item:
            raise RuntimeError(f"rpc batch item: {item['error'].get('message','')[:200]}")
        out[item["id"]] = item["result"]
    return out

# ---------------------------------------------------------------- log scan

def get_logs_range(topic, frm, to, address=None):
    """Adaptive-chunk topic scan (optionally address-filtered). Returns raw log dicts.
    Shrinks the range only on timeout / result-cap errors; 429s are handled by waiting
    inside _rpc_send (shrinking on rate limits multiplies the request count)."""
    out, chunk, cur = [], CHUNK_MAX, frm
    while cur <= to:
        end = min(cur + chunk - 1, to)
        flt = {"fromBlock": hex(cur), "toBlock": hex(end), "topics": [topic]}
        if address:
            flt["address"] = address
        try:
            res = rpc("eth_getLogs", [flt], timeout=70)
            out.extend(res)
            cur = end + 1
            chunk = min(CHUNK_MAX, int(chunk * 3 // 2))
        except RuntimeError as e:
            if chunk <= CHUNK_MIN:
                raise
            chunk = max(CHUNK_MIN, chunk // 2)
            log(f"getLogs shrink to {chunk} after: {str(e)[:120]}")
    return out

def words(data_hex):
    h = data_hex[2:]
    return [h[i:i + 64] for i in range(0, len(h), 64)]

def addr_of(word):
    return "0x" + word[-40:]

# ---------------------------------------------------------------- decoding

def parse_events(logs_claimed, logs_fees, legacy_map):
    """Returns (rows, legacy_pending, collected)
    rows: list of dicts {blk, txh, kind, token, tokenId, eth, tok, era, emitter}
    legacy_pending: set of unresolved legacy tokenIds
    """
    rows, legacy_pending = [], set()
    for lg in logs_claimed:
        em = lg["address"].lower()
        w = words(lg["data"])
        if len(w) < 7:
            continue
        tokenId = int(lg["topics"][1], 16)
        eth, tok = int(w[0], 16), int(w[1], 16)
        token = addr_of(w[3]).lower()
        if em == COMPOUNDER:
            kind = "c"
        elif em in VAULTS:
            kind = "r"
        elif em == BUYBACK:
            kind = "b"
        else:
            kind = "o"
        rows.append(dict(blk=int(lg["blockNumber"], 16), txh=lg["transactionHash"],
                         kind=kind, token=token, tokenId=tokenId, eth=eth, tok=tok,
                         era="N", emitter=em))
    for lg in logs_fees:
        em = lg["address"].lower()
        w = words(lg["data"])
        if len(w) < 2:
            continue
        tokenId = int(lg["topics"][1], 16)
        a0, a1 = int(w[0], 16), int(w[1], 16)
        blk = int(lg["blockNumber"], 16)
        if em == LOCKER_OLD:
            token = legacy_map.get(str(tokenId))
            if token is None:
                legacy_pending.add(tokenId)
            # legacy flywheel: 60% of ETH side + 100% of token side is relocked
            rows.append(dict(blk=blk, txh=lg["transactionHash"], kind="c",
                             token=token, tokenId=tokenId,
                             eth=a0 * LEGACY_ETH_RECYCLE_NUM // LEGACY_ETH_RECYCLE_DEN,
                             tok=a1, era="L", emitter=em))
            rows.append(dict(blk=blk, txh=lg["transactionHash"], kind="f",
                             token=token, tokenId=tokenId, eth=a0, tok=a1,
                             era="L", emitter=em))
        else:
            token = ("0x" + lg["topics"][2][-40:]).lower()
            rows.append(dict(blk=blk, txh=lg["transactionHash"], kind="f",
                             token=token, tokenId=tokenId, eth=a0, tok=a1,
                             era="N", emitter=em))
    return rows, legacy_pending

def resolve_legacy(token_ids, weights=None):
    """tokenId -> token address via POSM.getPoolAndPositionInfo. Return {str(id): addr|None}.
    When the pending set is huge (the one-time backfill: ~4k legacy positions, nearly all
    dead dust), resolve by descending ETH weight until 99.5% of legacy ETH is attributed;
    the rest map to None and their amounts land in the 'unattributed legacy' bucket, so
    ecosystem totals stay complete while per-token history exists for every live token."""
    ids = list(token_ids)
    skipped = []
    if weights and len(ids) > 500:
        total = sum(weights.get(t, 0) for t in ids) or 1
        ids.sort(key=lambda t: -weights.get(t, 0))
        acc, cut = 0, len(ids)
        for i, t in enumerate(ids):
            acc += weights.get(t, 0)
            if acc >= total * 0.995 and i >= 99:
                cut = i + 1
                break
        skipped = ids[cut:]
        ids = ids[:cut]
        log(f"legacy resolve: {len(ids)} ids cover 99.5% of legacy ETH, "
            f"{len(skipped)} dust ids -> unattributed")
    out = {str(t): None for t in skipped}
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        calls = [("eth_call", [{"to": POSM, "data": SEL_POOL_INFO + format(t, "064x")}, "latest"])
                 for t in chunk]
        # a failure here must be fatal: silently dropping legacy rows would lose
        # FRONG-era history for good once the cursor advances
        res = rpc_batch(calls)
        for t, r in zip(chunk, res):
            if r and len(r) >= 2 + 64 * 6:
                w = words(r)
                cur0, cur1 = addr_of(w[0]), addr_of(w[1])
                # curve pools are native-paired: currency0 == 0x0
                out[str(t)] = cur1.lower() if int(cur0, 16) == 0 else None
            else:
                out[str(t)] = None
    return out

def _fetch_ts_exact(blocks):
    out = {}
    for i in range(0, len(blocks), 40):
        chunk = blocks[i:i + 40]
        res = rpc_batch([("eth_getBlockByNumber", [hex(b), False]) for b in chunk])
        for b, r in zip(chunk, res):
            out[b] = int(r["timestamp"], 16)
    return out

def block_timestamps(blocks):
    """distinct block numbers -> {blk: unix_ts}.
    Small sets (every incremental run) are fetched exactly. Large sets (the one-time
    backfill) use ~1200 exact anchor blocks + linear interpolation between them:
    timestamps land within minutes, which only day-buckets historical rollups —
    every AMOUNT stays exact. Newest 200 blocks are always exact (tape display)."""
    blocks = sorted(set(blocks))
    if not blocks:
        return {}
    if len(blocks) <= 400:
        return _fetch_ts_exact(blocks)
    log(f"timestamps: {len(blocks)} blocks -> anchor interpolation")
    n_anchor = 700
    stride = max(1, len(blocks) // n_anchor)
    anchors = sorted(set(blocks[::stride]) | {blocks[0], blocks[-1]} | set(blocks[-100:]))
    ts = _fetch_ts_exact(anchors)
    out = dict(ts)
    aks = sorted(ts)
    ai = 0
    for b in blocks:
        if b in out:
            continue
        while ai + 1 < len(aks) and aks[ai + 1] < b:
            ai += 1
        lo, hi = aks[ai], aks[min(ai + 1, len(aks) - 1)]
        if hi == lo:
            out[b] = ts[lo]
        else:
            out[b] = ts[lo] + (ts[hi] - ts[lo]) * (b - lo) // (hi - lo)
    return out

# ---------------------------------------------------------------- pools.trade API

def fetch_api_tokens():
    """Merged records across the 4 sorts (dedup by tokenAddress, first-sort priority)."""
    merged, order = {}, []
    for s in SORTS:
        inp = json.dumps({"0": {"sortBy": s}}, separators=(",", ":"))
        from urllib.parse import quote
        url = f"{API}curve.listLaunchesDeep?batch=1&input={quote(inp)}"
        try:
            d = curl(url, timeout=60)
        except RuntimeError as e:
            log(f"API sort {s} failed:", e)
            continue
        for rec in _find_records(d):
            a = rec.get("tokenAddress", "").lower()
            if a and a not in merged:
                merged[a] = rec
                order.append(a)
    return merged

def _find_records(o):
    if isinstance(o, dict):
        if "tokenAddress" in o:
            return [o]
        return [r for v in o.values() for r in _find_records(v)]
    if isinstance(o, list):
        return [r for v in o for r in _find_records(v)]
    return []

# ---------------------------------------------------------------- depth math

def eth_depth_of(sqrtP, L):
    """ETH (currency0) held by curve-range liquidity at current sqrtPriceX96. Exact ints."""
    if L == 0 or sqrtP <= 0:
        return 0, 0
    sp = min(max(sqrtP, SQRT_PL), SQRT_PU)
    eth = L * (SQRT_PU - sp) * Q96 // (sp * SQRT_PU)
    tok = L * (sp - SQRT_PL) // Q96
    return eth, tok

def fetch_depths(pool_ids):
    """poolId(hex str) -> (sqrtP, tick, L). Batched StateView calls."""
    out = {}
    pids = list(pool_ids)
    for i in range(0, len(pids), 75):
        chunk = pids[i:i + 75]
        calls = []
        for p in chunk:
            pid = p[2:] if p.startswith("0x") else p
            calls.append(("eth_call", [{"to": STATEVIEW, "data": SEL_GET_SLOT0 + pid}, "latest"]))
            calls.append(("eth_call", [{"to": STATEVIEW, "data": SEL_GET_LIQ + pid}, "latest"]))
        res = rpc_batch(calls, timeout=90)
        for j, p in enumerate(chunk):
            s0, lq = res[2 * j], res[2 * j + 1]
            try:
                w = words(s0)
                sqrtP = int(w[0], 16)
                tick = int(w[1], 16)
                if tick >= 2**255:
                    tick -= 2**256
                L = int(lq, 16)
                out[p] = (sqrtP, tick, L)
            except Exception:
                pass
    return out

# ---------------------------------------------------------------- stores

def load(name, default):
    try:
        with open(os.path.join(DATA, name)) as f:
            return json.load(f)
    except Exception:
        return default

def atomic_write(name, obj, text=False):
    os.makedirs(DATA, exist_ok=True)
    p = os.path.join(DATA, name)
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        if text:
            f.write(obj)
        else:
            json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, p)

def compact_events(ev_rows, now):
    """rows [ts, kind, eth_str, tok_str, n] -> collapse rows older than 48h into
    per-(utc-day, kind) rollups. Deterministic, idempotent."""
    cutoff = now - 48 * 3600
    fresh = [r for r in ev_rows if r[0] > cutoff]
    old = [r for r in ev_rows if r[0] <= cutoff]
    daily = {}
    for ts, kind, eth, tok, n in old:
        day = ts - (ts % 86400)
        k = (day, kind)
        d = daily.setdefault(k, [day, kind, 0, 0, 0])
        d[2] += int(eth); d[3] += int(tok); d[4] += n
    rolled = [[d[0], d[1], str(d[2]), str(d[3]), d[4]] for d in
              sorted(daily.values(), key=lambda x: x[0])]
    return rolled + sorted(fresh, key=lambda r: r[0])

def window_sum(ev_rows, kind, since):
    tot = 0
    for r in ev_rows:
        if r[1] == kind and r[0] >= since:
            tot += int(r[2])
    return tot

# ---------------------------------------------------------------- main

def main():
    now = int(time.time())
    state = load("state.json", {"schema": 1, "last_block": GENESIS_BLOCK - 1,
                                "legacy_map": {}, "unknown_emitters": {}})
    fly = load("flywheel.json", {"tokens": {}, "tape": []})
    series = load("series.json", {})

    head = int(rpc("eth_blockNumber", []), 16)
    to_block = head - CONFIRM_LAG
    frm = state["last_block"] + 1
    if to_block < frm:
        log("no new blocks"); return 0

    # 1. scan events (cached to disk so a later-phase failure never re-scans) ----
    cache_p = os.path.join(DATA, ".scan_cache.json")
    lc = lf = None
    try:
        with open(cache_p) as f:
            cache = json.load(f)
        if cache.get("frm") == frm:
            lc, lf = cache["lc"], cache["lf"]
            to_block = cache["to"]
            log(f"reusing scan cache: Claimed={len(lc)} FeesLegacy={len(lf)} to={to_block:,}")
    except Exception:
        pass
    if lc is None:
        log(f"scanning blocks {frm:,} .. {to_block:,} ({to_block-frm+1:,})")
        lc = get_logs_range(TOPIC_CLAIMED, frm, to_block)
        # FeesCollected is only needed for the legacy locker (FRONG-era flywheel);
        # chain-wide it exceeds the RPC's 10k-logs cap and adds nothing Claimed lacks.
        lf = get_logs_range(TOPIC_FEES, frm, to_block, address=LOCKER_OLD)
        log(f"logs: Claimed={len(lc)} FeesCollected(legacy)={len(lf)}")
        os.makedirs(DATA, exist_ok=True)
        with open(cache_p, "w") as f:
            json.dump({"frm": frm, "to": to_block, "lc": lc, "lf": lf}, f)

    rows, legacy_pending = parse_events(lc, lf, state["legacy_map"])
    if legacy_pending:
        weights = {}
        for r in rows:
            if r["era"] == "L" and r["kind"] == "f":
                weights[r["tokenId"]] = weights.get(r["tokenId"], 0) + r["eth"]
        resolved = resolve_legacy(legacy_pending, weights)
        state["legacy_map"].update(resolved)
        for r in rows:
            if r["era"] == "L" and r["token"] is None:
                r["token"] = state["legacy_map"].get(str(r["tokenId"]))

    # 2. timestamps --------------------------------------------------------
    ts_map = block_timestamps([r["blk"] for r in rows]) if rows else {}

    # 3. merge into flywheel store ----------------------------------------
    for em in {r["emitter"] for r in rows if r["kind"] == "o"}:
        state["unknown_emitters"][em] = state["unknown_emitters"].get(em, 0) + 1
    tape = fly.get("tape", [])
    unatt = fly.setdefault("unattributed", {"fly_eth": "0", "collected_eth": "0", "n": 0})
    for r in sorted(rows, key=lambda r: r["blk"]):
        if r["token"] is None:
            if r["kind"] == "c":
                unatt["fly_eth"] = str(int(unatt["fly_eth"]) + r["eth"])
            elif r["kind"] == "f":
                unatt["collected_eth"] = str(int(unatt["collected_eth"]) + r["eth"])
            unatt["n"] += 1
            continue
        t = fly["tokens"].setdefault(r["token"], {"ev": [], "fly_eth": "0", "fly_tok": "0",
                                                  "creator_eth": "0", "collected_eth": "0",
                                                  "burn_eth": "0", "n_compounds": 0,
                                                  "first_seen": ts_map.get(r["blk"], now)})
        ts = ts_map.get(r["blk"], now)
        if r["kind"] != "f":
            t["ev"].append([ts, r["kind"], str(r["eth"]), str(r["tok"]), 1])
            tape.append([ts, r["kind"], r["token"], str(r["eth"]), str(r["tok"]), r["txh"]])
        if r["kind"] == "c":
            t["fly_eth"] = str(int(t["fly_eth"]) + r["eth"])
            t["fly_tok"] = str(int(t["fly_tok"]) + r["tok"])
            t["n_compounds"] += 1
        elif r["kind"] == "r":
            t["creator_eth"] = str(int(t["creator_eth"]) + r["eth"])
        elif r["kind"] == "b":
            t["burn_eth"] = str(int(t["burn_eth"]) + r["eth"])
        elif r["kind"] == "f":
            t["collected_eth"] = str(int(t["collected_eth"]) + r["eth"])
    for t in fly["tokens"].values():
        t["ev"] = compact_events(t["ev"], now)
    tape = sorted(tape, key=lambda r: r[0])[-80:]
    fly["tape"] = tape
    fly["updated"] = now

    # 4. API + depths ------------------------------------------------------
    api = fetch_api_tokens()
    if len(api) < 100:
        log(f"GATE: api merge too small ({len(api)}) — aborting without write")
        return 1
    ethusd_samples = sorted(
        rec["poolStats"]["priceUsd"] / rec["poolStats"]["priceEth"]
        for rec in api.values()
        if rec.get("poolStats", {}).get("priceEth") and rec["poolStats"].get("priceUsd"))
    ethusd = ethusd_samples[len(ethusd_samples) // 2]
    if not (500 < ethusd < 20000):
        log(f"GATE: implausible ETHUSD {ethusd} — aborting"); return 1

    # depth reads are the heaviest RPC phase; cap to the top pools by reported
    # liquidity (comfortably covers the 300-row leaderboard even in singles mode)
    by_liq = sorted(api.values(),
                    key=lambda r: -(r.get("poolStats", {}).get("liquidityUsd") or 0))
    depths = fetch_depths([rec["poolId"] for rec in by_liq[:450] if rec.get("poolId")])

    # 4b. fold dust tokens into the pruned aggregate (never prunes API-listed tokens;
    # a revived token restarts a fresh entry — understatement bounded by the tiny floor)
    pr = fly.setdefault("pruned", {"fly_eth": "0", "fly_tok": "0", "creator_eth": "0",
                                   "collected_eth": "0", "burn_eth": "0",
                                   "n_tokens": 0, "n_compounds": 0})
    prune_cutoff = now - PRUNE_AGE_S
    drop = []
    for a, t in fly["tokens"].items():
        if a in api:
            continue
        last = t["ev"][-1][0] if t.get("ev") else t.get("first_seen", 0)
        tot = (int(t.get("fly_eth", "0")) + int(t.get("creator_eth", "0"))
               + int(t.get("burn_eth", "0")))
        if last < prune_cutoff and tot < PRUNE_ETH_WEI:
            for k in ("fly_eth", "fly_tok", "creator_eth", "collected_eth", "burn_eth"):
                pr[k] = str(int(pr[k]) + int(t.get(k, "0")))
            pr["n_compounds"] += t.get("n_compounds", 0)
            pr["n_tokens"] += 1
            drop.append(a)
    for a in drop:
        del fly["tokens"][a]
    if drop:
        log(f"pruned {len(drop)} dust tokens into aggregate "
            f"(lifetime pruned: {pr['n_tokens']})")

    # 5. series ------------------------------------------------------------
    for a, rec in api.items():
        ps = rec.get("poolStats", {})
        liq = ps.get("liquidityUsd") or 0
        pid = rec.get("poolId")
        eth_d = None
        if pid in depths:
            sqrtP, _, L = depths[pid]
            eth_wei, _ = eth_depth_of(sqrtP, L)
            eth_d = round(eth_wei / 1e18, 6)
        if liq < 2000 and a not in series:
            continue
        s = series.setdefault(a, {"first": [now, round(liq), round(rec.get("fdvUsd") or 0)],
                                  "pts": []})
        s["pts"].append([now, round(liq), eth_d])
    cutoff48, cutoff35d = now - 48 * 3600, now - 35 * 86400
    for a, s in series.items():
        fresh = [p for p in s["pts"] if p[0] > cutoff48]
        older = [p for p in s["pts"] if cutoff35d < p[0] <= cutoff48]
        thinned, last = [], 0
        for p in older:
            if p[0] - last >= 7200:
                thinned.append(p); last = p[0]
        s["pts"] = thinned + fresh

    # 6. leaderboard -------------------------------------------------------
    rows_lb = []
    for a, rec in api.items():
        ps = rec.get("poolStats", {})
        pid = rec.get("poolId")
        eth_wei = tok_wei = None
        tick = None
        if pid in depths:
            sqrtP, tick, L = depths[pid]
            eth_wei, tok_wei = eth_depth_of(sqrtP, L)
        f = fly["tokens"].get(a, {})
        ev = f.get("ev", [])
        depth_eth = (eth_wei / 1e18) if eth_wei is not None else None
        depth_usd = depth_eth * ethusd if depth_eth is not None else None
        tok_usd = None
        if tok_wei is not None and ps.get("priceUsd"):
            tok_usd = tok_wei / 1e18 * ps["priceUsd"]
        tier = next(name for floor, name in TIERS if (depth_usd or 0) >= floor)
        created = rec.get("createdAt")
        # depth deltas from series
        s = series.get(a, {})
        deltas = {}
        for label, secs in (("d1", 86400), ("d7", 7 * 86400), ("d30", 30 * 86400)):
            base = None
            for p in s.get("pts", []):
                if p[0] <= now - secs and p[2] is not None:
                    base = p[2]
            deltas[label] = (round(depth_eth - base, 4)
                             if (base is not None and depth_eth is not None) else None)
        spark = [[p[0], p[2]] for p in s.get("pts", [])[-49:] if p[2] is not None]
        rows_lb.append({
            "a": a, "sym": rec.get("tokenSymbol"), "name": rec.get("tokenName"),
            "img": rec.get("imageUrl"), "emoji": rec.get("imageEmoji"),
            "hue": rec.get("imageHue"), "x": rec.get("xUrl"), "xv": rec.get("xVerified"),
            "created": created, "status": rec.get("status"),
            "fdv": ps.get("priceUsd") and rec.get("fdvUsd"), "liq_api": ps.get("liquidityUsd"),
            "vol24": ps.get("volume24hUsd"), "holders": rec.get("holderCount"),
            "price": ps.get("priceUsd"), "chg24": ps.get("priceChange24hPct"),
            "depth_eth": depth_eth and round(depth_eth, 4),
            "depth_usd": depth_usd and round(depth_usd),
            "tok_side_usd": tok_usd and round(tok_usd),
            "tick": tick, "tier": tier,
            "backing": (round(depth_usd / rec["fdvUsd"], 4)
                        if depth_usd and rec.get("fdvUsd") else None),
            "dd1": deltas["d1"], "dd7": deltas["d7"], "dd30": deltas["d30"],
            "fly_eth": round(int(f.get("fly_eth", "0")) / 1e18, 6),
            "fly_tok": round(int(f.get("fly_tok", "0")) / 1e18),
            "fly_d1": round(window_sum(ev, "c", now - 86400) / 1e18, 6),
            "fly_d7": round(window_sum(ev, "c", now - 7 * 86400) / 1e18, 6),
            "n_comp": f.get("n_compounds", 0),
            "creator_eth": round(int(f.get("creator_eth", "0")) / 1e18, 6),
            "burn_eth": round(int(f.get("burn_eth", "0")) / 1e18, 6),
            "collected_eth": round(int(f.get("collected_eth", "0")) / 1e18, 6),
            "spark": spark,
        })
    rows_lb.sort(key=lambda r: (r["depth_eth"] is None, -(r["depth_eth"] or 0)))
    for i, r in enumerate(rows_lb):
        r["rank"] = i + 1

    eco = {
        "eth_usd": round(ethusd, 2),
        "total_depth_eth": round(sum(r["depth_eth"] or 0 for r in rows_lb), 2),
        "total_depth_usd": round(sum(r["depth_usd"] or 0 for r in rows_lb)),
        "total_fly_eth": round((sum(int(t.get("fly_eth", "0")) for t in fly["tokens"].values())
                                + int(fly.get("unattributed", {}).get("fly_eth", "0"))
                                + int(pr["fly_eth"])) / 1e18, 4),
        "total_creator_eth": round((sum(int(t.get("creator_eth", "0")) for t in fly["tokens"].values())
                                    + int(pr["creator_eth"])) / 1e18, 4),
        "total_burn_eth": round((sum(int(t.get("burn_eth", "0")) for t in fly["tokens"].values())
                                 + int(pr["burn_eth"])) / 1e18, 4),
        "fly_24h_eth": round(sum(r["fly_d1"] for r in rows_lb), 4),
        "n_tokens_api": len(api),
        "n_tokens_fly": len(fly["tokens"]),
        "n_compounds": (sum(t.get("n_compounds", 0) for t in fly["tokens"].values())
                        + pr["n_compounds"]),
        "scan_from": GENESIS_BLOCK, "scan_to": to_block,
    }

    sym_by_addr = {r["a"]: (r["sym"] or "?") for r in rows_lb}
    tape_ui = [[r[0], r[1], r[2], sym_by_addr.get(r[2]), round(int(r[3]) / 1e18, 6),
                round(int(r[4]) / 1e18, 2), r[5]] for r in reversed(fly["tape"])]

    lb = {"updated": now, "eco": eco, "tape": tape_ui, "rows": rows_lb[:300]}

    # 7. digest ------------------------------------------------------------
    dg = [f"THICC digest {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))}",
          f"Locked forever on Robinhood Chain: {eco['total_fly_eth']} ETH recycled "
          f"across {eco['n_compounds']} compounds",
          f"Total curve depth: {eco['total_depth_eth']} ETH "
          f"(${eco['total_depth_usd']:,})", "", "THICCest pools:"]
    for r in rows_lb[:5]:
        dg.append(f"  #{r['rank']} {r['sym']}: {r['depth_eth']} ETH depth, "
                  f"tier {r['tier']}, flywheel {r['fly_eth']} ETH all-time")
    dg.append("")
    dg.append("Biggest flywheel adds (24h):")
    for r in sorted(rows_lb, key=lambda x: -x["fly_d1"])[:3]:
        if r["fly_d1"] > 0:
            dg.append(f"  {r['sym']}: +{r['fly_d1']} ETH locked in 24h")

    # 8. write everything --------------------------------------------------
    state["last_block"] = to_block
    atomic_write("state.json", state)
    try:
        os.remove(os.path.join(DATA, ".scan_cache.json"))
    except OSError:
        pass
    atomic_write("flywheel.json", fly)
    atomic_write("series.json", series)
    atomic_write("leaderboard.json", lb)
    atomic_write("digest.txt", "\n".join(dg) + "\n", text=True)
    log(f"done: {len(rows)} new events, {len(rows_lb)} ranked, "
        f"eco depth {eco['total_depth_eth']} ETH, fly {eco['total_fly_eth']} ETH")
    return 0

if __name__ == "__main__":
    sys.exit(main())
