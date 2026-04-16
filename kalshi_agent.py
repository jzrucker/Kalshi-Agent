import os, time, logging, smtplib, requests, datetime, base64, re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("kalshi_log.txt"), logging.StreamHandler()])
log = logging.getLogger(__name__)

KALSHI_BASE     = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_KEY_ID   = os.environ.get("KALSHI_API_KEY_ID", "")
KALSHI_PRIV_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")
EMAIL_FROM      = os.environ.get("EMAIL_FROM", "")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_TO        = "jzrucker@gmail.com"

MIN_EDGE        = 0.05   # 5% minimum edge
MAX_POSITIONS   = 200    # max open positions across all markets
MAX_DATA_AGE    = 180    # minutes before weather data is stale

# Verified Kalshi series tickers from live site
KNOWN_SERIES = {
    "KXHIGHNY":   ("NYC",           "KNYC", 40.7829,  -73.9654),
    "KXHIGHMIA":  ("Miami",         "KMIA", 25.7959,  -80.2870),
    "KXHIGHLAX":  ("LA",            "KLAX", 33.9425,  -118.4081),
    "KXHIGHAUS":  ("Austin",        "KAUS", 30.1945,  -97.6699),
    "KXHIGHCHI":  ("Chicago",       "KMDW", 41.7868,  -87.7522),
    "KXHIGHTPHX": ("Phoenix",       "KPHX", 33.4373,  -112.0078),
    "KXHIGHTSFO": ("San Francisco", "KSFO", 37.6190,  -122.3750),
    "KXHIGHTATL": ("Atlanta",       "KATL", 33.6407,  -84.4277),
    "KXHIGHPHIL": ("Philadelphia",  "KPHL", 39.8729,  -75.2437),
    "KXHIGHTDC":  ("DC",            "KDCA", 38.8521,  -77.0377),
    "KXHIGHDEN":  ("Denver",        "KDEN", 39.8561,  -104.6737),
    "KXHIGHTSEA": ("Seattle",       "KSEA", 47.4502,  -122.3088),
    "KXHIGHDAL":  ("Dallas",        "KDFW", 32.8998,  -97.0403),
    "KXHIGHBOS":  ("Boston",        "KBOS", 42.3656,  -71.0096),
    "KXHIGHHOU":  ("Houston",       "KIAH", 29.9902,  -95.3368),
    "KXHIGHMSP":  ("Minneapolis",   "KMSP", 44.8848,  -93.2223),
    "KXHIGHDTW":  ("Detroit",       "KDTW", 42.2162,  -83.3554),
    "KXHIGHLAS":  ("Las Vegas",     "KLAS", 36.0840,  -115.1537),
    "KXHIGHNSH":  ("Nashville",     "KBNA", 36.1245,  -86.6782),
    "KXHIGHKCI":  ("Kansas City",   "KMCI", 39.2976,  -94.7139),
    "KXLOWTNYC":  ("NYC Low",       "KNYC", 40.7829,  -73.9654),
    "KXLOWTCHI":  ("Chicago Low",   "KMDW", 41.7868,  -87.7522),
    "KXLOWTAUS":  ("Austin Low",    "KAUS", 30.1945,  -97.6699),
}

def make_headers(method, path):
    try:
        key = serialization.load_pem_private_key(KALSHI_PRIV_KEY.encode(), password=None, backend=default_backend())
        ts  = str(int(time.time() * 1000))
        fp  = urlparse(KALSHI_BASE + path).path
        msg = (ts + method.upper() + fp).encode("utf-8")
        sig = key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return {"Content-Type": "application/json", "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
                "KALSHI-ACCESS-TIMESTAMP": ts, "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}
    except Exception as e:
        log.error(f"Auth error: {e}")
        return {}

def kget(path, params=None):
    try:
        r = requests.get(KALSHI_BASE + path, headers=make_headers("GET", path), params=params, timeout=10)
        if r.status_code == 200: return r.json()
        log.warning(f"GET {path} -> {r.status_code}")
    except Exception as e:
        log.error(f"GET error: {e}")
    return None

def kpost(path, body):
    try:
        r = requests.post(KALSHI_BASE + path, headers=make_headers("POST", path), json=body, timeout=10)
        if r.status_code in (200, 201): return r.json()
        log.warning(f"POST {path} -> {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.error(f"POST error: {e}")
    return None

def get_balance():
    d = kget("/portfolio/balance")
    return d.get("balance", 0) / 100.0 if d else 0.0

def get_positions():
    d = kget("/portfolio/positions")
    return {p["ticker"]: p for p in d.get("market_positions", [])} if d else {}

def discover_markets():
    """Find all open markets, filtered to today and tomorrow only."""
    now        = datetime.datetime.utcnow()
    today_utc  = now.date()
    cutoff     = today_utc + datetime.timedelta(days=2)
    found      = {}
    for series, info in KNOWN_SERIES.items():
        d = kget("/markets", params={"series_ticker": series, "status": "open", "limit": 100})
        if not d:
            continue
        mkts = d.get("markets", [])
        valid = []
        for m in mkts:
            close = m.get("close_time", "")
            try:
                close_d = datetime.datetime.fromisoformat(close.replace("Z", "+00:00")).date()
                if today_utc <= close_d < cutoff:
                    valid.append(m)
            except Exception:
                valid.append(m)
        if valid:
            found[series] = valid
            log.info(f"  {series} ({info[0]}): {len(valid)} markets")
    log.info(f"Total: {len(found)} series, {sum(len(v) for v in found.values())} markets")
    return found

def get_real_price(ticker):
    """
    Get real market price AND available depth from Kalshi orderbook.

    Returns:
        (price: float 0-1, available_contracts: int)
        price is None if no real market / stale 50-50.
        available_contracts is how many we can fill at the best price level.
        We return the YES-side depth since we're almost always buying YES.
        If buying NO, caller should use no_available instead — but for now
        we size against the YES depth conservatively.
    """
    try:
        r = requests.get(
            f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook",
            timeout=5
        )
        if r.status_code != 200:
            return None, 0
        ob = r.json().get("orderbook_fp", {})
        yes_bids = ob.get("yes_dollars", [])
        no_bids  = ob.get("no_dollars", [])

        # Helper: parse [price, qty] — qty may be absent on thin books
        def parse_level(level):
            price = float(level[0])
            qty   = int(level[1]) if len(level) > 1 else 999  # 999 = unknown, don't constrain
            return price, qty

        if yes_bids and no_bids:
            best_yes_p, yes_qty = parse_level(yes_bids[0])
            best_no_p,  no_qty  = parse_level(no_bids[0])
            mid = (best_yes_p + (1.0 - best_no_p)) / 2.0
            if abs(mid - 0.50) < 0.005:
                return None, 0
            # Return YES-side depth; if we end up buying NO, we'll use no_qty
            return round(mid, 4), yes_qty
        elif yes_bids:
            p, qty = parse_level(yes_bids[0])
            if abs(p - 0.50) < 0.005:
                return None, 0
            return round(p, 4), qty
        elif no_bids:
            p, qty = parse_level(no_bids[0])
            implied = 1.0 - p
            if abs(implied - 0.50) < 0.005:
                return None, 0
            return round(implied, 4), qty
    except Exception:
        pass
    return None, 0

def parse_range(title):
    """Parse temperature range from Kalshi market title."""
    t = title.lower()
    for ch in ["°", ",", "*"]:
        t = t.replace(ch, "")
    try:
        m = re.search(r'>\s*(\d+)', t)
        if m: return (int(m.group(1)), 9999)
        m = re.search(r'<\s*(\d+)', t)
        if m: return (-9999, int(m.group(1)))
        m = re.search(r'(\d+)-(\d+)', t)
        if m: return (int(m.group(1)), int(m.group(2)))
        m = re.search(r'(\d+)\s+to\s+(\d+)', t)
        if m: return (int(m.group(1)), int(m.group(2)))
        if "or above" in t or "or higher" in t:
            nums = re.findall(r'\d+', t)
            return (int(nums[0]), 9999) if nums else None
        if "or below" in t or "or lower" in t:
            nums = re.findall(r'\d+', t)
            return (-9999, int(nums[0])) if nums else None
    except Exception:
        pass
    return None

_wx = {}

def fetch_weather():
    """Fetch current obs + NWS forecast high for all cities."""
    global _wx
    now  = datetime.datetime.now(datetime.timezone.utc)
    seen = set()
    for series, info in KNOWN_SERIES.items():
        city, station, lat, lon = info
        if city in seen:
            continue
        seen.add(city)
        try:
            r = requests.get(
                f"https://api.weather.gov/stations/{station}/observations/latest",
                headers={"User-Agent": "kalshi-agent/1.0 jzrucker@gmail.com"},
                timeout=10
            )
            if r.status_code != 200:
                continue
            props  = r.json().get("properties", {})
            temp_c = props.get("temperature", {}).get("value")
            ts     = props.get("timestamp", "")
            if temp_c is None:
                continue
            cur_f = round(temp_c * 9 / 5 + 32, 1)
            try:
                obs_dt  = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age_min = int((now - obs_dt).total_seconds() / 60)
            except Exception:
                age_min = 60

            # NWS forecast high
            est_high = None
            try:
                pt = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                    headers={"User-Agent": "kalshi-agent/1.0 jzrucker@gmail.com"}, timeout=8)
                if pt.status_code == 200:
                    fc = requests.get(pt.json()["properties"]["forecastHourly"],
                        headers={"User-Agent": "kalshi-agent/1.0 jzrucker@gmail.com"}, timeout=8)
                    if fc.status_code == 200:
                        periods = fc.json()["properties"]["periods"]
                        today   = datetime.datetime.now().date()
                        temps   = [p["temperature"] for p in periods
                                   if datetime.datetime.fromisoformat(p["startTime"]).date() == today]
                        if temps:
                            est_high = max(temps)
            except Exception:
                pass

            if est_high is None:
                hr_et    = (now.hour - 4) % 24
                cushion  = max(0, 14 - max(hr_et, 10))
                est_high = round(cur_f + cushion, 1)

            _wx[city] = (est_high, cur_f, age_min)
            log.info(f"  {city}: {cur_f}F obs, {est_high}F high, {age_min}min old")
        except Exception as e:
            log.warning(f"  Weather error {city}: {e}")

def get_wx(city):
    return _wx.get(city, (None, None, None))

def true_prob(est_high, cur_obs, temp_range, hr_et):
    """Estimate true probability based on forecast vs range."""
    if est_high is None:
        return None
    low, high = temp_range
    ref = max(cur_obs, est_high) if hr_et >= 14 and cur_obs else est_high
    if high == 9999:
        diff = ref - low
    elif low == -9999:
        diff = high - ref
    else:
        diff = ref - (low + high) / 2.0
    if diff >= 5:  return 0.90
    if diff >= 3:  return 0.78
    if diff >= 1:  return 0.63
    if diff >= -1: return 0.50
    if diff >= -3: return 0.35
    if diff >= -5: return 0.20
    return 0.08

def size_order(mkt_p, edge, balance, available_contracts):
    """
    Size by dollar risk budget, scaled to probability.
    Maximizes position within budget and available market depth.

    Risk tiers (in dollars):
        <  3%  →  $3   risk  (deep longshot, ~$100+ payout)
        3-7%   →  $8   risk  (~$115-265 payout)
        7-15%  →  $25  risk  (~$165-355 payout)
        15-30% →  $40  risk  (~$130-265 payout)
        30%+   →  Kelly off balance, capped at $50 risk

    Contracts = risk_dollars / price_per_contract.
    Capped by:
      1. available_contracts in the orderbook at best price
      2. Hard max $75 risk per market (safety valve)
    """
    if mkt_p <= 0.03:
        risk_dollars = 3.0
    elif mkt_p <= 0.07:
        risk_dollars = 8.0
    elif mkt_p <= 0.15:
        risk_dollars = 25.0
    elif mkt_p <= 0.30:
        risk_dollars = 40.0
    else:
        # Kelly for near-50/50 markets
        p = mkt_p + edge
        q = 1 - p
        f = max(0, (p - q) * 0.5)
        risk_dollars = min(f * balance, 50.0)

    # Never risk more than $75 on a single market regardless of tier
    risk_dollars = min(risk_dollars, 75.0)

    # Contracts = how many $1-payout shares we can buy at this price
    ideal_contracts = max(1, int(risk_dollars / max(mkt_p, 0.01)))

    # Cap to whatever the book actually has at this price level
    # available_contracts=999 means we didn't get depth info — don't constrain
    if available_contracts < 999:
        ideal_contracts = min(ideal_contracts, available_contracts)

    return ideal_contracts

def place_order(ticker, side, contracts, price_cents):
    if side == "yes":
        price_field = {"yes_price": price_cents}
    else:
        price_field = {"no_price": price_cents}
    body = {
        "ticker": ticker, "action": "buy", "side": side, "count": int(contracts),
        "type": "limit",
        "expiration_ts": int((datetime.datetime.utcnow() + datetime.timedelta(minutes=5)).timestamp()),
        **price_field,
    }
    result = kpost("/portfolio/orders", body)
    if result:
        log.info(f"  ORDER {side.upper()} {contracts}x {ticker} @ {price_cents}c")
        return result
    return None

def send_email(subject, body):
    if not EMAIL_FROM or not EMAIL_PASSWORD:
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"]   = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_FROM, EMAIL_PASSWORD)
            s.send_message(msg)
        log.info(f"Email sent: {subject}")
    except Exception as e:
        log.error(f"Email failed: {e}")

def run():
    log.info("=" * 60)
    log.info(f"KALSHI WEATHER AGENT v4 | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    balance   = get_balance()
    positions = get_positions()
    log.info(f"Balance: ${balance:.2f} | Open positions: {len(positions)} | Min edge: {MIN_EDGE*100:.0f}%")

    log.info("Fetching weather...")
    fetch_weather()

    log.info("Discovering markets...")
    all_markets = discover_markets()

    hr_et    = (datetime.datetime.utcnow().hour - 4) % 24
    trades   = []
    skipped  = 0
    deployed = 0.0

    for series, markets in all_markets.items():
        info = KNOWN_SERIES.get(series)
        if not info:
            continue
        city, station, lat, lon = info
        est_high, cur_obs, age_min = get_wx(city)

        if est_high is None:
            continue
        if age_min is not None and age_min > MAX_DATA_AGE:
            continue

        # Sort: today's markets first
        today_date = datetime.datetime.now().strftime("%Y-%m-%d")
        markets_sorted = sorted(markets, key=lambda m: 0 if today_date in m.get("close_time","") else 1)

        log.info(f"\n-- {city} | high={est_high}F cur={cur_obs}F --")

        for mkt in markets_sorted:
            ticker = mkt.get("ticker", "")
            title  = mkt.get("title", "")

            if ticker in positions:
                continue
            if len(positions) + len(trades) >= MAX_POSITIONS:
                log.info(f"  Max positions reached ({len(positions)} open + {len(trades)} this run = {MAX_POSITIONS})")
                break

            rng = parse_range(title)
            if not rng:
                continue

            # Get real price + orderbook depth
            mkt_p, available = get_real_price(ticker)
            if mkt_p is None:
                skipped += 1
                continue

            true_p = true_prob(est_high, cur_obs, rng, hr_et)
            if true_p is None:
                continue

            edge_y = true_p - mkt_p
            edge_n = (1 - true_p) - (1 - mkt_p)

            if abs(edge_y) >= abs(edge_n):
                side, edge, p = "yes", edge_y, true_p
            else:
                side, edge, p = "no", edge_n, 1 - true_p

            log.info(f"  {title[:50]} | mkt={mkt_p:.2f} true={true_p:.2f} edge={edge:+.2f} [{side}] depth={available}")

            if abs(edge) < MIN_EDGE:
                skipped += 1
                continue

            contracts = size_order(mkt_p, edge, balance, available)
            price_c   = max(1, min(99, round(mkt_p * 100) + (1 if side == "yes" else 0)))
            cost      = contracts * mkt_p

            result = place_order(ticker, side, contracts, price_c)
            if result:
                deployed += cost
                positions[ticker] = result
                trades.append(
                    f"  {side.upper()} {contracts}x {city} | {title[:40]} | "
                    f"edge={edge:+.2f} mkt={mkt_p:.2f} depth={available} cost=${cost:.2f} max_payout=${contracts:.0f}"
                )

    final_bal = get_balance()
    body      = (
        f"Kalshi Weather Agent v4 — Daily Summary\n{'='*40}\n"
        f"Time:     {datetime.datetime.now().strftime('%Y-%m-%d %H:%M ET')}\n"
        f"Balance:  ${final_bal:.2f}\n"
        f"Deployed today: ${deployed:.2f}\n\n"
        f"TRADES ({len(trades)}):\n" + ("\n".join(trades) if trades else "  None") +
        f"\n\nSkipped (no price/edge): {skipped}\n"
        f"Series scanned: {len(all_markets)} | Min edge: {MIN_EDGE*100:.0f}%\n"
    )
    log.info(f"\n{body}")

    # Email once per day at ~6pm ET, only if trades were placed today
    now_et_hour = (datetime.datetime.utcnow().hour - 4) % 24
    if trades and 17 <= now_et_hour <= 19:
        subject = f"Kalshi Daily | {len(trades)} trades | ${deployed:.2f} deployed | {datetime.datetime.now().strftime('%Y-%m-%d')}"
        send_email(subject, body)

if __name__ == "__main__":
    run()
