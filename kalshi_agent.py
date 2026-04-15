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

MAX_POSITION      = 25.00   # max $ per trade
MIN_EDGE          = 0.05    # 5% minimum edge
MAX_POSITIONS     = 20      # max simultaneous open positions
MAX_DATA_AGE      = 180     # minutes
MAX_PER_CITY      = 3       # max trades per city per run
TODAY_ONLY        = False   # trade today AND tomorrow

KNOWN_SERIES = {
    "KXHIGHNY":  ("NYC",          "KNYC", 40.7829, -73.9654),
    "KXHIGHCHI": ("Chicago",      "KMDW", 41.7868, -87.7522),
    "KXHIGHMIA": ("Miami",        "KMIA", 25.7959, -80.2870),
    "KXHIGHDEN": ("Denver",       "KDEN", 39.8561, -104.6737),
    "KXHIGHAUS": ("Austin",       "KAUS", 30.1945, -97.6699),
    "KXHIGHLA":  ("LA",           "KLAX", 33.9425, -118.4081),
    "KXHIGHATL": ("Atlanta",      "KATL", 33.6407, -84.4277),
    "KXHIGHDAL": ("Dallas",       "KDFW", 32.8998, -97.0403),
    "KXHIGHBOS": ("Boston",       "KBOS", 42.3656, -71.0096),
    "KXHIGHPHX": ("Phoenix",      "KPHX", 33.4373, -112.0078),
    "KXHIGHSEA": ("Seattle",      "KSEA", 47.4502, -122.3088),
    "KXHIGHHOU": ("Houston",      "KIAH", 29.9902, -95.3368),
    "KXHIGHMSP": ("Minneapolis",  "KMSP", 44.8848, -93.2223),
    "KXHIGHDTW": ("Detroit",      "KDTW", 42.2162, -83.3554),
    "KXHIGHPHL": ("Philadelphia", "KPHL", 39.8729, -75.2437),
    "KXHIGHDCA": ("DC",           "KDCA", 38.8521, -77.0377),
    "KXHIGHCLT": ("Charlotte",    "KCLT", 35.2140, -80.9431),
    "KXHIGHLAS": ("Las Vegas",    "KLAS", 36.0840, -115.1537),
    "KXHIGHPDX": ("Portland",     "KPDX", 45.5898, -122.5951),
    "KXHIGHNSH": ("Nashville",    "KBNA", 36.1245, -86.6782),
    "KXHIGHKCI": ("Kansas City",  "KMCI", 39.2976, -94.7139),
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
    today_str = datetime.datetime.now().strftime("%b %#d").upper()  # e.g. "APR 15"
    today_alt = datetime.datetime.now().strftime("%-d").lstrip("0")  # day without leading zero
    found = {}
    for series, info in KNOWN_SERIES.items():
        d = kget("/markets", params={"series_ticker": series, "status": "open", "limit": 100})
        if not d:
            continue
        mkts = d.get("markets", [])
        if not mkts:
            continue
        # Filter to today only if TODAY_ONLY is set
        # Only include today's and tomorrow's markets — skip yesterday/settled
        now        = datetime.datetime.utcnow()
        today_utc  = now.date()
        tomorrow   = today_utc + datetime.timedelta(days=1)
        valid_mkts = []
        for m in mkts:
            close = m.get("close_time", "")
            try:
                close_dt = datetime.datetime.fromisoformat(close.replace("Z", "+00:00"))
                close_d  = close_dt.date()
                if close_d >= today_utc:
                    valid_mkts.append(m)
            except Exception:
                valid_mkts.append(m)  # include if can't parse
        if valid_mkts:
            found[series] = valid_mkts
            log.info(f"  {series} ({info[0]}): {len(valid_mkts)} valid markets")
    log.info(f"Total: {len(found)} series, {sum(len(v) for v in found.values())} markets")
    return found

def get_real_price(market):
    """
    Get real bid/ask from orderbook, not stale mid price.
    Falls back to market yes_bid/yes_ask if orderbook fails.
    """
    ticker = market.get("ticker", "")
    try:
        d = kget(f"/markets/{ticker}/orderbook")
        if d and d.get("orderbook"):
            ob = d["orderbook"]
            yes_bids = ob.get("yes", [])
            no_bids  = ob.get("no", [])
            if yes_bids:
                best_yes_bid = yes_bids[0][0]  # price of best yes bid
                return best_yes_bid / 100.0
    except Exception:
        pass
    # Fallback to market data
    ask = market.get("yes_ask", 50)
    bid = market.get("yes_bid", 50)
    return ((ask + bid) / 2.0) / 100.0

def mid_prob(market):
    ask = market.get("yes_ask", 50)
    bid = market.get("yes_bid", 50)
    if ask == 50 and bid == 50:
        return None  # stale — skip
    return ((ask + bid) / 2.0) / 100.0

def parse_range(title):
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
    """
    Fetch weather using NOAA stations API.
    Also attempts to get today's forecast high from NWS gridpoint.
    """
    global _wx
    now  = datetime.datetime.now(datetime.timezone.utc)
    seen = set()
    for series, info in KNOWN_SERIES.items():
        city, station, lat, lon = info
        if city in seen:
            continue
        seen.add(city)
        try:
            # Current observation
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

            # Try to get NWS forecast high for today
            est_high = None
            try:
                pt = requests.get(
                    f"https://api.weather.gov/points/{lat},{lon}",
                    headers={"User-Agent": "kalshi-agent/1.0 jzrucker@gmail.com"},
                    timeout=8
                )
                if pt.status_code == 200:
                    fc_url = pt.json()["properties"]["forecastHourly"]
                    fc = requests.get(
                        fc_url,
                        headers={"User-Agent": "kalshi-agent/1.0 jzrucker@gmail.com"},
                        timeout=8
                    )
                    if fc.status_code == 200:
                        periods = fc.json()["properties"]["periods"]
                        today   = datetime.datetime.now().date()
                        today_temps = []
                        for p in periods:
                            start = datetime.datetime.fromisoformat(p["startTime"]).date()
                            if start == today:
                                today_temps.append(p["temperature"])
                        if today_temps:
                            est_high = max(today_temps)
                            log.info(f"  {city}: {cur_f}F obs, {est_high}F NWS forecast high, {age_min}min old")
            except Exception:
                pass

            if est_high is None:
                # Fallback: obs + time-of-day cushion
                hr_et    = (now.hour - 4) % 24
                cushion  = max(0, 14 - max(hr_et, 10))
                est_high = round(cur_f + cushion, 1)
                log.info(f"  {city}: {cur_f}F obs, {est_high}F est_high (cushion), {age_min}min old")

            _wx[city] = (est_high, cur_f, age_min)

        except Exception as e:
            log.warning(f"  Weather error {city}: {e}")

def get_wx(city):
    return _wx.get(city, (None, None, None))

def true_prob(est_high, cur_obs, temp_range, hr_et):
    if est_high is None:
        return None
    low, high = temp_range
    ref = max(cur_obs, est_high) if hr_et >= 14 and cur_obs is not None else est_high
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
    log.info(f"KALSHI WEATHER AGENT v2 | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    balance   = get_balance()
    positions = get_positions()
    log.info(f"Balance: ${balance:.2f} | Positions: {len(positions)} | Min edge: {MIN_EDGE*100:.0f}%")

    log.info("Fetching weather (with NWS forecast)...")
    fetch_weather()

    log.info("Discovering today's markets...")
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

        log.info(f"\n-- {city} | est_high={est_high} cur={cur_obs} age={age_min}min --")

        if est_high is None:
            log.warning("  No weather data — skip")
            continue
        if age_min is not None and age_min > MAX_DATA_AGE:
            log.warning(f"  Data too old ({age_min}min) — skip")
            continue

        city_trades = 0

        # Sort markets by edge potential — highest abs(diff from 50) first
        for mkt in markets:
            ticker = mkt.get("ticker", "")
            title  = mkt.get("title", "")

            if ticker in positions:
                continue
            if len(positions) + len(trades) >= MAX_POSITIONS:
                log.info("  Max total positions hit")
                break
            if city_trades >= MAX_PER_CITY:
                log.info(f"  Max {MAX_PER_CITY} trades for {city} this run")
                break

            rng = parse_range(title)
            if not rng:
                continue

            # Get real market price — skip if stale 50/50
            mkt_p = mid_prob(mkt)
            if mkt_p is None:
                log.info(f"  SKIP {title[:40]} — stale 50/50 price")
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

            log.info(f"  {title[:50]} | mkt={mkt_p:.2f} true={true_p:.2f} edge={edge:+.2f} [{side}]")

            if abs(edge) < MIN_EDGE:
                skipped += 1
                continue

            q         = 1 - p
            f         = max(0, (p - q) * 0.5)
            size      = min(f * balance, MAX_POSITION)
            size      = max(1.0, size)
            contracts = max(1, int(size))
            price_c   = max(1, min(99, int(mkt_p * 100) + (1 if side == "yes" else 0)))

            result = place_order(ticker, side, contracts, price_c)
            if result:
                cost      = contracts * price_c / 100.0
                deployed += cost
                positions[ticker] = result
                city_trades += 1
                trades.append(f"  {side.upper()} {contracts}x {city} | {title[:40]} | edge={edge:+.2f} ${cost:.2f}")

    final_bal = get_balance()
    subject   = f"{'No trades' if not trades else str(len(trades))+' trades'} | Kalshi | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body      = (
        f"Kalshi Weather Agent v2\n{'='*40}\n"
        f"Time:     {datetime.datetime.now().strftime('%Y-%m-%d %H:%M ET')}\n"
        f"Balance:  ${final_bal:.2f}\n"
        f"Deployed: ${deployed:.2f}\n\n"
        f"TRADES ({len(trades)}):\n" + ("\n".join(trades) if trades else "  None") +
        f"\n\nSkipped (stale/no edge): {skipped}\n"
        f"Series: {len(all_markets)} | Min edge: {MIN_EDGE*100:.0f}% | Max/trade: ${MAX_POSITION:.0f} | Max/city: {MAX_PER_CITY}\n"
        f"Today only: {TODAY_ONLY}\n"
    )
    log.info(f"\n{body}")
    send_email(subject, body)

if __name__ == "__main__":
    run()
