"""
Kalshi Weather Trading Agent v1
================================
Monitors Kalshi weather temperature markets.
Compares NOAA observations against market-implied probabilities.
Trades when edge >= 3% is detected.

GitHub Secrets required:
  KALSHI_API_KEY_ID     - Your Kalshi API Key ID
  KALSHI_PRIVATE_KEY    - Full contents of kalshi private key PEM
  EMAIL_FROM            - jzrucker@gmail.com
  EMAIL_PASSWORD        - Gmail App Password
"""

import os
import time
import logging
import smtplib
import requests
import datetime
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("kalshi_log.txt"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KALSHI_BASE     = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_KEY_ID   = os.environ.get("KALSHI_API_KEY_ID", "")
KALSHI_PRIV_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")
EMAIL_FROM      = os.environ.get("EMAIL_FROM", "")
EMAIL_PASSWORD  = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_TO        = "jzrucker@gmail.com"

MAX_POSITION    = 25.00   # max $ per trade
MIN_EDGE        = 0.03    # 3% minimum edge
MAX_POSITIONS   = 20      # max simultaneous positions
MAX_DATA_AGE    = 180     # minutes

# Series ticker → (city name, NOAA station, lat, lon)
KNOWN_SERIES = {
    "KXHIGHNY":  ("NYC",     "KNYC", 40.7829, -73.9654),
    "KXHIGHCHI": ("Chicago", "KMDW", 41.7868, -87.7522),
    "KXHIGHMIA": ("Miami",   "KMIA", 25.7959, -80.2870),
    "KXHIGHDEN": ("Denver",  "KDEN", 39.8561, -104.6737),
    "KXHIGHAUS": ("Austin",  "KAUS", 30.1945, -97.6699),
    "KXHIGHLA":  ("LA",      "KLAX", 33.9425, -118.4081),
    "KXHIGHATL": ("Atlanta", "KATL", 33.6407, -84.4277),
    "KXHIGHDAL": ("Dallas",  "KDFW", 32.8998, -97.0403),
    "KXHIGHBOS": ("Boston",  "KBOS", 42.3656, -71.0096),
    "KXHIGHPHX": ("Phoenix", "KPHX", 33.4373, -112.0078),
    "KXHIGHSEA": ("Seattle", "KSEA", 47.4502, -122.3088),
    "KXHIGHHOU": ("Houston", "KIAH", 29.9902, -95.3368),
    "KXHIGHMSP": ("Minneapolis", "KMSP", 44.8848, -93.2223),
    "KXHIGHDTW": ("Detroit", "KDTW", 42.2162, -83.3554),
    "KXHIGHPHL": ("Philadelphia", "KPHL", 39.8729, -75.2437),
    "KXHIGHDCA": ("DC",      "KDCA", 38.8521, -77.0377),
    "KXHIGHCLT": ("Charlotte","KCLT", 35.2140, -80.9431),
    "KXHIGHLAS": ("Las Vegas","KLAS", 36.0840, -115.1537),
    "KXHIGHPDX": ("Portland","KPDX", 45.5898, -122.5951),
    "KXHIGHNSH": ("Nashville","KBNA", 36.1245, -86.6782),
    "KXHIGHKCI": ("Kansas City","KMCI", 39.2976, -94.7139),
}

# ── Kalshi Auth ───────────────────────────────────────────────────────────────

def make_headers(method, path):
    try:
        key = serialization.load_pem_private_key(
            KALSHI_PRIV_KEY.encode(), password=None, backend=default_backend()
        )
        ts       = str(int(time.time() * 1000))
        fullpath = urlparse(KALSHI_BASE + path).path
        msg      = f"{ts}{method.upper()}{fullpath}".encode("utf-8")
        sig      = key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256()
        )
        return {
            "Content-Type":              "application/json",
            "KALSHI-ACCESS-KEY":         KALSHI_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP":   ts,
            "KALSHI-ACCESS-SIGNATURE":   base64.b64encode(sig).decode(),
        }
    except Exception as e:
        log.error(f"Auth error: {e}")
        return {}

def kget(path, params=None):
    try:
        r = requests.get(KALSHI_BASE + path, headers=make_headers("GET", path),
                         params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        log.warning(f"GET {path} → {r.status_code}")
    except Exception as e:
        log.error(f"GET error: {e}")
    return None

def kpost(path, body):
    try:
        r = requests.post(KALSHI_BASE + path, headers=make_headers("POST", path),
                          json=body, timeout=10)
        if r.status_code in (200, 201):
            return r.json()
        log.warning(f"POST {path} → {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.error(f"POST error: {e}")
    return None

# ── Portfolio ─────────────────────────────────────────────────────────────────

def get_balance():
    d = kget("/portfolio/balance")
    return d.get("balance", 0) / 100.0 if d else 0.0

def get_positions():
    d = kget("/portfolio/positions")
    return {p["ticker"]: p for p in d.get("market_positions", [])} if d else {}

# ── Markets ───────────────────────────────────────────────────────────────────

def discover_markets():
    """Scan all known series for open markets."""
    found = {}
    for series, info in KNOWN_SERIES.items():
        d = kget("/markets", params={"series_ticker": series, "status": "open", "limit": 100})
        if d:
            mkts = d.get("markets", [])
            if mkts:
                found[series] = mkts
                log.info(f"  {series} ({info[0]}): {len(mkts)} markets")
    log.info(f"Total: {len(found)} active series, {sum(len(v) for v in found.values())} markets")
    return found

def mid_prob(market):
    ask = market.get("yes_ask", 50)
    bid = market.get("yes_bid", 50)
    return ((ask + bid) / 2.0) / 100.0

def parse_range(title):
    """Parse temp range from Kalshi market title."""
    import re
    t = title.lower().replace('°', '').replace(',', '').replace('**', '')
    try:
        m = re.search(r'>\s*(\d+)', t)
        if m: return (int(m.group(1)), 9999)
        m = re.search(r'<\s*(\d+)', t)
        if m: return (-9999, int(m.group(1)))
        m = re.search(r'(\d+)-(\d+)', t)
        if m: return (int(m.group(1)), int(m.group(2)))
        m = re.search(r'(\d+)\s+to\s+(\d+)', t)
        if m: return (int(m.group(1)), int(m.group(2)))
        if 'or above' in t or 'or higher' in t:
            nums = re.findall(r'\d+', t)
            return (int(nums[0]), 9999) if nums else None
        if 'or below' in t or 'or lower' in t:
            nums = re.findall(r'\d+', t)
            return (-9999, int(nums[0])) if nums else None
    except Exception:
        pass
    return None

# ── Edge ──────────────────────────────────────────────────────────────────────

def true_prob(est_high, cur_obs, temp_range, hr_et):
    if est_high is None:
        return None
    low, high = temp_range

    # Late day — current obs is very close to true high
    if hr_et >= 14 and cur_obs is not None:
        ref = max(cur_obs, est_high)
    else:
        ref = est_high

    if high == 9999:
        diff = ref - low
    elif low == -9999:
        diff = high - ref
    else:
        center = (low + high) / 2.0
        diff   = ref - center

    if diff >= 5:   return 0.90
    if diff >= 3:   return 0.78
    if diff >= 1:   return 0.63
    if diff >= -1:  return 0.50
    if diff >= -3:  return 0.35
    if diff >= -5:  return 0.20
    return 0.08

# ── Orders ────────────────────────────────────────────────────────────────────

def place_order(ticker, side, contracts, price_cents):
    body = {
        "ticker":        ticker,
        "action":        "buy",
        "side":          side,
        "count":         int(contracts),
        "type":          "limit",
        "yes_price":     price_cents if side == "yes" else 100 - price_cents,
        "no_price":      100 - price_cents if side == "yes" else price_cents,
        "expiration_ts": int((datetime.datetime.utcnow() + datetime.timedelta(minutes=5)).timestamp()),
    }
    result = kpost("/portfolio/orders", body)
    if result:
        log.info(f"  ✓ {side.upper()} {contracts}x {ticker} @ {price_cents}¢")
        return result
    return None

# ── Email ─────────────────────────────────────────────────────────────────────

def send_email(subject, body):
    if not EMAIL_FROM or not EMAIL_PASSWORD:
        return
    try:
        msg            = MIMEMultipart()
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_FROM, EMAIL_PASSWORD)
            s.send_message(msg)
        log.info(f"Email sent: {subject}")
    except Exception as e:
        log.error(f"Email failed: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info(f"KALSHI WEATHER AGENT v1 | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    balance   = get_balance()
    positions = get_positions()
    log.info(f"Balance: ${balance:.2f} | Positions: {len(positions)} | Min edge: {MIN_EDGE*100:.0f}%")

    log.info("Fetching weather...")
    fetch_weather()

    log.info("Discovering markets...")
    all_markets = discover_markets()

    hr_et        = (datetime.datetime.utcnow().hour - 4) % 24
    trades       = []
    skipped      = 0
    deployed     = 0.0

    for series, markets in all_markets.items():
        info = KNOWN_SERIES.get(series)
        if not info:
            continue
        city, station, lat, lon = info
        est_high, cur_obs, age_min = get_wx(city)

        log.info(f"\n── {city} | est_high={est_high} cur={cur_obs} ──")

        if est_high is None:
            log.warning("  No weather data — skip")
            continue

        if age_min is not None and age_min > MAX_DATA_AGE:
            log.warning(f"  Data too old ({age_min}min) — skip")
            continue

        log.info(f"  Scoring {len(markets)} markets for {city}...")
        for mkt in markets:
            ticker = mkt.get("ticker", "")
            title  = mkt.get("title", "")

            if ticker in positions:
                log.info(f"  SKIP {ticker} — already in positions")
                continue
            if len(positions) + len(trades) >= MAX_POSITIONS:
                log.info("  Max positions hit")
                break

            rng = parse_range(title)
            if not rng:
                    continue

            mkt_p  = mid_prob(mkt)
            true_p = true_prob(est_high, cur_obs, rng, hr_et)
            if true_p is None:
                continue

            edge_y = true_p - mkt_p
            edge_n = (1 - true_p) - (1 - mkt_p)

            if abs(edge_y) >= abs(edge_n):
                side, edge, p = "yes", edge_y, true_p
            else:
                side, edge, p = "no", edge_n, 1 - true_p

            log.info(f"  {title[:45]:<45} mkt={mkt_p:.2f} true={true_p:.2f} edge={edge:+.2f} [{side}]")

            if abs(edge) < MIN_EDGE:
                skipped += 1
                continue

            # Half-Kelly sizing
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
                trades.append(f"  {side.upper()} {contracts}x {city} '{title}' edge={edge:+.2f} ${cost:.2f}")

    final_bal = get_balance()
    subject   = f"{'No trades' if not trades else f'{len(trades)} trades'} | Kalshi | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body      = (
        f"Kalshi Weather Agent\n{'='*40}\n"
        f"Time:     {datetime.datetime.now().strftime('%Y-%m-%d %H:%M ET')}\n"
        f"Balance:  ${final_bal:.2f}\n"
        f"Deployed: ${deployed:.2f}\n\n"
        f"TRADES ({len(trades)}):\n" + ("\n".join(trades) if trades else "  None") +
        f"\n\nSkipped (edge < {MIN_EDGE*100:.0f}%): {skipped}\n"
        f"Series scanned: {len(all_markets)}\n"
        f"Min edge: {MIN_EDGE*100:.0f}% | Max per trade: ${MAX_POSITION:.0f}\n"
    )
    log.info(f"\n{body}")
    send_email(subject, body)

if __name__ == "__main__":
    run()
