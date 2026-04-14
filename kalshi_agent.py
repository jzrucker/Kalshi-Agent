"""
Kalshi Weather Trading Agent v1
================================
Monitors Kalshi weather temperature markets across major US cities.
Compares market-implied probabilities against live weather forecast data.
Trades when edge >= 5% is detected.

GitHub Secrets required:
  KALSHI_API_KEY_ID     - Your Kalshi API Key ID
  KALSHI_PRIVATE_KEY    - Full contents of kalshi_private.pem
  EMAIL_FROM            - jzrucker@gmail.com
  EMAIL_PASSWORD        - Gmail App Password
"""

import os
import time
import json
import logging
import smtplib
import requests
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from urllib.parse import urlparse
from cryptography.hazmat.backends import default_backend
import base64

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("kalshi_log.txt"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KALSHI_BASE      = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_KEY_ID    = os.environ.get("KALSHI_API_KEY_ID", "")
KALSHI_PRIV_KEY  = os.environ.get("KALSHI_PRIVATE_KEY", "")

EMAIL_FROM       = os.environ.get("EMAIL_FROM", "")
EMAIL_PASSWORD   = os.environ.get("EMAIL_PASSWORD", "")
EMAIL_TO         = "jzrucker@gmail.com"

# Position sizing
MAX_POSITION     = 25.00    # max $ per single trade
MAX_DEPLOYED_PCT = 1.0      # deploy up to 100% of portfolio
MIN_EDGE         = 0.05     # 5% minimum edge to trade
MAX_POSITIONS    = 20       # max simultaneous open positions
MAX_DATA_AGE_MIN = 90       # skip if forecast data older than 90 minutes

# Cities: name → (kalshi_series_ticker, lat, lon)
CITIES = {
    "NYC":          ("KXHIGHNY",   40.7829, -73.9654),
    "Chicago":      ("KXHIGHCHI",  41.9742, -87.9073),
    "Miami":        ("KXHIGHMIA",  25.7959, -80.2870),
    "LA":           ("KXHIGHLA",   33.9425, -118.4081),
    "Atlanta":      ("KXHIGHATL",  33.6407, -84.4277),
    "Dallas":       ("KXHIGHDAL",  32.8998, -97.0403),
    "Boston":       ("KXHIGHBOS",  42.3656, -71.0096),
    "Phoenix":      ("KXHIGHPHX",  33.4373, -112.0078),
    "Seattle":      ("KXHIGHSEA",  47.4502, -122.3088),
    "Denver":       ("KXHIGHDEN",  39.8561, -104.6737),
    "Houston":      ("KXHIGHHOU",  29.9902, -95.3368),
    "Minneapolis":  ("KXHIGHMSP",  44.8848, -93.2223),
    "Detroit":      ("KXHIGHDTW",  42.2162, -83.3554),
    "Philadelphia": ("KXHIGHPHL",  39.8729, -75.2437),
    "DC":           ("KXHIGHDCA",  38.8521, -77.0377),
    "Charlotte":    ("KXHIGHCLT",  35.2140, -80.9431),
    "Las Vegas":    ("KXHIGHLАС",  36.0840, -115.1537),
    "Portland":     ("KXHIGHPDX",  45.5898, -122.5951),
    "Nashville":    ("KXHIGHNSH",  36.1245, -86.6782),
    "Kansas City":  ("KXHIGHKCI",  39.2976, -94.7139),
}

# ── Kalshi Auth ───────────────────────────────────────────────────────────────

def get_kalshi_headers(method, path):
    try:
        private_key = serialization.load_pem_private_key(
            KALSHI_PRIV_KEY.encode(),
            password=None,
            backend=default_backend()
        )
        ts = str(int(time.time() * 1000))
        # Strip query params, use full path including /trade-api/v2
        sign_path = urlparse(KALSHI_BASE + path).path
        msg = f"{ts}{method.upper()}{sign_path}".encode("utf-8")
        sig = private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
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


def kalshi_get(path, params=None):
    headers = get_kalshi_headers("GET", path)
    try:
        r = requests.get(KALSHI_BASE + path, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        log.warning(f"GET {path} → {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"GET {path} error: {e}")
    return None


def kalshi_post(path, body):
    headers = get_kalshi_headers("POST", path)
    try:
        r = requests.post(KALSHI_BASE + path, headers=headers, json=body, timeout=10)
        if r.status_code in (200, 201):
            return r.json()
        log.warning(f"POST {path} → {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"POST {path} error: {e}")
    return None


# ── Portfolio ─────────────────────────────────────────────────────────────────

def get_balance():
    data = kalshi_get("/portfolio/balance")
    if data:
        return data.get("balance", 0) / 100.0
    return 0.0


def get_open_positions():
    data = kalshi_get("/portfolio/positions")
    if data:
        return {p["ticker"]: p for p in data.get("market_positions", [])}
    return {}


# ── Market Data ───────────────────────────────────────────────────────────────

def get_weather_markets(series_ticker):
    data = kalshi_get("/markets", params={
        "series_ticker": series_ticker,
        "status":        "open",
        "limit":         100,
    })
    if data:
        return data.get("markets", [])
    return []


def parse_market_prob(market):
    """Mid price as probability 0-1."""
    yes_ask = market.get("yes_ask", 50)
    yes_bid = market.get("yes_bid", 50)
    return ((yes_ask + yes_bid) / 2.0) / 100.0


def parse_temp_range(title):
    """
    '85° to 86°'     → (85, 86)
    '87° or above'   → (87, 9999)
    '80° or below'   → (-9999, 80)
    """
    t = title.lower().replace("°", "").replace(",", "")
    try:
        if "or above" in t or "or higher" in t:
            nums = [int(x) for x in t.split() if x.lstrip("-").isdigit()]
            return (nums[0], 9999) if nums else None
        if "or below" in t or "or lower" in t:
            nums = [int(x) for x in t.split() if x.lstrip("-").isdigit()]
            return (-9999, nums[0]) if nums else None
        if " to " in t:
            nums = [int(x) for x in t.split() if x.lstrip("-").isdigit()]
            return (nums[0], nums[1]) if len(nums) >= 2 else None
    except Exception:
        pass
    return None


# ── Weather Data ──────────────────────────────────────────────────────────────

# Weather cache — populated once per run for all cities
_weather_cache = {}

def fetch_all_weather():
    """
    Batch fetch weather for all cities in one API call using Open-Meteo
    multi-location endpoint. Much faster and avoids rate limiting.
    """
    global _weather_cache
    try:
        now_utc = datetime.datetime.utcnow()
        start   = (now_utc - datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:00")
        end     = now_utc.strftime("%Y-%m-%dT%H:00")

        lats = [str(v[1]) for v in CITIES.values()]
        lons = [str(v[2]) for v in CITIES.values()]

        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         ",".join(lats),
                "longitude":        ",".join(lons),
                "daily":            "temperature_2m_max",
                "hourly":           "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone":         "UTC",
                "forecast_days":    1,
                "start_hour":       start,
                "end_hour":         end,
            },
            timeout=30
        )

        if r.status_code != 200:
            log.warning(f"Open-Meteo batch error: {r.status_code}")
            return

        results = r.json()
        # Multi-location returns a list
        if not isinstance(results, list):
            results = [results]

        cities_list = list(CITIES.keys())
        for i, data in enumerate(results):
            city = cities_list[i]
            try:
                forecast_max = data["daily"]["temperature_2m_max"][0]
                hourly_temps = data["hourly"]["temperature_2m"]
                hourly_times = data["hourly"]["time"]

                current_obs = None
                age_minutes = 999
                for j in range(len(hourly_temps) - 1, -1, -1):
                    if hourly_temps[j] is not None:
                        obs_time    = datetime.datetime.fromisoformat(hourly_times[j])
                        age_minutes = int((now_utc - obs_time).total_seconds() / 60)
                        current_obs = hourly_temps[j]
                        break

                _weather_cache[city] = (forecast_max, current_obs, age_minutes)
                log.info(f"  Weather fetched: {city} max={forecast_max:.1f}F obs={current_obs}F age={age_minutes}min")
            except Exception as e:
                log.warning(f"  Weather parse error {city}: {e}")

    except Exception as e:
        log.warning(f"Batch weather fetch error: {e}")


def get_weather(city, lat, lon):
    """
    Returns (forecast_max_f, current_obs_f, data_age_minutes).
    Uses cached data from fetch_all_weather().
    Falls back to individual call if not in cache.
    """
    if city in _weather_cache:
        return _weather_cache[city]

    # Fallback individual call
    try:
        now_utc = datetime.datetime.utcnow()
        start   = (now_utc - datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:00")
        end     = now_utc.strftime("%Y-%m-%dT%H:00")

        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         lat,
                "longitude":        lon,
                "daily":            "temperature_2m_max",
                "hourly":           "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone":         "UTC",
                "forecast_days":    1,
                "start_hour":       start,
                "end_hour":         end,
            },
            timeout=30
        )
        if r.status_code != 200:
            return None, None, None

        data         = r.json()
        forecast_max = data["daily"]["temperature_2m_max"][0]
        hourly_temps = data["hourly"]["temperature_2m"]
        hourly_times = data["hourly"]["time"]

        current_obs = None
        age_minutes = 999
        for i in range(len(hourly_temps) - 1, -1, -1):
            if hourly_temps[i] is not None:
                obs_time    = datetime.datetime.fromisoformat(hourly_times[i])
                age_minutes = int((now_utc - obs_time).total_seconds() / 60)
                current_obs = hourly_temps[i]
                break

        return forecast_max, current_obs, age_minutes

    except Exception as e:
        log.warning(f"Weather fallback error ({city}): {e}")
        return None, None, None


# ── Edge Calculation ──────────────────────────────────────────────────────────

def estimate_true_prob(forecast_max, current_obs, temp_range, hour_utc):
    """
    Estimate true probability that today's high falls in temp_range.
    Late-day observed temps carry more weight than forecast.
    """
    if forecast_max is None:
        return None

    low, high = temp_range

    # Rough ET hour
    hour_et = (hour_utc - 4) % 24

    # Late day — observed temp anchors the estimate
    if hour_et >= 14 and current_obs is not None:
        if low == -9999:    # "X or below"
            if current_obs > high + 3:
                return 0.04
            if current_obs <= high:
                return 0.88
        if high == 9999:    # "X or above"
            if current_obs >= low - 1:
                return 0.88
            if current_obs < low - 5:
                return 0.04
        # Range band — use observed as proxy for eventual max
        if current_obs > high + 3:
            return 0.04
        if current_obs >= low and current_obs <= high + 1:
            return 0.72

    # Use forecast_max as the signal
    expected = forecast_max

    if high == 9999:
        diff = expected - low
    elif low == -9999:
        diff = high - expected
    else:
        center = (low + high) / 2.0
        diff   = expected - center

    # Map diff to probability
    if diff >= 5:
        return 0.88
    elif diff >= 3:
        return 0.75
    elif diff >= 1:
        return 0.62
    elif diff >= -1:
        return 0.50
    elif diff >= -3:
        return 0.35
    elif diff >= -5:
        return 0.22
    else:
        return 0.08


def kelly_size(edge, p_true, bankroll):
    """Half-Kelly, capped at MAX_POSITION."""
    if edge <= 0:
        return 0
    q   = 1 - p_true
    f   = (p_true - q) * 0.5   # half Kelly
    amt = f * bankroll
    return min(amt, MAX_POSITION)


# ── Trade Execution ───────────────────────────────────────────────────────────

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
    result = kalshi_post("/portfolio/orders", body)
    if result:
        log.info(f"  ✓ ORDER {side.upper()} {contracts}x {ticker} @ {price_cents}¢")
        return result
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info(f"KALSHI WEATHER AGENT v1 | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    balance   = get_balance()
    positions = get_open_positions()
    max_deploy = balance * MAX_DEPLOYED_PCT
    deployed   = sum(abs(p.get("total_cost", 0)) / 100.0 for p in positions.values())

    log.info(f"Balance:    ${balance:.2f}")
    log.info(f"Deployed:   ${deployed:.2f}")
    log.info(f"Positions:  {len(positions)}")
    log.info(f"Min edge:   {MIN_EDGE*100:.0f}%")

    hour_utc     = datetime.datetime.utcnow().hour

    log.info("Fetching weather data for all cities...")
    fetch_all_weather()

    trades_placed = []
    skipped_count = 0

    for city, (series_ticker, lat, lon) in CITIES.items():

        if len(positions) + len(trades_placed) >= MAX_POSITIONS:
            log.info("Max positions reached — stopping scan")
            break

        if deployed >= max_deploy:
            log.info("Max deployed — stopping scan")
            break

        log.info(f"\n── {city} ({series_ticker}) ──")

        forecast_max, current_obs, age_min = get_weather(city, lat, lon)

        if forecast_max is None:
            log.warning(f"  No weather data — skip")
            continue

        if age_min is not None and age_min > MAX_DATA_AGE_MIN:
            log.warning(f"  Data too old ({age_min}min) — skip")
            continue

        obs_str = f"{current_obs:.1f}°F ({age_min}min ago)" if current_obs else "n/a"
        log.info(f"  Forecast max: {forecast_max:.1f}°F | Current obs: {obs_str}")

        markets = get_weather_markets(series_ticker)
        if not markets:
            log.warning(f"  No open markets")
            continue

        log.info(f"  {len(markets)} open markets")

        for mkt in markets:
            ticker  = mkt.get("ticker", "")
            title   = mkt.get("title", "")
            volume  = mkt.get("volume", 0)

            if ticker in positions:
                continue

            temp_range = parse_temp_range(title)
            if not temp_range:
                continue

            mkt_prob  = parse_market_prob(mkt)
            true_prob = estimate_true_prob(forecast_max, current_obs, temp_range, hour_utc)

            if true_prob is None:
                continue

            edge_yes = true_prob - mkt_prob
            edge_no  = (1 - true_prob) - (1 - mkt_prob)

            if abs(edge_yes) >= abs(edge_no):
                side = "yes"
                edge = edge_yes
                p    = true_prob
            else:
                side = "no"
                edge = edge_no
                p    = 1 - true_prob

            log.info(
                f"  {title[:40]:<40} | "
                f"mkt={mkt_prob:.2f} true={true_prob:.2f} "
                f"edge={edge:+.2f} [{side.upper()}] vol={volume}"
            )

            if abs(edge) < MIN_EDGE:
                skipped_count += 1
                continue

            # Size position
            size      = kelly_size(abs(edge), p, balance)
            size      = max(1.0, min(size, MAX_POSITION, max_deploy - deployed))
            contracts = max(1, int(size))

            # Price to submit
            if side == "yes":
                price_cents = max(1, min(99, int(mkt_prob * 100) + 1))
            else:
                price_cents = max(1, min(99, int((1 - mkt_prob) * 100) + 1))

            result = place_order(ticker, side, contracts, price_cents)
            if result:
                cost     = contracts * price_cents / 100.0
                deployed += cost
                positions[ticker] = result
                trades_placed.append(
                    f"  {side.upper()} {contracts}x | {city} | '{title}' | "
                    f"edge={edge:+.2f} | ${cost:.2f}"
                )

    # ── Email summary ──────────────────────────────────────────────────────────
    final_bal = get_balance()
    no_trades = len(trades_placed) == 0

    subject = (
        ("No trades | " if no_trades else f"{len(trades_placed)} trades placed | ") +
        f"Kalshi Weather | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    body = (
        f"Kalshi Weather Agent v1\n"
        f"{'='*50}\n"
        f"Time:        {datetime.datetime.now().strftime('%Y-%m-%d %H:%M ET')}\n"
        f"Balance:     ${final_bal:.2f}\n"
        f"Deployed:    ${deployed:.2f}\n\n"
        f"TRADES PLACED ({len(trades_placed)}):\n"
        + ("\n".join(trades_placed) if trades_placed else "  None") +
        f"\n\nSKIPPED (edge < {MIN_EDGE*100:.0f}%): {skipped_count} markets\n"
        f"\nCITIES SCANNED:    {len(CITIES)}\n"
        f"MIN EDGE:          {MIN_EDGE*100:.0f}%\n"
        f"MAX PER TRADE:     ${MAX_POSITION:.0f}\n"
        f"MAX POSITIONS:     {MAX_POSITIONS}\n"
    )

    log.info(f"\n{body}")
    send_email(subject, body)


def send_email(subject, body):
    if not EMAIL_FROM or not EMAIL_PASSWORD:
        log.info("Email not configured — skipping")
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


if __name__ == "__main__":
    run()
