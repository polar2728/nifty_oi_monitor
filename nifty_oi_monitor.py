import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
OI_SPIKE_THRESHOLD  = 500
MIN_BASE_OI         = 1000
STRIKE_RANGE_POINTS = 100
CHECK_MARKET_HOURS  = False
BASELINE_FILE       = "baseline_oi.json"

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

# ================= SECRETS / ENV VARS =================
CLIENT_ID    = os.environ["CLIENT_ID"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    log_path=""
)

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def is_market_open():
    t = now_ist().time()
    return datetime.strptime("09:15", "%H:%M").time() <= t <= datetime.strptime("15:30", "%H:%M").time()

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram alert skipped: missing token/chat_id")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code == 200:
            print("✅ Telegram alert sent")
        else:
            print("❌ Telegram alert failed:", r.text)
    except Exception as e:
        print("❌ Telegram alert exception:", e)

# ================= BASELINE =================
def load_baseline():
    if os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "r") as f:
            return json.load(f)
    return {"prev_oi": {}, "last_run_date": None}

def save_baseline(baseline):
    with open(BASELINE_FILE, "w") as f:
        json.dump(baseline, f)

def reset_on_new_day(baseline):
    today_str = now_ist().date().isoformat()
    if baseline.get("last_run_date") != today_str:
        print("🔄 New trading day → resetting baseline")
        baseline["prev_oi"] = {}
        baseline["last_run_date"] = today_str
    return baseline

# ================= API CALLS =================
def get_nifty_spot():
    q = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    if q.get("s") == "ok" and q.get("d"):
        return round(q["d"][0]["v"]["lp"])
    return None

def fetch_option_chain():
    resp = fyers.optionchain({
        "symbol": "NSE:NIFTY50-INDEX",
        "strikecount": 40,
        "timestamp": ""
    })
    if resp.get("s") != "ok":
        return None, None
    data = resp["data"]
    return data.get("optionsChain", []), data.get("expiryData", [])

def expiry_to_symbol_format(date_str):
    try:
        d = datetime.strptime(date_str, "%d-%m-%Y")
        return d.strftime("%y") + str(d.month) + d.strftime("%d")
    except:
        return None

def get_current_weekly_expiry(expiry_list):
    today = now_ist().date()
    candidates = []
    for exp in expiry_list:
        try:
            exp_date = datetime.fromtimestamp(int(exp["expiry"])).date()
            candidates.append(((exp_date - today).days, exp["date"]))
        except:
            pass
    return sorted(candidates)[0][1] if candidates else None

# ================= SCAN =================
def scan():
    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market is closed (filter enabled)")
        return

    # Load baseline
    baseline = load_baseline()
    baseline = reset_on_new_day(baseline)
    prev_oi = baseline.get("prev_oi", {})

    spot = get_nifty_spot()
    if spot is None:
        print("❌ Failed to fetch NIFTY spot")
        return

    atm = int(round(spot / 50) * 50)
    print(f"📊 NIFTY Spot: {spot}, ATM Strike: {atm}")

    raw, expiry_info = fetch_option_chain()
    if not raw:
        print("❌ Option chain unavailable")
        return

    expiry = get_current_weekly_expiry(expiry_info)
    expiry_filter = expiry_to_symbol_format(expiry) or expiry

    df = pd.DataFrame(raw)
    df = df[df["symbol"].str.contains(expiry_filter, regex=False, na=False)]
    df = df[
        (df["strike_price"] >= atm - STRIKE_RANGE_POINTS) &
        (df["strike_price"] <= atm + STRIKE_RANGE_POINTS)
    ]

    alerts = []

    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt = r.option_type
        oi = int(r.oi)
        key = f"{opt}_{strike}"
        prev = prev_oi.get(key, 0)
        oi_pct = ((oi - prev) / prev * 100) if prev >= MIN_BASE_OI else 0

        if abs(oi_pct) > OI_SPIKE_THRESHOLD:
            alerts.append(f"{opt} {strike}: OI spike {oi_pct:+.1f}% (Prev {prev} → Current {oi})")

        prev_oi[key] = oi

    # Save baseline
    baseline["prev_oi"] = prev_oi
    save_baseline(baseline)

    if alerts:
        alert_msg = f"*NIFTY OI Spike Alert ({now_ist().strftime('%H:%M')})*\n" + "\n".join(alerts)
        send_telegram_alert(alert_msg)
    else:
        print("✅ No significant OI spikes detected.")

if __name__ == "__main__":
    scan()
