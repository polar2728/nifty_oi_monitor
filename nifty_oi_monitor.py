import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
OI_SPIKE_THRESHOLD  = float(os.environ.get("OI_SPIKE_THRESHOLD", 300))
MIN_BASE_OI         = 1000
STRIKE_RANGE_POINTS = 100
CHECK_MARKET_HOURS  = True
BASELINE_FILE       = "baseline_oi.json"
DEBUG_MODE          = os.environ.get("DEBUG_MODE", "False").lower() == "true"

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

# ================= SECRETS =================
CLIENT_ID        = os.environ["CLIENT_ID"]
ACCESS_TOKEN     = os.environ["ACCESS_TOKEN"]
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    log_path=""
)

print("DEBUG_MODE raw:", os.environ.get("DEBUG_MODE"))
print("DEBUG_MODE parsed:", DEBUG_MODE)
print("CLIENT_ID raw:", os.environ.get("CLIENT_ID"))

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
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    r = requests.post(url, data=payload, timeout=10)
    if DEBUG_MODE:
        print("Telegram:", r.status_code, r.text)

# ================= BASELINE =================
def load_baseline():
    if os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "r") as f:
            return json.load(f)
    return {"date": None, "data": {}, "first_alert_sent": False}

def save_baseline(b):
    with open(BASELINE_FILE, "w") as f:
        json.dump(b, f, indent=2)

def reset_on_new_day(b):
    today = now_ist().date().isoformat()
    if b.get("date") != today:
        print("🔄 New trading day → baseline reset")
        b["date"] = today
        b["data"] = {}
        b["first_alert_sent"] = False
        save_baseline(b)
    return b

# ================= API CALLS =================
def get_nifty_spot():
    q = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    return round(q["d"][0]["v"]["lp"])

def fetch_option_chain():
    r = fyers.optionchain({
        "symbol": "NSE:NIFTY50-INDEX",
        "strikecount": 40,
        "timestamp": ""
    })
    return r["data"]["optionsChain"], r["data"]["expiryData"]

def expiry_to_symbol_format(date_str):
    d = datetime.strptime(date_str, "%d-%m-%Y")
    return d.strftime("%y") + str(d.month) + d.strftime("%d")

def get_current_weekly_expiry(expiry_list):
    today = now_ist().date()
    expiries = []
    for e in expiry_list:
        exp = datetime.fromtimestamp(int(e["expiry"])).date()
        expiries.append((exp - today).days, e["date"])
    return sorted(expiries)[0][1]

# ================= SCAN =================
def scan():
    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        if DEBUG_MODE:
            send_telegram_alert("⏱ Market closed")
        return

    baseline = reset_on_new_day(load_baseline())

    spot = get_nifty_spot()
    atm = int(round(spot / 50) * 50)

    raw, expiry_info = fetch_option_chain()
    expiry = expiry_to_symbol_format(get_current_weekly_expiry(expiry_info))

    df = pd.DataFrame(raw)
    df = df[df["symbol"].str.contains(expiry, regex=False)]
    df = df[
        (df["strike_price"] >= atm - STRIKE_RANGE_POINTS) &
        (df["strike_price"] <= atm + STRIKE_RANGE_POINTS)
    ]

    alerts_ce = []
    alerts_pe = []

    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt = r.option_type
        oi = int(r.oi)
        key = f"{opt}_{strike}"

        entry = baseline["data"].get(key)

        # ===== FIRST TIME → SET BASELINE =====
        if entry is None:
            baseline["data"][key] = {
                "baseline": oi,
                "last_alert": oi
            }
            if DEBUG_MODE:
                print(f"Baseline set {key} = {oi}")
                send_telegram_alert(f"Baseline set {key} = {oi}")
            continue

        base_oi = entry["baseline"]
        last_alert_oi = entry["last_alert"]

        if base_oi < MIN_BASE_OI:
            continue

        oi_pct = ((oi - base_oi) / base_oi) * 100

        # ===== ACCELERATION ALERT =====
        if oi_pct >= OI_SPIKE_THRESHOLD and oi > last_alert_oi:
            msg = f"{strike} {opt} +{oi_pct:.0f}% (Base {base_oi} → {oi})"
            if opt == "CE":
                alerts_ce.append(msg)
            else:
                alerts_pe.append(msg)

            baseline["data"][key]["last_alert"] = oi

    # ===== FIRST SCAN ALERT =====
    if not baseline["first_alert_sent"]:
        send_telegram_alert(
            f"*NIFTY OI Monitor Started*\n"
            f"Spot: {spot}\nATM: {atm}\nBaseline captured."
        )
        baseline["first_alert_sent"] = True

    # ===== GROUPED TELEGRAM ALERT =====
    if alerts_ce or alerts_pe:
        msg = f"*🚨 NIFTY OI SPIKE*\nSpot: {spot} | ATM: {atm}\n\n"
        if alerts_ce:
            msg += "*📈 CALL BUILDUP*\n" + "\n".join(alerts_ce) + "\n\n"
        if alerts_pe:
            msg += "*📉 PUT BUILDUP*\n" + "\n".join(alerts_pe)

        send_telegram_alert(msg)

    save_baseline(baseline)

    if  not alerts_ce and not alerts_pe:
        print("✅ No spikes detected")
        if DEBUG_MODE:
            send_telegram_alert("✅ No spikes detected")

# ================= ENTRY =================
if __name__ == "__main__":
    scan()
