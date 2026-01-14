# =============================================================================
# NIFTY OI SPIKE MONITOR — FINAL STABLE VERSION
# =============================================================================

import os
import json
import time
from datetime import datetime, timezone, timedelta
import requests
from fyers_apiv3 import fyersModel

# ========================= CONFIG ========================= #

CHECK_MARKET_HOURS = False   # ✅ Turn ON/OFF as needed
OI_SPIKE_THRESHOLD = int(os.getenv("OI_SPIKE_THRESHOLD", 250000))
BASELINE_FILE = "baseline_oi.json"

IST = timezone(timedelta(hours=5, minutes=30))

# ========================= FYERS SETUP ========================= #

CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    log_path=None
)

# ========================= TELEGRAM ========================= #

TG_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg}, timeout=5)
    except Exception:
        pass

# ========================= UTILITIES ========================= #

def now_ist():
    return datetime.now(IST)

def today_str():
    return now_ist().strftime("%Y-%m-%d")

def is_market_open():
    if not CHECK_MARKET_HOURS:
        return True

    t = now_ist().time()
    return t >= datetime.strptime("09:15", "%H:%M").time() and \
           t <= datetime.strptime("15:30", "%H:%M").time()

# ========================= BASELINE ========================= #

def load_baseline():
    if not os.path.exists(BASELINE_FILE):
        return {
            "trading_day": today_str(),
            "startup_alert_sent": False,
            "data": {}
        }
    with open(BASELINE_FILE, "r") as f:
        return json.load(f)

def save_baseline(data):
    with open(BASELINE_FILE, "w") as f:
        json.dump(data, f, indent=2)

def reset_if_new_day(baseline):
    today = today_str()
    if baseline.get("trading_day") != today:
        print("🔄 New trading day → resetting baseline")
        baseline.clear()
        baseline.update({
            "trading_day": today,
            "startup_alert_sent": False,
            "data": {}
        })
    return baseline

# ========================= DATA FETCH ========================= #

def get_nifty_spot():
    resp = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
    if resp.get("s") != "ok":
        return None
    return float(resp["d"][0]["v"]["lp"])

def get_option_chain():
    return fyers.optionchain({
        "symbol": "NSE:NIFTY50-INDEX",
        "strikecount": 20,
        "timestamp": ""
    })

# ========================= CORE LOGIC ========================= #

def scan_for_oi_spikes():
    if not is_market_open():
        print("⏸ Market closed — skipping scan")
        return

    baseline = reset_if_new_day(load_baseline())

    # 🔔 Startup alert ONCE per day
    if not baseline["startup_alert_sent"]:
        send_telegram("🚀 NIFTY OI Monitor Started")
        baseline["startup_alert_sent"] = True

    spot = get_nifty_spot()
    if not spot:
        print("❌ NIFTY spot unavailable")
        save_baseline(baseline)
        return

    atm = int(round(spot / 50) * 50)
    print(f"[{now_ist()}] NIFTY: {spot:.2f} | ATM: {atm}")

    chain = get_option_chain()
    if chain.get("s") != "ok":
        print("❌ Option chain fetch failed")
        save_baseline(baseline)
        return

    alerts = []

    for row in chain["data"]["optionsChain"]:
        strike = row["strikePrice"]

        for opt in ["CE", "PE"]:
            if opt not in row:
                continue

            key = f"{opt}_{strike}"
            oi = row[opt]["oi"]

            if key not in baseline["data"]:
                baseline["data"][key] = {
                    "baseline": oi,
                    "last_alert": oi
                }
                continue

            base = baseline["data"][key]["baseline"]
            last_alert = baseline["data"][key]["last_alert"]

            delta = oi - last_alert

            if abs(delta) >= OI_SPIKE_THRESHOLD:
                alerts.append(
                    f"{opt} {strike} | ΔOI: {delta:,} | OI: {oi:,}"
                )
                baseline["data"][key]["last_alert"] = oi

    if alerts:
        msg = "📊 NIFTY OI SPIKE ALERT\n\n" + "\n".join(alerts)
        send_telegram(msg)

    save_baseline(baseline)

# ========================= ENTRY ========================= #

if __name__ == "__main__":
    print("📡 Scan started")
    scan_for_oi_spikes()
