import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
OI_WATCH_THRESHOLD    = 300    # %
OI_EXEC_THRESHOLD     = 500    # %
MIN_BASE_OI           = 1000
STRIKE_RANGE_POINTS   = 100
CHECK_MARKET_HOURS    = False
BASELINE_FILE         = "baseline_oi.json"

DEBUG_MODE = str(os.environ.get("DEBUG_MODE", "false")).lower() == "true"

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

# ================= SECRETS =================
CLIENT_ID        = os.environ.get("CLIENT_ID")
ACCESS_TOKEN     = os.environ.get("ACCESS_TOKEN")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise RuntimeError("❌ Missing FYERS credentials")

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
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

# ================= BASELINE =================
def load_baseline():
    """
    Load the baseline from file.
    If not exists, return default structure.
    """
    if os.path.exists(BASELINE_FILE):
        try:
            with open(BASELINE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠ Baseline file corrupted → resetting")
            return {"date": None, "data": {}, "first_alert_sent": False}
    return {"date": None, "data": {}, "first_alert_sent": False}



def save_baseline(b):
    with open(BASELINE_FILE, "w") as f:
        json.dump(b, f, indent=2)

def reset_on_new_day(b):
    """
    Reset baseline on a new trading day.
    """
    today = now_ist().date().isoformat()
    if b.get("date") != today:
        print("🔄 New trading day → baseline reset")
        b["date"] = today
        b["data"] = {}                # clear previous day's strikes
        b["first_alert_sent"] = False
        save_baseline(b)              # save immediately after reset
    return b


# ================= API =================
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

def get_current_weekly_expiry(expiry_info):
    today = now_ist().date()
    expiries = []
    for e in expiry_info:
        try:
            exp = datetime.fromtimestamp(int(e["expiry"])).date()
            expiries.append(((exp - today).days, e["date"]))
        except Exception:
            continue
    expiries = [x for x in expiries if x[0] >= 0]
    return sorted(expiries, key=lambda x: x[0])[0][1] if expiries else None

# ================= STRIKE SELECTION =================
def select_trade_strike(atm, buildup_type):
    if buildup_type == "CE":   # short buildup → buy PE
        return atm - 50, "PE"
    else:                      # long buildup → buy CE
        return atm + 50, "CE"

# ================= SCAN =================
def scan():
    print("▶ Scan started")

    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_on_new_day(load_baseline())

    spot = get_nifty_spot()
    atm = int(round(spot / 50) * 50)

    raw, expiry_info = fetch_option_chain()
    expiry_date = get_current_weekly_expiry(expiry_info)
    if not expiry_date:
        return

    expiry = expiry_to_symbol_format(expiry_date)

    df = pd.DataFrame(raw)
    df = df[df["symbol"].str.contains(expiry, regex=False)]
    df = df[(df["strike_price"] >= atm - STRIKE_RANGE_POINTS) &
            (df["strike_price"] <= atm + STRIKE_RANGE_POINTS)]

    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt    = r.option_type
        oi     = int(r.oi)
        ltp    = float(r.ltp)
        vol    = int(r.volume)

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)

        if entry is None:
            baseline["data"][key] = {
                "baseline_oi": oi,
                "baseline_ltp": ltp,
                "baseline_vol": vol,
                "state": "NONE"
            }
            continue

        base_oi  = entry["baseline_oi"]
        base_ltp = entry["baseline_ltp"]
        base_vol = entry["baseline_vol"]
        state    = entry["state"]

        if base_oi < MIN_BASE_OI:
            continue

        oi_pct = ((oi - base_oi) / base_oi) * 100
        ltp_ok = ltp > base_ltp * 1.05
        vol_ok = vol > base_vol * 1.3

        # ================= WATCH =================
        if oi_pct >= OI_WATCH_THRESHOLD and state == "NONE":
            send_telegram_alert(
                f"👀 *OI WATCH*\n"
                f"{strike} {opt}\n"
                f"OI +{oi_pct:.0f}%\n"
                f"Spot: {spot}"
            )
            entry["state"] = "WATCH"

        # ================= EXECUTION =================
        if oi_pct >= OI_EXEC_THRESHOLD and state == "WATCH":
            if ltp_ok and vol_ok:
                trade_strike, trade_opt = select_trade_strike(atm, opt)
                send_telegram_alert(
                    f"🚀 *EXECUTION SIGNAL*\n"
                    f"{opt} buildup confirmed\n"
                    f"Buy {trade_strike} {trade_opt}\n\n"
                    f"OI +{oi_pct:.0f}%\n"
                    f"LTP ↑ | Volume ↑\n"
                    f"Spot: {spot}"
                )
                entry["state"] = "EXECUTED"

    if not baseline["first_alert_sent"]:
        send_telegram_alert(
            f"*NIFTY OI MONITOR STARTED*\n"
            f"Spot: {spot}\nATM: {atm}"
        )
        baseline["first_alert_sent"] = True

    save_baseline(baseline)

# ================= ENTRY =================
if __name__ == "__main__":
    scan()
