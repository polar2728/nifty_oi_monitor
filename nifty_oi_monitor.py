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
CHECK_MARKET_HOURS    = True
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
    try:
        d = datetime.strptime(date_str, "%d-%m-%Y")
        yy = d.strftime("%y")           # '26'
        m_num = str(d.month)            # '1'
        dd = d.strftime("%d")           # '27'
        m_short = d.strftime("%b").upper()  # 'JAN'

        # Weekly format: YYMDD (no zero on month)
        weekly = yy + m_num + dd        # '26127'

        # Monthly format: YYMMM (3-letter month)
        monthly = yy + m_short          # '26JAN'

        return weekly, monthly          # return both
    except Exception as e:
        print(f"Date conversion error: {e}")
        return None, None

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
    
    # Optional debug (safe version)
    print("All future expiries:")
    for days, date_str in sorted(expiries):
        print(f"  - {date_str} ({days} days away)")
    
    if not expiries:
        print("No future expiry found")
        return None
    
    return sorted(expiries, key=lambda x: x[0])[0][1]

# ================= STRIKE SELECTION =================
def select_trade_strike(strike, buildup_type):
    # Same-strike contrarian: buy opposite option at the same strike
    if buildup_type == "CE":   # short buildup on CE → buy PE at same strike
        return strike, "PE"
    else:                      # short buildup on PE → buy CE at same strike
        return strike, "CE"

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

    weekly, monthly = expiry_to_symbol_format(expiry_date)
    if weekly is None:
        return

    print(f"Trying weekly filter: '{weekly}'")
    print(f"Trying monthly filter: '{monthly}'")

    df = pd.DataFrame(raw)

    # Try weekly first
    df_filtered = df[df["symbol"].str.contains(weekly, regex=False, na=False)]
    print(f"After weekly expiry filter: {len(df_filtered)}")
    if len(df_filtered) > 0:
        print("First 3 matching symbols after filter:", df_filtered["symbol"].head(3).tolist())
    else:
        print("No match in either format — showing first 5 raw symbols:")
        print([row['symbol'] for row in raw[:5]])

    if len(df_filtered) == 0:
        # Fallback to monthly format
        print("Weekly filter failed — trying monthly format")
        df_filtered = df[df["symbol"].str.contains(monthly, regex=False, na=False)]
        print(f"After monthly expiry filter: {len(df_filtered)}")

    df = df_filtered

    # Proceed with strike range
    df = df[(df["strike_price"] >= atm - STRIKE_RANGE_POINTS) &
            (df["strike_price"] <= atm + STRIKE_RANGE_POINTS)]

    # Now safe to print debug info
    print(f"Selected expiry date: {expiry_date}")
    print(f"Total raw options: {len(raw)}")
    print(f"After expiry filter: {len(df[df['symbol'].str.contains(expiry)])}")  # redundant now, but ok
    print(f"After strike range filter: {len(df)}")
    print(f"Number of valid CE/PE rows: {len(df[df['option_type'].isin(['CE', 'PE'])])}")

    updated = False

    # Collect qualifying strikes per side (to group alerts)
    ce_buildups = []
    pe_buildups = []

    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt = r.option_type
        oi = int(r.oi)
        ltp = float(r.ltp)
        vol = int(r.volume)

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)

        if entry is None:
            print(f"Entry not found, creating new entry in baseline Strike Key: {key}")
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
        ltp_change_pct = ((ltp - base_ltp) / base_ltp * 100) if base_ltp > 0 else 0
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
            updated = True

        # ================= EXECUTION =================
        if oi_pct >= OI_EXEC_THRESHOLD:
            print(f"Threshold breached for strike : {strike}")
            buildup_info = {
                "strike": strike,
                "oi_pct": oi_pct,
                "ltp_change_pct": ltp_change_pct,
                "vol_ok": vol_ok
            }
            # Collect instead of immediate send
            if opt == "CE":
                ce_buildups.append(buildup_info)
            else:
                pe_buildups.append(buildup_info)

            entry["state"] = "EXECUTED"
            updated = True

    # Grouped alerts after loop (one per side)
    if ce_buildups:
        # Pick first qualifying strike for the trade recommendation
        first = ce_buildups[0]
        trade_strike = first["strike"]
        trade_opt = "PE"  # same-strike contrarian

        details = "\n".join(
            f"{b['strike']} CE: +{b['oi_pct']:.0f}%"
            for b in ce_buildups
        )

        msg = (
            f"🚀 *EXECUTION SIGNAL - CE BUILDUP*\n"
            f"Buy {trade_strike} {trade_opt}\n\n"
            f"Qualifying CE strikes:\n{details}\n\n"
            f"Spot: {spot}"
        )
        send_telegram_alert(msg)

    if pe_buildups:
        first = pe_buildups[0]
        trade_strike = first["strike"]
        trade_opt = "CE"

        details = "\n".join(
            f"{b['strike']} PE: +{b['oi_pct']:.0f}%"
            for b in pe_buildups
        )

        msg = (
            f"🚀 *EXECUTION SIGNAL - PE BUILDUP*\n"
            f"Buy {trade_strike} {trade_opt}\n\n"
            f"Qualifying PE strikes:\n{details}\n\n"
            f"Spot: {spot}"
        )
        send_telegram_alert(msg)

    if not baseline["first_alert_sent"]:
        send_telegram_alert(
            f"*NIFTY OI MONITOR STARTED*\n"
            f"Spot: {spot}\nATM: {atm}"
        )
        baseline["first_alert_sent"] = True
        updated = True

    # Save if any changes
    if baseline["data"] or updated:
        if not baseline["data"]:
            print("WARNING: Processed rows but no baseline entries added (all OI < MIN_BASE_OI?)")
        save_baseline(baseline)
        print("Baseline saved — entries count:", len(baseline["data"]))
    else:
        print("No changes/alerts — baseline not saved this run")

# ================= ENTRY =================
if __name__ == "__main__":
    scan()