import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone, time
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
OI_WATCH_THRESHOLD = 300  # %
OI_EXEC_THRESHOLD = 500  # %
MIN_BASE_OI = 1000
STRIKE_RANGE_POINTS = 100
CHECK_MARKET_HOURS = True
BASELINE_FILE = "baseline_oi.json"

# Quality filters
OI_BOTH_SIDES_AVOID = 250
PREMIUM_MAX_RISE = 8  # Base tolerance, adjusted dynamically
MIN_DECLINE_PCT = -1.5
MIN_CUMULATIVE_DECLINE_PCT = -10  # NEW: For already-unwound positions

# Conviction scoring
CONVICTION_MODE = os.environ.get("CONVICTION_MODE", "BALANCED").upper()
CONVICTION_THRESHOLDS = {
    "STRICT": 110,
    "BALANCED": 90,
    "AGGRESSIVE": 70
}
MIN_CONVICTION_SCORE_BASE = CONVICTION_THRESHOLDS.get(CONVICTION_MODE, 90)

# Time filters
TIME_FILTER_START = time(9, 45)
TIME_FILTER_END = time(15, 0)

# Daily limits
MAX_SIGNALS_PER_DAY = 3
MAX_WATCH_PER_DAY = 3  # NEW: Separate cap for WATCH
SCORE_IMPROVEMENT_THRESHOLD = 10

# Logging
SCORE_LOG_FILE = "conviction_scores.jsonl"
DEBUG_MODE = str(os.environ.get("DEBUG_MODE", "false")).lower() == "true"

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

# ================= SECRETS =================
CLIENT_ID = os.environ.get("CLIENT_ID")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
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

def is_trading_window():
    """Check if current time is within valid trading window for signals"""
    t = now_ist().time()
    return TIME_FILTER_START <= t <= TIME_FILTER_END

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

def log_conviction_score(signal_data):
    """Log all conviction scores for calibration analysis"""
    try:
        with open(SCORE_LOG_FILE, "a") as f:
            log_entry = {
                "timestamp": now_ist().isoformat(),
                "date": now_ist().date().isoformat(),
                "strike": signal_data["strike"],
                "opt_type": signal_data["opt_type"],
                "score": signal_data["conviction_score"],
                "tier": signal_data["tier"],
                "oi_pct": signal_data["oi_pct"],
                "opp_decline_pct": signal_data.get("opp_decline_pct", 0),
                "days_to_expiry": signal_data.get("days_to_expiry", 0),
                "signal_type": signal_data.get("signal_type", "EXECUTION"),
                "components": {
                    "strike_quality": signal_data.get("strike_quality_pts", 0),
                    "volume": signal_data.get("volume_pts", 0),
                    "velocity": signal_data.get("velocity_pts", 0),
                    "decline_magnitude": signal_data.get("decline_pts", 0),
                    "decline_streak": signal_data.get("decline_streak_pts", 0),
                    "spot_alignment": signal_data.get("spot_pts", 0),
                    "sustainability": signal_data.get("sustainability_pts", 0),
                    "cluster": signal_data.get("cluster_pts", 0),
                    "premium_behavior": signal_data.get("premium_behavior_pts", 0)
                }
            }
            f.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        print(f"Score logging error: {e}")

# ================= BASELINE =================
def load_baseline():
    if os.path.exists(BASELINE_FILE):
        try:
            with open(BASELINE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠ Baseline file corrupted → resetting")
            return create_empty_baseline()
    return create_empty_baseline()

def create_empty_baseline():
    return {
        "date": None,
        "data": {},
        "first_alert_sent": False,
        "day_open": None,
        "signals_today": 0,
        "watch_today": 0,  # NEW
        "daily_signals": []
    }

def save_baseline(b):
    with open(BASELINE_FILE, "w") as f:
        json.dump(b, f, indent=2)

def migrate_baseline_if_needed(baseline):
    """Auto-migrate old baseline format to new format"""
    migrated = False
    
    # Root-level fields
    if "day_open" not in baseline:
        baseline["day_open"] = None
        migrated = True
    if "signals_today" not in baseline:
        baseline["signals_today"] = 0
        migrated = True
    if "watch_today" not in baseline:
        baseline["watch_today"] = 0
        migrated = True
    if "daily_signals" not in baseline:
        baseline["daily_signals"] = []
        migrated = True
    
    # Entry-level fields
    for key, entry in baseline.get("data", {}).items():
        if "state" not in entry:
            entry["state"] = "NONE"
            migrated = True
        if "first_exec_time" not in entry:
            entry["first_exec_time"] = None
            migrated = True
        if "scan_count" not in entry:
            entry["scan_count"] = 0
            migrated = True
        if "prev_oi" not in entry:
            entry["prev_oi"] = entry.get("baseline_oi", 0)
            migrated = True
        if "decline_streak" not in entry:
            entry["decline_streak"] = 0
            migrated = True
    
    if migrated:
        save_baseline(baseline)
        print("✅ Baseline auto-migrated to new format")
    
    return baseline

def reset_on_new_day(b):
    today = now_ist().date().isoformat()
    if b.get("date") != today:
        print("🔄 New trading day → baseline reset")
        b["date"] = today
        b["data"] = {}
        b["first_alert_sent"] = False
        b["day_open"] = None
        b["signals_today"] = 0
        b["watch_today"] = 0
        b["daily_signals"] = []
        save_baseline(b)
    return b

# ================= API =================
def get_nifty_spot():
    try:
        q = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX"})
        return round(q["d"][0]["v"]["lp"])
    except Exception as e:
        error_msg = f"❌ *API ERROR - Spot Fetch Failed*\n{str(e)}"
        send_telegram_alert(error_msg)
        raise

def fetch_option_chain():
    try:
        r = fyers.optionchain({
            "symbol": "NSE:NIFTY50-INDEX",
            "strikecount": 40,
            "timestamp": ""
        })
        return r["data"]["optionsChain"], r["data"]["expiryData"]
    except Exception as e:
        error_msg = f"❌ *API ERROR - Option Chain Fetch Failed*\n{str(e)}"
        send_telegram_alert(error_msg)
        raise

def expiry_to_symbol_format(date_str):
    try:
        d = datetime.strptime(date_str, "%d-%m-%Y")
        yy = d.strftime("%y")
        m_num = str(d.month)
        dd = d.strftime("%d")
        m_short = d.strftime("%b").upper()

        weekly = yy + m_num + dd
        monthly = yy + m_short
        return weekly, monthly
    except Exception as e:
        print(f"Date conversion error: {e}")
        return None, None

def get_current_weekly_expiry(expiry_info):
    today = now_ist().date()
    expiries = []
    for e in expiry_info:
        try:
            exp = datetime.fromtimestamp(int(e["expiry"]), tz=IST).date()
            expiries.append(((exp - today).days, e["date"]))
        except Exception:
            continue
    expiries = [x for x in expiries if x[0] >= 0]
    if not expiries:
        print("No future expiry found")
        return None, None
    
    nearest = sorted(expiries, key=lambda x: x[0])[0]
    return nearest[1], nearest[0]  # Return (date_string, days_to_expiry)

# ================= CONVICTION SCORING HELPERS =================
def calculate_buildup_time(entry, current_time):
    """Calculate minutes since OI first crossed threshold"""
    first_exec_time = entry.get("first_exec_time")
    if first_exec_time:
        try:
            first_time = datetime.fromisoformat(first_exec_time)
            minutes = (current_time - first_time).total_seconds() / 60
            return max(0, minutes)
        except:
            return 0
    return 0

def check_adjacent_cluster(strike, opt, strike_oi_changes, exec_threshold):
    """
    Check for institutional cluster patterns:
    - Same-side buildup on adjacent strikes (ladder strategy)
    - Opposite-side decline on adjacent strikes (unwinding confirmation)
    """
    adjacent_same_side = 0
    adjacent_opp_declining = 0
    
    opp_opt = "PE" if opt == "CE" else "CE"
    
    # Check 4 adjacent strikes (±50, ±100 points)
    for offset in [-100, -50, 50, 100]:
        adj_strike = strike + offset
        
        # Same-side buildup (70% of threshold counts)
        if opt == "CE":
            ce_pct = strike_oi_changes.get(adj_strike, {}).get("CE", 0)
            if ce_pct >= exec_threshold * 0.7:
                adjacent_same_side += 1
        else:
            pe_pct = strike_oi_changes.get(adj_strike, {}).get("PE", 0)
            if pe_pct >= exec_threshold * 0.7:
                adjacent_same_side += 1
        
        # Opposite-side decline (at least 5% drop)
        opp_pct = strike_oi_changes.get(adj_strike, {}).get(opp_opt, 0)
        if opp_pct < -5:
            adjacent_opp_declining += 1
    
    return adjacent_same_side, adjacent_opp_declining

def calculate_conviction_score(buildup_data, atm, day_open, spot, strike_oi_changes):
    """
    Calculate conviction score with detailed breakdown
    Max: 190 points (with premium behavior component)
    Returns: (score, tier, emoji, details, component_scores)
    """
    score = 0
    details = []
    components = {}
    
    strike = buildup_data['strike']
    opt = buildup_data['opt_type']
    oi_pct = buildup_data['oi_pct']
    opp_decline_pct = buildup_data['opp_decline_pct']
    vol_multiplier = buildup_data['vol_multiplier']
    buildup_time_mins = buildup_data['buildup_time_mins']
    scan_count = buildup_data['scan_count']
    decline_streak = buildup_data.get('decline_streak', 0)
    exec_threshold = buildup_data.get('exec_threshold', 500)
    ltp_change_pct = buildup_data.get('ltp_change_pct', 0)
    
    # A. Strike Quality (0-30 points)
    strike_distance = abs(strike - atm)
    if strike_distance <= 25:
        pts = 30
        details.append("✓ ATM strike (+30)")
    elif strike_distance <= 50:
        pts = 20
        details.append("✓ Near ATM (+20)")
    elif strike_distance <= 75:
        pts = 10
        details.append("✓ Mid-range (+10)")
    else:
        pts = 0
        details.append("○ Far OTM (+0)")
    score += pts
    components['strike_quality_pts'] = pts
    
    # B. Volume Confirmation (0-20 points)
    if vol_multiplier >= 3:
        pts = 20
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+20)")
    elif vol_multiplier >= 2:
        pts = 10
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+10)")
    elif vol_multiplier >= 1.5:
        pts = 5
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+5)")
    else:
        pts = 0
        details.append(f"○ Low volume {vol_multiplier:.1f}x (+0)")
    score += pts
    components['volume_pts'] = pts
    
    # C. Buildup Velocity (0-25 points)
    if buildup_time_mins <= 30:
        pts = 25
        details.append(f"✓ Fast buildup {buildup_time_mins:.0f}m (+25)")
    elif buildup_time_mins <= 60:
        pts = 15
        details.append(f"✓ Moderate speed {buildup_time_mins:.0f}m (+15)")
    elif buildup_time_mins <= 120:
        pts = 5
        details.append(f"○ Gradual {buildup_time_mins:.0f}m (+5)")
    else:
        pts = 0
        details.append(f"○ Slow buildup (+0)")
    score += pts
    components['velocity_pts'] = pts
    
    # D. Opposite Decline Magnitude (0-25 points)
    opp_decline_abs = abs(opp_decline_pct)
    if opp_decline_abs >= 10:
        pts = 25
        details.append(f"✓ Heavy covering -{opp_decline_abs:.1f}% (+25)")
    elif opp_decline_abs >= 5:
        pts = 15
        details.append(f"✓ Moderate covering -{opp_decline_abs:.1f}% (+15)")
    elif opp_decline_abs >= 1.5:
        pts = 5
        details.append(f"○ Weak covering -{opp_decline_abs:.1f}% (+5)")
    else:
        pts = 0
    score += pts
    components['decline_pts'] = pts
    
    # D2. Sustained Decline Bonus (0-20 points)
    if decline_streak >= 3:
        pts = 20
        details.append(f"✓ Sustained decline {decline_streak} scans (+20)")
    elif decline_streak >= 2:
        pts = 10
        details.append(f"✓ Confirmed decline 2 scans (+10)")
    else:
        pts = 0
    score += pts
    components['decline_streak_pts'] = pts
    
    # E. Spot Momentum Alignment (0-20 points or -20 penalty)
    if day_open:
        spot_move_pct = ((spot - day_open) / day_open) * 100
        
        if opt == "CE":  # CE buildup = bearish, spot should fall
            if spot_move_pct <= -0.3:
                pts = 20
                details.append(f"✓ Spot aligned {spot_move_pct:.2f}% (+20)")
            elif spot_move_pct < 0:
                pts = 10
                details.append(f"✓ Weak alignment {spot_move_pct:.2f}% (+10)")
            else:
                pts = -20
                details.append(f"✗ MISALIGNED +{spot_move_pct:.2f}% (-20)")
        else:  # PE buildup = bullish, spot should rise
            if spot_move_pct >= 0.3:
                pts = 20
                details.append(f"✓ Spot aligned +{spot_move_pct:.2f}% (+20)")
            elif spot_move_pct > 0:
                pts = 10
                details.append(f"✓ Weak alignment +{spot_move_pct:.2f}% (+10)")
            else:
                pts = -20
                details.append(f"✗ MISALIGNED {spot_move_pct:.2f}% (-20)")
    else:
        pts = 0
        details.append("○ No day-open data (+0)")
    score += pts
    components['spot_pts'] = pts
    
    # F. Sustainability Check (0-15 points)
    if scan_count >= 3:
        pts = 15
        details.append(f"✓ Sustained {scan_count} scans (+15)")
    elif scan_count >= 2:
        pts = 10
        details.append(f"✓ Confirmed 2 scans (+10)")
    else:
        pts = 0
        details.append("○ Single scan (+0)")
    score += pts
    components['sustainability_pts'] = pts
    
    # G. Adjacent Strike Cluster (0-20 points)
    adj_same, adj_opp = check_adjacent_cluster(strike, opt, strike_oi_changes, exec_threshold)
    
    if adj_same >= 3 or adj_opp >= 2:
        pts = 20
        details.append(f"✓ Strong cluster (same:{adj_same}, opp:{adj_opp}) (+20)")
    elif adj_same >= 2 or adj_opp >= 1:
        pts = 10
        details.append(f"✓ Moderate cluster (+10)")
    else:
        pts = 0
        details.append("○ Isolated strike (+0)")
    score += pts
    components['cluster_pts'] = pts

    # H. Premium Behavior (0-15 points) - NEW
    # Validates true short buildup vs delta/gamma effects
    if ltp_change_pct <= -5:
        pts = 15
        details.append(f"✓ Premium falling {ltp_change_pct:.1f}% (+15)")
    elif ltp_change_pct <= 0:
        pts = 10
        details.append(f"✓ Premium flat {ltp_change_pct:.1f}% (+10)")
    elif ltp_change_pct <= 5:
        pts = 5
        details.append(f"○ Slight rise {ltp_change_pct:.1f}% (+5)")
    elif ltp_change_pct <= PREMIUM_MAX_RISE:
        pts = 0
        details.append(f"○ Rising {ltp_change_pct:.1f}% (+0)")
    else:
        pts = -10
        details.append(f"✗ High rise {ltp_change_pct:.1f}% (-10)")
    score += pts
    components['premium_behavior_pts'] = pts
    
    # Determine tier
    if score >= 120:
        tier = "🔥 PREMIUM"
        emoji = "🔥"
    elif score >= 90:
        tier = "✅ HIGH"
        emoji = "✅"
    elif score >= 60:
        tier = "⚠️ MEDIUM"
        emoji = "⚠️"
    else:
        tier = "❌ LOW"
        emoji = "❌"
    
    return score, tier, emoji, details, components

# ================= DAILY SIGNAL MANAGEMENT =================
def should_send_signal(baseline, new_signal_score):
    """
    Priority-based filtering: Allow up to 3 signals per day.
    If at limit, only send if new signal beats previous by 10+ points.
    """
    signals_today = baseline.get("signals_today", 0)
    
    if signals_today < MAX_SIGNALS_PER_DAY:
        return True, "UNDER_LIMIT"
    
    # At limit - check if this signal is significantly better
    daily_signals = baseline.get("daily_signals", [])
    if not daily_signals:
        return True, "FIRST_SIGNAL"
    
    # Find lowest scoring signal sent today
    min_score = min(s["score"] for s in daily_signals)
    
    if new_signal_score > min_score + SCORE_IMPROVEMENT_THRESHOLD:
        return True, f"REPLACES_LOWER (beat {min_score} by {new_signal_score - min_score})"
    
    return False, f"REJECTED (score {new_signal_score} not better than {min_score})"

def record_signal(baseline, signal_data):
    """Track signals sent today for priority filtering"""
    baseline["signals_today"] = baseline.get("signals_today", 0) + 1
    
    signal_record = {
        "time": now_ist().strftime("%H:%M"),
        "strike": signal_data["strike"],
        "opt_type": signal_data["opt_type"],
        "score": signal_data["conviction_score"],
        "tier": signal_data["tier"]
    }
    
    daily_signals = baseline.get("daily_signals", [])
    daily_signals.append(signal_record)
    
    # Keep only top 3 by score
    daily_signals = sorted(daily_signals, key=lambda x: x["score"], reverse=True)[:MAX_SIGNALS_PER_DAY]
    baseline["daily_signals"] = daily_signals

# ================= MAIN SCAN =================
def scan():
    print("▶ Scan started")

    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_on_new_day(load_baseline())
    baseline = migrate_baseline_if_needed(baseline)
    
    # Send startup ping
    if not baseline["first_alert_sent"]:
        try:
            spot = get_nifty_spot()
            atm = int(round(spot / 50) * 50)
            send_telegram_alert(
                f"✅ *NIFTY OI MONITOR STARTED*\n"
                f"Spot: {spot}\nATM: {atm}\n"
                f"Mode: {CONVICTION_MODE} ({MIN_CONVICTION_SCORE_BASE}+ base score)\n"
                f"Limits: {MAX_SIGNALS_PER_DAY} signals, {MAX_WATCH_PER_DAY} watch/day"
            )
            baseline["first_alert_sent"] = True
            save_baseline(baseline)
        except Exception as e:
            print(f"Failed to send startup ping: {e}")
            return

    # Time-of-day filter
    if not is_trading_window():
        current_time = now_ist().strftime('%H:%M')
        print(f"⏸ Outside trading window ({current_time})")
        return

    spot = get_nifty_spot()
    atm = int(round(spot / 50) * 50)
    
    # Capture day open
    if baseline.get("day_open") is None:
        baseline["day_open"] = spot
        save_baseline(baseline)
        print(f"📊 Day open captured: {spot}")

    raw, expiry_info = fetch_option_chain()
    expiry_result = get_current_weekly_expiry(expiry_info)
    if not expiry_result:
        return
    
    expiry_date, days_to_expiry = expiry_result

    weekly, monthly = expiry_to_symbol_format(expiry_date)
    if weekly is None:
        return

    # Dynamic conviction requirements based on days to expiry (weekly)
    if days_to_expiry >= 4:
        MIN_CONVICTION_SCORE = MIN_CONVICTION_SCORE_BASE
        print(f"Days to expiry: {days_to_expiry} → Early week ({MIN_CONVICTION_SCORE_BASE}+ score)")
    elif days_to_expiry >= 2:
        MIN_CONVICTION_SCORE = MIN_CONVICTION_SCORE_BASE
        print(f"Days to expiry: {days_to_expiry} → Mid-week ({MIN_CONVICTION_SCORE_BASE}+ score)")
    elif days_to_expiry == 1:
        MIN_CONVICTION_SCORE = 100
        print(f"Days to expiry: {days_to_expiry} → Pre-expiry (100+ score)")
    else:
        MIN_CONVICTION_SCORE = 120
        print(f"Days to expiry: {days_to_expiry} → Expiry day (120+ PREMIUM only)")

    # Calculate dynamic premium tolerance based on spot move
    spot_move_pct = 0
    if baseline.get("day_open"):
        spot_move_pct = ((spot - baseline["day_open"]) / baseline["day_open"]) * 100
    
    abs_spot_move = abs(spot_move_pct)
    if abs_spot_move >= 0.5:
        PREMIUM_TOLERANCE = 15
        print(f"Spot move {abs_spot_move:.2f}% → Premium tolerance: 15%")
    elif abs_spot_move >= 0.3:
        PREMIUM_TOLERANCE = 10
        print(f"Spot move {abs_spot_move:.2f}% → Premium tolerance: 10%")
    else:
        PREMIUM_TOLERANCE = PREMIUM_MAX_RISE
        print(f"Spot move {abs_spot_move:.2f}% → Premium tolerance: {PREMIUM_MAX_RISE}%")

    df = pd.DataFrame(raw)
    df_filtered = df[df["symbol"].str.contains(weekly, regex=False, na=False)]
    
    if len(df_filtered) == 0:
        df_filtered = df[df["symbol"].str.contains(monthly, regex=False, na=False)]
    
    df = df_filtered
    df = df[(df["strike_price"] >= atm - STRIKE_RANGE_POINTS) &
            (df["strike_price"] <= atm + STRIKE_RANGE_POINTS)]

    print(f"[{now_ist().strftime('%H:%M:%S')}] Spot: {spot} | ATM: {atm}")
    
    current_time = now_ist()
    
    # Pre-compute OI changes and current OI map
    strike_oi_changes = {}
    current_oi_map = {}
    
    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt = r.option_type
        oi = int(r.oi)
        
        current_oi_map[(strike, opt)] = oi

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)
        if entry is None or entry.get("baseline_oi", 0) < MIN_BASE_OI:
            continue

        base_oi = entry["baseline_oi"]
        oi_pct = ((oi - base_oi) / base_oi) * 100 if base_oi > 0 else 0

        if strike not in strike_oi_changes:
            strike_oi_changes[strike] = {}
        strike_oi_changes[strike][opt] = oi_pct

    # Main processing loop
    updated = False
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
            baseline["data"][key] = {
                "baseline_oi": oi,
                "baseline_ltp": ltp,
                "baseline_vol": vol,
                "prev_oi": oi,
                "state": "NONE",
                "first_exec_time": None,
                "scan_count": 0,
                "decline_streak": 0
            }
            updated = True
            continue

        base_oi = entry["baseline_oi"]
        base_ltp = entry["baseline_ltp"]
        base_vol = entry["baseline_vol"]
        state = entry["state"]

        if base_oi < MIN_BASE_OI:
            continue

        oi_pct = ((oi - base_oi) / base_oi) * 100
        ltp_change_pct = ((ltp - base_ltp) / base_ltp * 100) if base_ltp > 0 else 0
        vol_multiplier = vol / base_vol if base_vol > 0 else 1

        # ================= WATCH (WITH FILTERS) =================
        if oi_pct >= OI_WATCH_THRESHOLD and state == "NONE":
            # Basic quality filters for WATCH
            ce_pct = strike_oi_changes.get(strike, {}).get("CE", 0)
            pe_pct = strike_oi_changes.get(strike, {}).get("PE", 0)
            conflicted = (ce_pct >= OI_BOTH_SIDES_AVOID and pe_pct >= OI_BOTH_SIDES_AVOID)
            
            # Strike proximity filter
            too_far_otm = abs(strike - atm) > 100
            
            # Daily cap for WATCH
            watch_limit_hit = baseline.get("watch_today", 0) >= MAX_WATCH_PER_DAY
            
            if not conflicted and not too_far_otm and not watch_limit_hit:
                send_telegram_alert(
                    f"👁 *OI WATCH*\n"
                    f"{strike} {opt}\n"
                    f"OI +{oi_pct:.0f}%\n"
                    f"Not actionable yet - monitoring\n"
                    f"Spot: {spot}"
                )
                baseline["watch_today"] = baseline.get("watch_today", 0) + 1
                updated = True
            elif watch_limit_hit and DEBUG_MODE:
                print(f"⏸ WATCH suppressed: daily limit ({MAX_WATCH_PER_DAY})")
            elif conflicted and DEBUG_MODE:
                print(f"⏸ WATCH suppressed: conflicted at {strike}")
            
            entry["state"] = "WATCH"
            updated = True

        # Track buildup time and scan count
        if oi_pct >= OI_EXEC_THRESHOLD:
            if entry.get("first_exec_time") is None:
                entry["first_exec_time"] = current_time.isoformat()
                entry["scan_count"] = 1
                updated = True
            else:
                entry["scan_count"] = entry.get("scan_count", 0) + 1
                updated = True
        else:
            if entry.get("first_exec_time"):
                entry["first_exec_time"] = None
                entry["scan_count"] = 0
                updated = True

        # ================= EXECUTION =================
        if state == "EXECUTED":
            continue

        # Use dynamic premium tolerance
        is_aggressive_writing = (oi_pct >= OI_EXEC_THRESHOLD) and (ltp_change_pct <= PREMIUM_TOLERANCE)

        if is_aggressive_writing:
            # Conflict check
            ce_pct_here = strike_oi_changes.get(strike, {}).get("CE", 0)
            pe_pct_here = strike_oi_changes.get(strike, {}).get("PE", 0)
            conflicted = (ce_pct_here >= OI_BOTH_SIDES_AVOID and pe_pct_here >= OI_BOTH_SIDES_AVOID)

            if conflicted:
                print(f"⛔ Skipping conflicted: CE +{ce_pct_here:.0f}%, PE +{pe_pct_here:.0f}%")
                continue

            # ================= DUAL DECLINE CHECK (NEW) =================
            opp_opt = "PE" if opt == "CE" else "CE"
            opp_key = f"{opp_opt}_{strike}"
            opp_entry = baseline["data"].get(opp_key)

            if not opp_entry:
                print(f"⚠️ No opposite entry for {strike} {opt}")
                continue

            opp_current_oi = current_oi_map.get((strike, opp_opt), 0)
            if opp_current_oi == 0:
                print(f"⚠️ No opposite data for {opp_opt} at {strike}")
                continue

            opp_prev_oi = opp_entry.get("prev_oi", opp_entry.get("baseline_oi", 0))
            opp_baseline_oi = opp_entry.get("baseline_oi", 0)

            # Calculate both scan-to-scan and cumulative declines
            opp_decline_pct = ((opp_current_oi - opp_prev_oi) / opp_prev_oi * 100) if opp_prev_oi > 0 else 0
            opp_cumulative_decline_pct = ((opp_current_oi - opp_baseline_oi) / opp_baseline_oi * 100) if opp_baseline_oi > 0 else 0

            # Check EITHER scan-to-scan decline OR significant cumulative unwinding
            scan_to_scan_covering = (opp_current_oi < opp_prev_oi) and (opp_decline_pct <= MIN_DECLINE_PCT)
            already_unwound = opp_cumulative_decline_pct <= MIN_CUMULATIVE_DECLINE_PCT

            is_covering = scan_to_scan_covering or already_unwound

            if not is_covering:
                # Reset decline streak
                opp_entry["decline_streak"] = 0
                updated = True
                print(f"⚠️ Rejected {strike} {opt}: opposite not covering")
                print(f"   Scan-to-scan: {opp_decline_pct:+.2f}% (need <= {MIN_DECLINE_PCT}%)")
                print(f"   Cumulative: {opp_cumulative_decline_pct:+.2f}% (need <= {MIN_CUMULATIVE_DECLINE_PCT}%)")
                continue

            # Update decline streak
            opp_entry["decline_streak"] = opp_entry.get("decline_streak", 0) + 1
            updated = True

            # Use the more significant decline for scoring
            opp_decline_for_scoring = min(opp_decline_pct, opp_cumulative_decline_pct)

            print(f"✓ Covering detected: {opp_opt} {opp_decline_for_scoring:.1f}%")
            print(f"  Scan-to-scan: {opp_decline_pct:+.2f}%, Cumulative: {opp_cumulative_decline_pct:+.2f}%")
            print(f"  ({opp_prev_oi:,} → {opp_current_oi:,})")
            print(f"  Decline streak: {opp_entry['decline_streak']} scans")

            # Prepare buildup data for scoring
            buildup_data = {
                "strike": strike,
                "opt_type": opt,
                "oi_pct": oi_pct,
                "ltp_change_pct": ltp_change_pct,
                "vol_multiplier": vol_multiplier,
                "opp_decline_pct": opp_decline_for_scoring,
                "decline_streak": opp_entry["decline_streak"],
                "buildup_time_mins": calculate_buildup_time(entry, current_time),
                "scan_count": entry.get("scan_count", 1),
                "exec_threshold": OI_EXEC_THRESHOLD,
                "days_to_expiry": days_to_expiry
            }

            # Calculate conviction score
            score, tier, emoji, score_details, components = calculate_conviction_score(
                buildup_data, atm, baseline.get("day_open"), spot, strike_oi_changes
            )

            print(f"📊 Conviction Score: {score} - {tier}")
            for detail in score_details:
                print(f"   {detail}")

            # Check minimum score threshold
            if score < MIN_CONVICTION_SCORE:
                print(f"⚠️ Score {score} below threshold {MIN_CONVICTION_SCORE}")
                continue

            # Add score metadata
            buildup_data.update({
                "conviction_score": score,
                "tier": tier,
                "emoji": emoji,
                "score_details": score_details,
                **components  # Add individual component scores
            })

            # Collect buildups
            if opt == "CE":
                ce_buildups.append(buildup_data)
            else:
                pe_buildups.append(buildup_data)

            entry["state"] = "EXECUTED"
            updated = True

    # Update prev_oi for all entries
    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt = r.option_type
        oi = int(r.oi)
        key = f"{opt}_{strike}"
        
        if key in baseline["data"]:
            baseline["data"][key]["prev_oi"] = oi
            updated = True

    # Process and send alerts with priority filtering
    all_buildups = []
    
    if ce_buildups:
        ce_buildups_sorted = sorted(ce_buildups, key=lambda x: x["conviction_score"], reverse=True)
        for buildup in ce_buildups_sorted:
            buildup["trade_opt"] = "PE"  # Buy opposite
            all_buildups.append(buildup)
    
    if pe_buildups:
        pe_buildups_sorted = sorted(pe_buildups, key=lambda x: x["conviction_score"], reverse=True)
        for buildup in pe_buildups_sorted:
            buildup["trade_opt"] = "CE"  # Buy opposite
            all_buildups.append(buildup)

    # Sort all buildups by score
    all_buildups_sorted = sorted(all_buildups, key=lambda x: x["conviction_score"], reverse=True)

    # Send alerts with daily limit
    for buildup in all_buildups_sorted:
        should_send, reason = should_send_signal(baseline, buildup["conviction_score"])
        
        if should_send:
            # Format score breakdown
            score_breakdown = "\n".join(f"  {d}" for d in buildup["score_details"])
            
            # Determine if replacing
            is_replacement = "REPLACES" in reason

            msg = (
                f"{buildup['emoji']} *EXECUTION SIGNAL - {buildup['opt_type']} BUILDUP*\n"
                f"*Tier: {buildup['tier']} | Score: {buildup['conviction_score']}/190*\n"
            )
            
            if is_replacement:
                msg += f"*(Replaced lower signal)*\n"
            
            msg += (
                f"\n*Action: Buy {buildup['strike']} {buildup['trade_opt']}*\n\n"
                f"📊 *Score Breakdown:*\n{score_breakdown}\n\n"
                f"OI: +{buildup['oi_pct']:.0f}% | Opp: {buildup['opp_decline_pct']:.1f}%\n"
                f"Premium: {buildup['ltp_change_pct']:+.1f}%\n"
                f"Spot: {spot}\n"
                f"Signals today: {baseline.get('signals_today', 0) + 1}/{MAX_SIGNALS_PER_DAY}"
            )
            
            send_telegram_alert(msg)
            
            # Record signal
            record_signal(baseline, buildup)
            
            # Log for calibration
            buildup["signal_type"] = "EXECUTION"
            log_conviction_score(buildup)
            
            updated = True
            
            print(f"✅ Signal sent: {buildup['strike']} {buildup['opt_type']} (Score: {buildup['conviction_score']})")
        else:
            print(f"⏸ Signal skipped: {buildup['strike']} {buildup['opt_type']} - {reason}")
            
            # Still log skipped signals for analysis
            buildup_copy = buildup.copy()
            buildup_copy["skipped"] = True
            buildup_copy["skip_reason"] = reason
            buildup_copy["signal_type"] = "EXECUTION_SKIPPED"
            log_conviction_score(buildup_copy)

    # Save baseline
    if updated:
        save_baseline(baseline)
        print(f"✓ Baseline saved — {len(baseline['data'])} entries")
    else:
        print("No changes this scan")

# ================= ENTRY =================
if __name__ == "__main__":
    try:
        scan()
    except Exception as e:
        print(f"Fatal error: {e}")
        send_telegram_alert(f"❌ *SCANNER CRASHED*\n{str(e)}")