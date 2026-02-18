import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone, time
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
OI_WATCH_THRESHOLD    = 300    # %
OI_EXEC_THRESHOLD     = 500    # %
MIN_BASE_OI           = 1000
STRIKE_RANGE_POINTS   = 100
CHECK_MARKET_HOURS    = True
BASELINE_FILE         = "baseline_oi.json"

# Additional thresholds for better alignment with the video strategy
OI_BOTH_SIDES_AVOID     = 250      # % if both CE & PE >= this → skip (range-bound)
PREMIUM_DROP_TOLERANCE  = 5        # max premium % change allowed during buildup (confirms short)
MIN_DECLINE_PCT = -1.5

# ================= CONVICTION SCORING CONFIG =================
MIN_CONVICTION_SCORE = 90          # Balanced mode: Premium (120+) + High (90-119)
TIME_FILTER_START = time(9, 45)    # No signals before 9:45 AM
TIME_FILTER_END = time(15, 0)      # No signals after 3:00 PM

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

# ================= BASELINE =================
def load_baseline():
    if os.path.exists(BASELINE_FILE):
        try:
            with open(BASELINE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠ Baseline file corrupted → resetting")
            return {"date": None, "data": {}, "first_alert_sent": False, "day_open": None}
    return {"date": None, "data": {}, "first_alert_sent": False, "day_open": None}

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
        b["day_open"] = None
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
        return None
    return sorted(expiries, key=lambda x: x[0])[0][1]

# ================= CONVICTION SCORING =================
def calculate_conviction_score(buildup_info, atm, day_open, spot, strike_oi_changes):
    """
    Calculate conviction score with detailed breakdown
    Returns: (score, tier, details_list)
    """
    score = 0
    details = []
    
    strike = buildup_info['strike']
    opt = buildup_info['opt_type']
    oi_pct = buildup_info['oi_pct']
    opp_decline_pct = buildup_info['opp_decline_pct']
    vol_multiplier = buildup_info['vol_multiplier']
    buildup_time_mins = buildup_info['buildup_time_mins']
    scan_count = buildup_info['scan_count']
    
    # A. Strike Quality (0-30 points)
    strike_distance = abs(strike - atm)
    if strike_distance <= 25:
        score += 30
        details.append("✓ ATM strike (+30)")
    elif strike_distance <= 50:
        score += 20
        details.append("✓ Near ATM (+20)")
    elif strike_distance <= 75:
        score += 10
        details.append("✓ Mid-range (+10)")
    else:
        details.append("○ Far OTM (+0)")
    
    # B. Volume Confirmation (0-20 points)
    if vol_multiplier >= 3:
        score += 20
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+20)")
    elif vol_multiplier >= 2:
        score += 10
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+10)")
    elif vol_multiplier >= 1.5:
        score += 5
        details.append(f"✓ Volume {vol_multiplier:.1f}x (+5)")
    else:
        details.append(f"○ Low volume {vol_multiplier:.1f}x (+0)")
    
    # C. Buildup Velocity (0-25 points)
    if buildup_time_mins <= 30:
        score += 25
        details.append(f"✓ Fast buildup {buildup_time_mins}m (+25)")
    elif buildup_time_mins <= 60:
        score += 15
        details.append(f"✓ Moderate speed {buildup_time_mins}m (+15)")
    elif buildup_time_mins <= 120:
        score += 5
        details.append(f"○ Gradual buildup {buildup_time_mins}m (+5)")
    else:
        details.append(f"○ Slow buildup {buildup_time_mins}m (+0)")
    
    # D. Opposite Decline Magnitude (0-25 points)
    opp_decline_abs = abs(opp_decline_pct)
    if opp_decline_abs >= 10:
        score += 25
        details.append(f"✓ Heavy covering -{opp_decline_abs:.1f}% (+25)")
    elif opp_decline_abs >= 5:
        score += 15
        details.append(f"✓ Moderate covering -{opp_decline_abs:.1f}% (+15)")
    elif opp_decline_abs >= 1.5:
        score += 5
        details.append(f"○ Weak covering -{opp_decline_abs:.1f}% (+5)")
    
    # E. Spot Momentum Alignment (0-20 points or -20 penalty)
    if day_open:
        spot_move_pct = ((spot - day_open) / day_open) * 100
        
        if opt == "CE":  # CE buildup = bearish, spot should be falling
            if spot_move_pct <= -0.3:
                score += 20
                details.append(f"✓ Spot aligned {spot_move_pct:.2f}% (+20)")
            elif spot_move_pct < 0:
                score += 10
                details.append(f"✓ Weak alignment {spot_move_pct:.2f}% (+10)")
            else:
                score -= 20
                details.append(f"✗ MISALIGNED +{spot_move_pct:.2f}% (-20)")
        else:  # PE buildup = bullish, spot should be rising
            if spot_move_pct >= 0.3:
                score += 20
                details.append(f"✓ Spot aligned +{spot_move_pct:.2f}% (+20)")
            elif spot_move_pct > 0:
                score += 10
                details.append(f"✓ Weak alignment +{spot_move_pct:.2f}% (+10)")
            else:
                score -= 20
                details.append(f"✗ MISALIGNED {spot_move_pct:.2f}% (-20)")
    else:
        details.append("○ No day-open data (+0)")
    
    # F. Sustainability Check (0-15 points)
    if scan_count >= 3:
        score += 15
        details.append(f"✓ Sustained {scan_count} scans (+15)")
    elif scan_count >= 2:
        score += 10
        details.append(f"✓ Confirmed 2 scans (+10)")
    else:
        details.append("○ Single scan (+0)")
    
    # G. Adjacent Strike Confirmation (0-15 points)
    # Count adjacent strikes also building (±50 points from current)
    adjacent_building = 0
    for offset in [-50, 50]:
        adj_strike = strike + offset
        ce_pct = strike_oi_changes.get(adj_strike, {}).get("CE", 0)
        pe_pct = strike_oi_changes.get(adj_strike, {}).get("PE", 0)
        
        if opt == "CE" and ce_pct >= OI_EXEC_THRESHOLD:
            adjacent_building += 1
        elif opt == "PE" and pe_pct >= OI_EXEC_THRESHOLD:
            adjacent_building += 1
    
    if adjacent_building >= 2:
        score += 15
        details.append(f"✓ {adjacent_building} adjacent strikes (+15)")
    elif adjacent_building >= 1:
        score += 10
        details.append(f"✓ 1 adjacent strike (+10)")
    else:
        details.append("○ Isolated strike (+0)")
    
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
    
    return score, tier, emoji, details


# ================= BASELINE MIGRATION =================
def migrate_baseline_if_needed(baseline):
    """
    Automatically migrate old baseline format to new format.
    Safe to call every time - only modifies if needed.
    """
    migrated = False
    
    # Add root-level day_open if missing
    if "day_open" not in baseline:
        baseline["day_open"] = None
        migrated = True
        print("🔄 Migrated: Added day_open to baseline")
    
    # Add entry-level fields if missing
    for key, entry in baseline.get("data", {}).items():
        if "first_exec_time" not in entry:
            entry["first_exec_time"] = None
            entry["scan_count"] = 0
            migrated = True
    
    if migrated:
        save_baseline(baseline)
        print("✅ Baseline auto-migrated to new format")
    
    return baseline

# ================= SCAN =================
def scan():
    print("▶ Scan started")

    if CHECK_MARKET_HOURS and not is_market_open():
        print("⏱ Market closed")
        return

    baseline = reset_on_new_day(load_baseline())
    
    # AUTO-MIGRATE: Add this line right after loading baseline
    baseline = migrate_baseline_if_needed(baseline)
    
    # Send startup ping first (before any trade alerts)
    if not baseline["first_alert_sent"]:
        # ... rest of code unchanged ...
        try:
            spot = get_nifty_spot()
            atm = int(round(spot / 50) * 50)
            send_telegram_alert(
                f"✅ *NIFTY OI MONITOR STARTED*\n"
                f"Spot: {spot}\nATM: {atm}\n"
                f"Mode: Balanced (90+ score)"
            )
            baseline["first_alert_sent"] = True
            save_baseline(baseline)
        except Exception as e:
            print(f"Failed to send startup ping: {e}")
            return

    # Time-of-day filter
    if not is_trading_window():
        current_time = now_ist().strftime('%H:%M')
        print(f"⏸ Outside trading window ({current_time}) - signals only between 9:45 AM - 3:00 PM")
        return

    spot = get_nifty_spot()
    atm = int(round(spot / 50) * 50)
    
    # Capture day open (first scan after 9:15 AM)
    if baseline.get("day_open") is None:
        baseline["day_open"] = spot
        save_baseline(baseline)
        print(f"📊 Day open captured: {spot}")

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
    if len(df_filtered) == 0:
        print("Weekly filter failed — trying monthly format")
        df_filtered = df[df["symbol"].str.contains(monthly, regex=False, na=False)]
        print(f"After monthly expiry filter: {len(df_filtered)}")

    df = df_filtered

    # Strike range filter
    df = df[(df["strike_price"] >= atm - STRIKE_RANGE_POINTS) &
            (df["strike_price"] <= atm + STRIKE_RANGE_POINTS)]

    print(f"[{now_ist().strftime('%H:%M:%S')}] Spot: {spot} | ATM: {atm} | Range: {atm - STRIKE_RANGE_POINTS} – {atm + STRIKE_RANGE_POINTS}")
    
    current_time = now_ist()
    
    # === Pre-compute OI % changes and build current OI lookup ===
    strike_oi_changes = {}   # strike -> {"CE": pct, "PE": pct}
    current_oi_map = {}      # (strike, opt) -> current_oi
    
    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt    = r.option_type
        oi     = int(r.oi)
        
        # Store current OI for later lookup
        current_oi_map[(strike, opt)] = oi

        key = f"{opt}_{strike}"
        entry = baseline["data"].get(key)
        if entry is None or entry.get("baseline_oi", 0) < MIN_BASE_OI:
            continue

        base_oi = entry["baseline_oi"]
        oi_pct  = ((oi - base_oi) / base_oi) * 100 if base_oi > 0 else 0

        if strike not in strike_oi_changes:
            strike_oi_changes[strike] = {}
        strike_oi_changes[strike][opt] = oi_pct

    # === Main processing loop ===
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
            print(f"Creating baseline entry for {key}")
            baseline["data"][key] = {
                "baseline_oi": oi,
                "baseline_ltp": ltp,
                "baseline_vol": vol,
                "prev_oi": oi,
                "state": "NONE",
                "first_exec_time": None,  # Track when first crossed threshold
                "scan_count": 0           # Consecutive scans above threshold
            }
            updated = True
            continue

        base_oi  = entry["baseline_oi"]
        base_ltp = entry["baseline_ltp"]
        base_vol = entry["baseline_vol"]
        state    = entry["state"]

        if base_oi < MIN_BASE_OI:
            continue

        oi_pct = ((oi - base_oi) / base_oi) * 100
        ltp_change_pct = ((ltp - base_ltp) / base_ltp * 100) if base_ltp > 0 else 0
        vol_multiplier = vol / base_vol if base_vol > 0 else 1

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
        # Track when first crossed threshold
        if oi_pct >= OI_EXEC_THRESHOLD:
            if entry.get("first_exec_time") is None:
                entry["first_exec_time"] = current_time.isoformat()
                entry["scan_count"] = 1
                updated = True
            else:
                entry["scan_count"] = entry.get("scan_count", 0) + 1
                updated = True
        else:
            # Reset if dropped below threshold
            if entry.get("first_exec_time"):
                entry["first_exec_time"] = None
                entry["scan_count"] = 0
                updated = True
        
        # Only process if not already executed (prevent repeat alerts)
        if state == "EXECUTED":
            continue
            
        # Aggressive writing confirmation (OI spike + premium flat/falling)
        is_aggressive_writing = (oi_pct >= OI_EXEC_THRESHOLD) and (ltp_change_pct <= PREMIUM_DROP_TOLERANCE)

        if is_aggressive_writing:
            # Check conflict (both sides building strongly = range-bound)
            ce_pct_here = strike_oi_changes.get(strike, {}).get("CE", 0)
            pe_pct_here = strike_oi_changes.get(strike, {}).get("PE", 0)
            conflicted = (ce_pct_here >= OI_BOTH_SIDES_AVOID and pe_pct_here >= OI_BOTH_SIDES_AVOID)

            if conflicted:
                print(f"⛔ Skipping conflicted buildup at {strike}: CE +{ce_pct_here:.0f}%, PE +{pe_pct_here:.0f}%")
                continue

            # === Check if opposite side is DECLINING (hard filter) ===
            opp_opt = "PE" if opt == "CE" else "CE"
            opp_key = f"{opp_opt}_{strike}"
            opp_entry = baseline["data"].get(opp_key)

            if opp_entry:
                opp_current_oi = current_oi_map.get((strike, opp_opt), 0)
                
                if opp_current_oi == 0:
                    print(f"⚠️ No current data for opposite {opp_opt} at {strike}")
                    continue
                
                opp_prev_oi = opp_entry.get("prev_oi", opp_entry.get("baseline_oi", 0))
                opp_decline_pct = ((opp_current_oi - opp_prev_oi) / opp_prev_oi * 100) if opp_prev_oi > 0 else 0
                is_covering = (opp_current_oi < opp_prev_oi) and (opp_decline_pct <= MIN_DECLINE_PCT)
                
                if not is_covering:
                    opp_pct = strike_oi_changes.get(strike, {}).get(opp_opt, 0)
                    print(f"⚠️ Rejected {strike} {opt}: opposite {opp_opt} not declining (current: {opp_current_oi}, prev: {opp_prev_oi}, {opp_pct:+.1f}%)")
                    continue
                    
                # Calculate buildup time
                first_exec_time = entry.get("first_exec_time")
                if first_exec_time:
                    buildup_time_mins = (current_time - datetime.fromisoformat(first_exec_time)).total_seconds() / 60
                else:
                    buildup_time_mins = 0
                
                # Debug print when covering detected
                print(f"✓ Covering detected at {strike} {opt}: {opp_opt} {opp_decline_pct:.1f}% "
                      f"({opp_prev_oi} → {opp_current_oi})")
                
                # Prepare buildup info for conviction scoring
                buildup_info = {
                    "strike": strike,
                    "opt_type": opt,
                    "oi_pct": oi_pct,
                    "ltp_change_pct": ltp_change_pct,
                    "vol_multiplier": vol_multiplier,
                    "opp_decline_pct": opp_decline_pct,
                    "buildup_time_mins": buildup_time_mins,
                    "scan_count": entry.get("scan_count", 1)
                }
                
                # === CONVICTION SCORING ===
                conviction_score, tier, emoji, score_details = calculate_conviction_score(
                    buildup_info, atm, baseline.get("day_open"), spot, strike_oi_changes
                )
                
                print(f"📊 Conviction Score: {conviction_score} - {tier}")
                for detail in score_details:
                    print(f"   {detail}")
                
                # Filter by minimum conviction score (Balanced mode: 90+)
                if conviction_score < MIN_CONVICTION_SCORE:
                    print(f"⚠️ Score {conviction_score} below threshold {MIN_CONVICTION_SCORE} - signal ignored")
                    continue
                
                # Add to buildups with score
                buildup_info["conviction_score"] = conviction_score
                buildup_info["tier"] = tier
                buildup_info["emoji"] = emoji
                buildup_info["score_details"] = score_details
                
                if opt == "CE":
                    ce_buildups.append(buildup_info)
                else:
                    pe_buildups.append(buildup_info)
                    
                entry["state"] = "EXECUTED"
                updated = True
            else:
                print(f"⚠️ No opposite side entry for {strike} {opt}")

    # === Update prev_oi for all entries (for next scan) ===
    for _, r in df.iterrows():
        strike = int(r.strike_price)
        opt = r.option_type
        oi = int(r.oi)
        key = f"{opt}_{strike}"
        
        if key in baseline["data"]:
            baseline["data"][key]["prev_oi"] = oi
            updated = True

    # ================= Send grouped alerts =================
    if ce_buildups:
        # Sort by conviction score (not just OI%)
        ce_buildups_sorted = sorted(ce_buildups, key=lambda x: x["conviction_score"], reverse=True)
        best = ce_buildups_sorted[0]
        trade_strike = best["strike"]
        trade_opt = "PE"

        # Format score breakdown
        score_breakdown = "\n".join(f"  {d}" for d in best["score_details"])

        details = "\n".join(
            f"{b['strike']} CE: +{b['oi_pct']:.0f}% (PE {b['opp_decline_pct']:.1f}%) | Score: {b['conviction_score']}"
            for b in ce_buildups_sorted[:3]  # Show top 3
        )

        msg = (
            f"{best['emoji']} *EXECUTION SIGNAL - CE BUILDUP*\n"
            f"*Tier: {best['tier']} | Score: {best['conviction_score']}/150*\n\n"
            f"*Action: Buy {trade_strike} {trade_opt}*\n\n"
            f"📊 *Score Breakdown:*\n{score_breakdown}\n\n"
            f"*Top Signals:*\n{details}\n\n"
            f"Spot: {spot}"
        )
        send_telegram_alert(msg)

    if pe_buildups:
        # Sort by conviction score
        pe_buildups_sorted = sorted(pe_buildups, key=lambda x: x["conviction_score"], reverse=True)
        best = pe_buildups_sorted[0]
        trade_strike = best["strike"]
        trade_opt = "CE"

        # Format score breakdown
        score_breakdown = "\n".join(f"  {d}" for d in best["score_details"])

        details = "\n".join(
            f"{b['strike']} PE: +{b['oi_pct']:.0f}% (CE {b['opp_decline_pct']:.1f}%) | Score: {b['conviction_score']}"
            for b in pe_buildups_sorted[:3]  # Show top 3
        )

        msg = (
            f"{best['emoji']} *EXECUTION SIGNAL - PE BUILDUP*\n"
            f"*Tier: {best['tier']} | Score: {best['conviction_score']}/150*\n\n"
            f"*Action: Buy {trade_strike} {trade_opt}*\n\n"
            f"📊 *Score Breakdown:*\n{score_breakdown}\n\n"
            f"*Top Signals:*\n{details}\n\n"
            f"Spot: {spot}"
        )
        send_telegram_alert(msg)

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