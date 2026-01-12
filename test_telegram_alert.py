import os
import requests

# ================= SECRETS =================
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram alert skipped: missing token/chat_id")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        print("Telegram API Response:", r.status_code, r.text)
        if r.status_code == 200:
            print("✅ Telegram alert sent successfully")
        else:
            print("❌ Telegram alert failed")
    except Exception as e:
        print("❌ Telegram alert exception:", e)

if __name__ == "__main__":
    send_telegram_alert("✅ Test Telegram alert from GitHub Actions - Everything works!")
