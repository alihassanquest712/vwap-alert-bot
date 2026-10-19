#!/usr/bin/env python3
"""
VWAP NoWick Alert Bot — 24/7 Continuous Multi-Timeframe Engine
- US30 & NAS100 (BlackBull Markets): 5m, 15m, 30m, 1h
- Gold XAUUSD (OANDA): 5m, 15m, 30m, 1h
- Forex Pairs (OANDA): 15m, 30m, 1h
Runs 24/7 continuously in GitHub Actions (Cycles every 25-30 seconds).
Zero cron delays. Instant real-time alerts.
"""

import pandas as pd
import numpy as np
import requests
import json
import os
import sys
import time
import random
import re
import string
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta
from websocket import create_connection
from concurrent.futures import ThreadPoolExecutor, as_completed

# ══════════════════════════════════════════════════════════════════════
#  EMBEDDED TRADINGVIEW WEBSOCKET CLIENT
# ══════════════════════════════════════════════════════════════════════

class TVWebSocketClient:
    def __init__(self, symbol, timeframe, candles):
        self.ws = None
        self.symbol = symbol
        self.timeframe = str(timeframe)
        self.candles = candles
        self.result_data = None

    def connect(self):
        headers = {
            'Connection': 'upgrade',
            'Host': 'data.tradingview.com',
            'Origin': 'https://data.tradingview.com',
            'Sec-WebSocket-Key': '1H41q97V8BbMKUq0knV1UA==',
            'Sec-WebSocket-Version': '13',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Pragma': 'no-cache',
            'Upgrade': 'websocket'
        }
        self.ws = create_connection('wss://data.tradingview.com/socket.io/websocket', headers=headers, timeout=8)

    def generate_session(self):
        return "qs_" + ''.join(random.choice(string.ascii_lowercase) for _ in range(12))

    def generate_chart_session(self):
        return "cs_" + ''.join(random.choice(string.ascii_lowercase) for _ in range(12))

    def create_message(self, func, param_list):
        msg = json.dumps({"m": func, "p": param_list}, separators=(',', ':'))
        return f"~m~{len(msg)}~m~{msg}"

    def send_message(self, func, param_list):
        m = self.create_message(func, param_list)
        self.ws.send(m)

    def run(self):
        chart_session = self.generate_chart_session()

        self.send_message("set_auth_token", ["unauthorized_user_token"])
        self.send_message("chart_create_session", [chart_session, ""])
        sym_spec = json.dumps({'symbol': self.symbol, 'adjustment': 'splits'})
        self.send_message("resolve_symbol", [chart_session, "symbol_1", f"={sym_spec}"])
        self.send_message("create_series", [chart_session, "s" + self.timeframe, "s" + self.timeframe, "symbol_1", self.timeframe, self.candles])

        st = time.time()
        while self.result_data is None and (time.time() - st) < 5:
            try:
                result = self.ws.recv()
                data = re.split(r'~m~\d+~m~', result)
                for item in data:
                    if item.strip():
                        parsed = json.loads(item)
                        if parsed.get('m') == 'timescale_update':
                            self.result_data = parsed['p'][1]['s' + str(self.timeframe)]['s']
                            return
            except Exception:
                break
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════════════
#  CONFIG — GitHub Secrets & Environment
# ══════════════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")
DISCORD_WEBHOOK    = os.environ.get("DISCORD_WEBHOOK",    "")
GMAIL_SENDER       = os.environ.get("GMAIL_SENDER",       "")
GMAIL_APP_PASS     = os.environ.get("GMAIL_APP_PASS",     "")
GMAIL_RECEIVER     = os.environ.get("GMAIL_RECEIVER",     "")
NTFY_TOPIC         = os.environ.get("NTFY_TOPIC") or "trading_alerts_nowick_99"

# ── Symbols & Timeframes Mapping ─────────────────────────────────────
VIP_SYMBOLS = [
    "OANDA:XAUUSD",       # Gold
    "BLACKBULL:US30",     # Dow Jones (BlackBull Markets)
    "BLACKBULL:NAS100",   # Nasdaq 100 (BlackBull Markets)
]
VIP_TIMEFRAMES = [
    ("5",  "5m"),
    ("15", "15m"),
    ("30", "30m"),
    ("60", "1h"),
]

FOREX_SYMBOLS = [
    "OANDA:EURUSD",
    "OANDA:GBPUSD",
    "OANDA:USDJPY",
    "OANDA:AUDUSD",
    "OANDA:GBPJPY",
    "OANDA:USDCAD",
    "OANDA:USDCHF",
    "OANDA:NZDUSD",
    "OANDA:EURJPY",
    "OANDA:AUDCAD",
    "OANDA:AUDJPY",
    "OANDA:GBPCAD",
    "OANDA:EURCAD",
    "OANDA:CADJPY",
]
FOREX_TIMEFRAMES = [
    ("15", "15m"),
    ("30", "30m"),
    ("60", "1h"),
]

# ── Indicator Parameters (Matching Pine Script v6 Exactly) ───────────
BAND_MULT_1        = 1.0
STRICT_ZERO_WICK   = True
WICK_TOL_TICKS     = 0
REQUIRE_PREV_COL   = True
ENABLE_INSTANT     = True
WICK_BODY_RATIO    = 1.0
ENABLE_TIME_FILTER = True
SESSION_START      = (2,  0)   # 02:00 UTC (London & NY session window)
SESSION_END        = (17, 45)  # 17:45 UTC
MAX_SWEEP_BARS     = 10
ENABLE_BB_FILTER   = True
BB_LOOKBACK        = 7
BB_ENTIRE_CANDLE   = True
ENABLE_IC_BYPASS   = True
MIN_IC_REQ         = 2
ENABLE_LEFT_FILTER = True
LEFT_WINDOW        = 20
LEFT_CLEARANCE     = True

STATE_FILE = "bot_state.json"

# ══════════════════════════════════════════════════════════════════════
#  STATE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2, default=str)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════
#  DATA FETCHING WITH RETRIES
# ══════════════════════════════════════════════════════════════════════

def fetch_tv_candles(symbol, timeframe="15", count=60):
    for attempt in range(2):
        try:
            ws = TVWebSocketClient(symbol, timeframe, count)
            ws.connect()
            ws.run()
            if not ws.result_data:
                time.sleep(0.3)
                continue
            rows = [item['v'] for item in ws.result_data]
            df = pd.DataFrame(rows, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            df['ts'] = pd.to_datetime(df['ts'], unit='s', utc=True)
            df.set_index('ts', inplace=True)
            return df
        except Exception:
            time.sleep(0.4)
    return None

def get_mintick(symbol):
    if "US30" in symbol:
        return 1.0
    elif "NAS100" in symbol:
        return 0.1
    elif "JPY" in symbol:
        return 0.001
    elif "XAU" in symbol:
        return 0.01
    else:
        return 0.00001

# ══════════════════════════════════════════════════════════════════════
#  VWAP & BANDS ENGINE
# ══════════════════════════════════════════════════════════════════════

def add_vwap_bands(df):
    df = df.copy()
    df['tp'] = (df['high'] + df['low'] + df['close']) / 3.0
    df['date'] = df.index.normalize()

    vwap_l, upper_l, lower_l = [], [], []
    for _, g in df.groupby('date', sort=True):
        vol = g['volume'].replace(0, 1)
        cv   = vol.cumsum()
        cpv  = (g['tp'] * vol).cumsum()
        cpv2 = ((g['tp'] ** 2) * vol).cumsum()

        v    = cpv / cv.replace(0, np.nan)
        diff = (cpv2 / cv.replace(0, np.nan)) - (v ** 2)
        diff = diff.clip(lower=0)
        sd   = np.sqrt(diff)

        vwap_l.extend(v.tolist())
        upper_l.extend((v + sd * BAND_MULT_1).tolist())
        lower_l.extend((v - sd * BAND_MULT_1).tolist())

    df['vwap']  = vwap_l
    df['upper'] = upper_l
    df['lower'] = lower_l
    return df

def in_session(ts):
    if not ENABLE_TIME_FILTER:
        return True
    t = ts.time()
    s_start = datetime.strptime(f"{SESSION_START[0]:02d}:{SESSION_START[1]:02d}", "%H:%M").time()
    s_end   = datetime.strptime(f"{SESSION_END[0]:02d}:{SESSION_END[1]:02d}", "%H:%M").time()
    return s_start <= t <= s_end

# ══════════════════════════════════════════════════════════════════════
#  SIGNAL DETECTION ENGINE
# ══════════════════════════════════════════════════════════════════════

def detect_signals(df, sym_state, symbol="", tf_code="15"):
    df = add_vwap_bands(df)
    mintick = get_mintick(symbol)
    tol = 0.0 if STRICT_ZERO_WICK else (WICK_TOL_TICKS * mintick)
    tf_mins = int(tf_code) if str(tf_code).isdigit() else 15

    sent_keys     = set(sym_state.get('sent_keys', []))
    new_signals   = []
    active_setups = []

    bull_ic = 0
    bull_hi = None
    bear_ic = 0
    bear_lo = None

    start_i = max(LEFT_WINDOW + BB_LOOKBACK + 2, 2)
    for i in range(start_i, len(df)):
        ts = df.index[i]
        o = df['open'].iloc[i]
        h = df['high'].iloc[i]
        l = df['low'].iloc[i]
        c = df['close'].iloc[i]
        upp  = df['upper'].iloc[i]
        low_ = df['lower'].iloc[i]

        is_g = c > o
        is_r = c < o

        # ── 1. IC Engine ──
        tl, tu = l <= low_, h >= upp
        if tl:
            bull_ic, bull_hi = (1, h) if is_g else (0, max(bull_hi or h, h))
        else:
            if bull_ic == 0:
                if is_g: bull_ic, bull_hi = 1, h
                elif bull_hi is not None: bull_hi = max(bull_hi, h)
            elif bull_hi is not None:
                if c > bull_hi: bull_ic += 1; bull_hi = h
                elif h > bull_hi: bull_hi = h

        if tu:
            bear_ic, bear_lo = (1, l) if is_r else (0, min(bear_lo or l, l))
        else:
            if bear_ic == 0:
                if is_r: bear_ic, bear_lo = 1, l
                elif bear_lo is not None: bear_lo = min(bear_lo, l)
            elif bear_lo is not None:
                if c < bear_lo: bear_ic += 1; bear_lo = l
                elif l < bear_lo: bear_lo = l

        # ── 2. Determine if candle i is completed/closed ──
        candle_end = ts + timedelta(minutes=tf_mins)
        is_closed = (i < len(df) - 1) or (datetime.now(timezone.utc) >= candle_end)

        ins = in_session(ts)

        def emit(sig_type, price, level, setup_ts=None):
            key = f"{sig_type}_{ts}_{setup_ts}" if setup_ts else f"{sig_type}_{ts}"
            # Only emit live notifications for recent bars (within 2 candle periods)
            is_recent = (datetime.now(timezone.utc) - ts) <= timedelta(minutes=tf_mins * 2)
            if key not in sent_keys:
                if is_recent:
                    new_signals.append({'type': sig_type, 'ts': ts, 'price': price,
                                        'level': level, 'setup_ts': setup_ts})
                sent_keys.add(key)

        # ── 3. Candidate & Instant Evaluation (STRICTLY ON CLOSED CANDLES ONLY) ──
        if is_closed:
            # Previous Candle Same Color
            prev_green = df['close'].iloc[i-1] > df['open'].iloc[i-1]
            prev_red   = df['close'].iloc[i-1] < df['open'].iloc[i-1]
            bp = (not REQUIRE_PREV_COL) or prev_green
            sp = (not REQUIRE_PREV_COL) or prev_red

            # Left Lookback Window Clearance
            hi_h = df['high'].iloc[max(0, i-LEFT_WINDOW):i].max() if ENABLE_LEFT_FILTER else 0
            lo_l = df['low'].iloc[max(0, i-LEFT_WINDOW):i].min() if ENABLE_LEFT_FILTER else 9e9
            bl = (h > hi_h) if LEFT_CLEARANCE else (l > hi_h)
            sl = (l < lo_l) if LEFT_CLEARANCE else (h < lo_l)
            if not ENABLE_LEFT_FILTER: bl = sl = True

            # VWAP Band Position Filter
            if not ENABLE_BB_FILTER:
                bo = so = True
            elif BB_ENTIRE_CANDLE:
                bo, so = l > upp, h < low_
            else:
                bo, so = c > upp, c < low_

            # Opposite Band No-Touch Lookback Window
            boc = boc2 = True
            if ENABLE_BB_FILTER and i >= BB_LOOKBACK:
                for k in range(BB_LOOKBACK):
                    ki = i - k
                    if df['low'].iloc[ki]  <= df['lower'].iloc[ki]: boc  = False
                    if df['high'].iloc[ki] >= df['upper'].iloc[ki]: boc2 = False

            biq = ENABLE_IC_BYPASS and bull_ic >= MIN_IC_REQ + 1
            siq = ENABLE_IC_BYPASS and bear_ic >= MIN_IC_REQ + 1
            bbb = (not ENABLE_BB_FILTER) or (bo and (boc  or biq))
            sbb = (not ENABLE_BB_FILTER) or (so and (boc2 or siq))

            # No-Wick Detection (Flat edge)
            nlw = is_g and (abs(o - l) <= tol)
            nuw = is_r and (abs(o - h) <= tol)
            cb  = nlw and bp and bl and bbb
            cs  = nuw and sp and sl and sbb

            # Instant Signal (Opposite Wick >= Body)
            body   = abs(c - o)
            bw_opp = (h - c) if is_g else 0.0
            sw_opp = (c - l) if is_r else 0.0
            hib = ENABLE_INSTANT and body > 0 and bw_opp >= body * WICK_BODY_RATIO
            his = ENABLE_INSTANT and body > 0 and sw_opp >= body * WICK_BODY_RATIO

            # Instant signals fire immediately upon candle close
            if cb and hib and ins:
                emit('INSTANT_BUY', c, l)
            if cs and his and ins:
                emit('INSTANT_SELL', c, h)

            # Register Setup Dots (Wick < Body) for sweep queue
            # Note: Dots do NOT trigger alerts; they are queued to wait for a sweep!
            bk = str(ts)
            existing = {s['ts'] for s in active_setups}
            if cb and not hib and bk not in existing:
                active_setups.append({'bar_i': i, 'side': 1,  'level': float(l), 'ts': bk})
            if cs and not his and bk not in existing:
                active_setups.append({'bar_i': i, 'side': -1, 'level': float(h), 'ts': bk})

        # ── 4. Sweep Engine Queue (Active for BOTH closed AND live running candles) ──
        rem = []
        for s in active_setups:
            el = i - s['bar_i']
            if el < 1:
                rem.append(s)
                continue
            if el <= MAX_SWEEP_BARS:
                if s['side'] == 1 and l <= s['level']:
                    if ins:
                        emit('SWEEP_BUY', c, s['level'], s['ts'])
                elif s['side'] == -1 and h >= s['level']:
                    if ins:
                        emit('SWEEP_SELL', c, s['level'], s['ts'])
                else:
                    rem.append(s)
        active_setups = rem

    sym_state.update({
        'active_setups':   active_setups,
        'bull_ic_count':   bull_ic,
        'bear_ic_count':   bear_ic,
        'bull_tracked_hi': float(bull_hi) if bull_hi else None,
        'bear_tracked_lo': float(bear_lo) if bear_lo else None,
        'sent_keys':       list(sent_keys)[-2000:],
    })
    return new_signals, sym_state

# ══════════════════════════════════════════════════════════════════════
#  ALERT SENDERS
# ══════════════════════════════════════════════════════════════════════

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'HTML'},
            timeout=10
        )
        if r.status_code == 200:
            print("    [+] Telegram: sent")
    except Exception as e:
        print(f"    [-] Telegram: {e}")

def send_discord(msg):
    if not DISCORD_WEBHOOK:
        return
    clean = msg.replace('<b>', '**').replace('</b>', '**')
    embed = {
        "embeds": [{
            "description": clean,
            "color": 0x00E676 if "BUY" in msg else 0xEF4444,
            "footer": {"text": f"VWAP NoWick Bot 24/7 • {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"}
        }]
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=embed, timeout=10)
        if r.status_code in (200, 204):
            print("    [+] Discord: sent")
    except Exception as e:
        print(f"    [-] Discord: {e}")

def send_ntfy(disp_name, sig, tf_label, plain_body):
    if not NTFY_TOPIC:
        return
    is_buy = "BUY" in sig['type']
    side = "BUY" if is_buy else "SELL"
    price_str = f"{sig['price']:.5f}" if sig['price'] < 500 else f"{sig['price']:.2f}"
    clean_title = f"{side} [{tf_label}]: {disp_name} @ {price_str}"
    
    headers = {
        "Title": clean_title,
        "Priority": "urgent",
        "Tags": "rotating_light,chart_with_upwards_trend" if is_buy else "rotating_light,chart_with_downwards_trend"
    }
    try:
        r = requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=plain_body.encode('utf-8'), headers=headers, timeout=10)
        if r.status_code == 200:
            print("    [+] ntfy: sent")
    except Exception as e:
        print(f"    [-] ntfy: {e}")

def send_gmail(subject, plain_body):
    if not GMAIL_SENDER or not GMAIL_APP_PASS or not GMAIL_RECEIVER:
        return
    try:
        is_buy = "BUY" in subject
        color  = "#00E676" if is_buy else "#EF4444"
        icon   = "[BUY]" if is_buy else "[SELL]"
        html_body = f"""
        <html><body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;padding:20px;">
          <div style="max-width:420px;margin:auto;background:#161b22;border-radius:12px;
                      border:1px solid {color};padding:20px;">
            <h2 style="color:{color};margin:0 0 12px">{icon} VWAP NoWick Signal</h2>
            <pre style="font-size:14px;line-height:1.7;white-space:pre-wrap;color:#e6edf3;">{plain_body}</pre>
            <hr style="border-color:#30363d;margin:12px 0">
            <small style="color:#8b949e;">VWAP NoWick Bot 24/7</small>
          </div>
        </body></html>
        """
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{icon} {subject}"
        msg["From"]    = GMAIL_SENDER
        msg["To"]      = GMAIL_RECEIVER
        msg.attach(MIMEText(plain_body, "plain"))
        msg.attach(MIMEText(html_body,  "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_SENDER, GMAIL_APP_PASS)
            smtp.send_message(msg)
        print("    [+] Gmail: sent")
    except Exception as e:
        print(f"    [-] Gmail: {e}")

def clean_display_name(symbol):
    if "BLACKBULL:" in symbol:
        return symbol.replace("BLACKBULL:", "") + " (BlackBull)"
    elif "OANDA:XAUUSD" in symbol:
        return "XAUUSD Gold (OANDA)"
    else:
        return symbol.replace("OANDA:", "") + " (OANDA)"

def send_all(symbol, sig, tf_label):
    disp_name = clean_display_name(symbol)
    html_msg  = fmt(disp_name, sig, tf_label)
    plain_msg = fmt_plain(disp_name, sig, tf_label)

    send_ntfy(disp_name, sig, tf_label, plain_msg)
    send_telegram(html_msg)
    send_discord(html_msg)
    send_gmail(
        subject   = f"[{tf_label}] {sig['type']} — {disp_name} @ {sig['price']:.5f}",
        plain_body= plain_msg
    )

def fmt(disp_name, sig, tf_label):
    icons  = {'INSTANT_BUY':'🟢⚡','SWEEP_BUY':'🟢🔄','INSTANT_SELL':'🔴⚡','SWEEP_SELL':'🔴🔄'}
    labels = {
        'INSTANT_BUY':  'INSTANT BUY  — Opposite Wick >= Body',
        'SWEEP_BUY':    'SWEEP BUY    — No-Wick Low Swept',
        'INSTANT_SELL': 'INSTANT SELL — Opposite Wick >= Body',
        'SWEEP_SELL':   'SWEEP SELL   — No-Wick High Swept',
    }
    disp_now = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
    return (
        f"{icons.get(sig['type'],'🔔')} <b>VWAP NoWick Signal</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Symbol     : <b>{disp_name}</b>\n"
        f"⏱ Timeframe  : <b>{tf_label}</b>\n"
        f"📌 Signal     : <b>{labels.get(sig['type'], sig['type'])}</b>\n"
        f"💰 Price      : <b>{sig['price']:.5f}</b>\n"
        f"📏 Level      : {sig['level']:.5f}\n"
        f"🕐 Candle     : {str(sig['ts'])[:19]} UTC\n"
        f"⚡ Dispatched : {disp_now}"
    )

def fmt_plain(disp_name, sig, tf_label):
    labels = {
        'INSTANT_BUY':  'INSTANT BUY  — Opposite Wick >= Body',
        'SWEEP_BUY':    'SWEEP BUY    — No-Wick Low Swept',
        'INSTANT_SELL': 'INSTANT SELL — Opposite Wick >= Body',
        'SWEEP_SELL':   'SWEEP SELL   — No-Wick High Swept',
    }
    disp_now = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
    return (
        f"Symbol     : {disp_name}\n"
        f"Timeframe  : {tf_label}\n"
        f"Signal     : {labels.get(sig['type'], sig['type'])}\n"
        f"Price      : {sig['price']:.5f}\n"
        f"Level      : {sig['level']:.5f}\n"
        f"Candle     : {str(sig['ts'])[:19]} UTC\n"
        f"Dispatched : {disp_now}"
    )

# ══════════════════════════════════════════════════════════════════════
#  GITHUB SELF-DISPATCH (Infinite 24/7 Chaining)
# ══════════════════════════════════════════════════════════════════════

def trigger_next_workflow():
    repo  = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        print("  [i] GITHUB_TOKEN or GITHUB_REPOSITORY not set (skipping self-dispatch).")
        return
    url = f"https://api.github.com/repos/{repo}/actions/workflows/bot.yml/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "VWAP-NoWick-Bot"
    }
    try:
        r = requests.post(url, headers=headers, json={"ref": "main"}, timeout=10)
        if r.status_code in (200, 204):
            print("  [+] Next 24/7 workflow cycle successfully dispatched to GitHub!")
        else:
            print(f"  [-] Failed to dispatch next workflow: {r.status_code} {r.text}")
    except Exception as e:
        print(f"  [-] Dispatch error: {e}")

# ══════════════════════════════════════════════════════════════════════
#  THREADED SCANNER WORKER
# ══════════════════════════════════════════════════════════════════════

def scan_single_task(symbol, tf_code, tf_label, state_entry):
    disp_sym = symbol.split(':')[-1]
    df = fetch_tv_candles(symbol, timeframe=tf_code, count=60)
    if df is None or len(df) < LEFT_WINDOW + BB_LOOKBACK + 5:
        return symbol, tf_code, tf_label, [], state_entry, False

    try:
        signals, updated_state = detect_signals(df, state_entry, symbol, tf_code)
        return symbol, tf_code, tf_label, signals, updated_state, True
    except Exception:
        return symbol, tf_code, tf_label, [], state_entry, False

# ══════════════════════════════════════════════════════════════════════
#  MAIN CONTINUOUS RUNNER
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if sys.stdout.encoding != 'utf-8':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    has_telegram = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    has_discord  = bool(DISCORD_WEBHOOK)
    has_gmail    = bool(GMAIL_SENDER and GMAIL_APP_PASS and GMAIL_RECEIVER)
    has_ntfy     = bool(NTFY_TOPIC)

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("\n--- SENDING TEST ALERT TO ALL ACTIVE CHANNELS ---")
        sig = {'type': 'INSTANT_BUY', 'price': 1.12450, 'level': 1.12400, 'ts': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}
        send_all("OANDA:EURUSD", sig, "15m")
        print("--- TEST ALERT DISPATCH COMPLETE ---")
        sys.exit(0)

    continuous_mode = "--once" not in sys.argv
    max_duration    = int(os.environ.get("MAX_RUNTIME_SECONDS", 5 * 3600 + 30 * 60)) # 5.5 hours
    start_time      = time.time()
    cycle           = 0

    scan_tasks = []
    for s in VIP_SYMBOLS:
        for code, label in VIP_TIMEFRAMES:
            scan_tasks.append((s, code, label))

    for s in FOREX_SYMBOLS:
        for code, label in FOREX_TIMEFRAMES:
            scan_tasks.append((s, code, label))

    print(f"\n============================================================")
    print(f"  VWAP NoWick 24/7 CONTINUOUS MULTI-TIMEFRAME ENGINE")
    print(f"  Total Tasks: {len(scan_tasks)} | Mode: {'Continuous (Real-time)' if continuous_mode else 'Single Scan'}")
    print(f"  VIP (Gold, US30, NAS100): 5m, 15m, 30m, 1h")
    print(f"  Forex (14 Pairs): 15m, 30m, 1h")
    print(f"  Channels: "
          f"{'ntfy: ON' if has_ntfy else 'ntfy: OFF'} | "
          f"{'Discord: ON' if has_discord else 'Discord: OFF'} | "
          f"{'Gmail: ON' if has_gmail else 'Gmail: OFF'} | "
          f"{'Telegram: ON' if has_telegram else 'Telegram: OFF'}")
    print(f"============================================================")

    state = load_state()

    while True:
        cycle += 1
        now_str = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        cycle_signals = 0
        t0 = time.time()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(scan_single_task, sym, code, label, state.get(f"{sym.replace(':', '_')}_{code}", {}))
                for sym, code, label in scan_tasks
            ]
            for f in as_completed(futures):
                sym, code, label, sigs, updated_state, ok = f.result()
                key = f"{sym.replace(':', '_')}_{code}"
                state[key] = updated_state
                for sig in sigs:
                    disp_sym = sym.split(':')[-1]
                    print(f"\n  [!] {disp_sym} [{label}]: {sig['type']} @ {sig['price']:.5f} (Candle: {str(sig['ts'])[:19]} UTC)")
                    send_all(sym, sig, label)
                    cycle_signals += 1

        save_state(state)
        elapsed = time.time() - t0
        print(f"[{now_str}] Cycle #{cycle} completed in {elapsed:.1f}s. New signals: {cycle_signals}")

        if not continuous_mode:
            break

        # Check 5.5 hours limit
        if time.time() - start_time >= max_duration:
            print(f"\n[!] 5.5-hour continuous run completed. Handing over to next runner...")
            trigger_next_workflow()
            sys.exit(0)

        # Sleep between scans (20s)
        time.sleep(20)
