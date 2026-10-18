#!/usr/bin/env python3
"""
VWAP NoWick Alert Bot — Cloud Version (GitHub Actions)
Ek baar run hota hai, last completed candle check karta hai.
State file se duplicates avoid karta hai.
"""

import ccxt
import pandas as pd
import numpy as np
import requests
import json
import os
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════════════════════
#  CONFIG — GitHub Secrets se aata hai (Secrets mein daalo, yahan nahi!)
# ══════════════════════════════════════════════════════════════════════

# ── Telegram (Optional — khali chhodo agar use nahi karna) ────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# ── Discord (Optional — khali chhodo agar use nahi karna) ────────────
DISCORD_WEBHOOK    = os.environ.get("DISCORD_WEBHOOK",    "")

# ── Gmail (Optional — khali chhodo agar use nahi karna) ──────────────
GMAIL_SENDER    = os.environ.get("GMAIL_SENDER",    "")  # aapka@gmail.com
GMAIL_APP_PASS  = os.environ.get("GMAIL_APP_PASS",  "")  # 16-digit App Password
GMAIL_RECEIVER  = os.environ.get("GMAIL_RECEIVER",  "")  # alert kahan bhejein (same ya alag)

# ── Ye settings change karo apne hisaab se ───────────────────────────
EXCHANGE_ID = "binance"
SYMBOLS     = [
    "BTC/USDT",
    "ETH/USDT",
    # "SOL/USDT",
]
TIMEFRAME   = "5m"

# ── Indicator Settings (Pine Script se match karo) ────────────────────
BAND_MULT_1        = 1.0
STRICT_ZERO_WICK   = False
WICK_TOL_TICKS     = 1
REQUIRE_PREV_COL   = True
ENABLE_INSTANT     = True
WICK_BODY_RATIO    = 1.0
ENABLE_TIME_FILTER = True
SESSION_START      = (2,  0)   # 02:00 UTC
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
exchange = getattr(ccxt, EXCHANGE_ID)({'enableRateLimit': True})

# ══════════════════════════════════════════════════════════════════════
#  STATE
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
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)

# ══════════════════════════════════════════════════════════════════════
#  DATA
# ══════════════════════════════════════════════════════════════════════

def fetch_ohlcv(symbol, limit=300):
    raw = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=limit)
    df  = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    df.set_index('ts', inplace=True)
    return df.iloc[:-1]  # Drop last incomplete bar

# ══════════════════════════════════════════════════════════════════════
#  VWAP BANDS
# ══════════════════════════════════════════════════════════════════════

def add_vwap_bands(df):
    df = df.copy()
    df['tp']   = (df['high'] + df['low'] + df['close']) / 3.0
    df['date'] = df.index.normalize()
    vwap_l, upper_l, lower_l = [], [], []
    for _, g in df.groupby('date', sort=True):
        cv   = g['volume'].cumsum()
        cpv  = (g['tp'] * g['volume']).cumsum()
        cpv2 = ((g['tp']**2) * g['volume']).cumsum()
        v    = cpv / cv.replace(0, np.nan)
        sd   = np.sqrt(((cpv2 / cv.replace(0, np.nan)) - v**2).clip(0))
        vwap_l.extend(v.tolist())
        upper_l.extend((v + sd * BAND_MULT_1).tolist())
        lower_l.extend((v - sd * BAND_MULT_1).tolist())
    df['vwap'], df['upper'], df['lower'] = vwap_l, upper_l, lower_l
    return df

# ══════════════════════════════════════════════════════════════════════
#  SESSION CHECK
# ══════════════════════════════════════════════════════════════════════

def in_session(ts):
    if not ENABLE_TIME_FILTER:
        return True
    t = ts.time()
    from datetime import time as dtime
    s = dtime(SESSION_START[0], SESSION_START[1])
    e = dtime(SESSION_END[0],   SESSION_END[1])
    return s <= t <= e

# ══════════════════════════════════════════════════════════════════════
#  SIGNAL DETECTION
# ══════════════════════════════════════════════════════════════════════

def detect_signals(df, sym_state):
    df = add_vwap_bands(df)

    active_setups   = sym_state.get('active_setups',   [])
    bull_ic         = sym_state.get('bull_ic_count',   0)
    bear_ic         = sym_state.get('bear_ic_count',   0)
    bull_hi         = sym_state.get('bull_tracked_hi', None)
    bear_lo         = sym_state.get('bear_tracked_lo', None)
    sent_keys       = set(sym_state.get('sent_keys',   []))

    new_signals = []
    n = len(df)
    start_i = max(LEFT_WINDOW + BB_LOOKBACK + 2, n - 60)

    for i in range(start_i, n):
        ts = df.index[i]
        o, h, l, c = df['open'].iloc[i], df['high'].iloc[i], df['low'].iloc[i], df['close'].iloc[i]
        upp, low_ = df['upper'].iloc[i], df['lower'].iloc[i]

        if pd.isna(upp) or pd.isna(low_):
            continue

        is_g, is_r = c > o, c < o

        # Tol
        mintick = c * 0.00005
        tol = (WICK_TOL_TICKS * mintick) if not STRICT_ZERO_WICK else 0.0

        # IC tracking
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

        # Prev color
        pg = df['close'].iloc[i-1] > df['open'].iloc[i-1]
        pr = df['close'].iloc[i-1] < df['open'].iloc[i-1]
        bp = (not REQUIRE_PREV_COL) or pg
        sp = (not REQUIRE_PREV_COL) or pr

        # Left window
        hi_h = df['high'].iloc[max(0,i-LEFT_WINDOW):i].max() if ENABLE_LEFT_FILTER else 0
        lo_l = df['low'].iloc[max(0,i-LEFT_WINDOW):i].min() if ENABLE_LEFT_FILTER else 9e9
        bl = (h > hi_h) if LEFT_CLEARANCE else (l > hi_h)
        sl = (l < lo_l) if LEFT_CLEARANCE else (h < lo_l)
        if not ENABLE_LEFT_FILTER: bl = sl = True

        # Band pos
        if not ENABLE_BB_FILTER:
            bo = so = True
        elif BB_ENTIRE_CANDLE:
            bo, so = l > upp, h < low_
        else:
            bo, so = c > upp, c < low_

        # Opp band lookback
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

        # No-wick
        nlw = is_g and (abs(o - l) <= tol)
        nuw = is_r and (abs(o - h) <= tol)
        cb  = nlw and bp and bl and bbb
        cs  = nuw and sp and sl and sbb

        # Instant
        body   = abs(c - o)
        bw_opp = (h - c) if is_g else 0.0
        sw_opp = (c - l) if is_r else 0.0
        hib = ENABLE_INSTANT and body > 0 and bw_opp >= body * WICK_BODY_RATIO
        his = ENABLE_INSTANT and body > 0 and sw_opp >= body * WICK_BODY_RATIO

        ins = in_session(ts)

        def emit(sig_type, price, level, setup_ts=None):
            key = f"{sig_type}_{ts}"
            if key not in sent_keys:
                new_signals.append({'type': sig_type, 'ts': ts, 'price': price,
                                    'level': level, 'setup_ts': setup_ts})
                sent_keys.add(key)

        if cb and hib and ins: emit('INSTANT_BUY',  c, l)
        if cs and his and ins: emit('INSTANT_SELL', c, h)

        # Register sweeps
        bk = str(ts)
        existing = {s['ts'] for s in active_setups}
        if cb and not hib and bk not in existing:
            active_setups.append({'bar_i': i, 'side': 1,  'level': float(l), 'ts': bk})
        if cs and not his and bk not in existing:
            active_setups.append({'bar_i': i, 'side': -1, 'level': float(h), 'ts': bk})

        # Sweep check
        rem = []
        for s in active_setups:
            el = i - s['bar_i']
            if el < 1: rem.append(s); continue
            if el <= MAX_SWEEP_BARS:
                if s['side'] == 1 and l <= s['level']:
                    if ins: emit('SWEEP_BUY',  c, s['level'], s['ts'])
                elif s['side'] == -1 and h >= s['level']:
                    if ins: emit('SWEEP_SELL', c, s['level'], s['ts'])
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
#  ALERTS — Discord + Gmail (Dono Free)
# ══════════════════════════════════════════════════════════════════════

def send_telegram(msg):
    """Telegram (optional)"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'HTML'},
            timeout=10
        )
        if r.status_code == 200:
            print("    ✅ Telegram: sent")
        else:
            print(f"    ❌ Telegram error: {r.text[:80]}")
    except Exception as e:
        print(f"    ❌ Telegram: {e}")


def send_discord(msg):
    """
    Discord Webhook — 100% Free
    Setup: Discord Server → Settings → Integrations → Webhooks → New Webhook → Copy URL
    GitHub Secret naam: DISCORD_WEBHOOK
    """
    if not DISCORD_WEBHOOK:
        return
    # Discord markdown: <b> → **bold**
    clean = msg.replace('<b>', '**').replace('</b>', '**')
    # Discord embed (styled box)
    embed = {
        "embeds": [{
            "description": clean,
            "color": 0x00E676 if "BUY" in msg else 0xEF4444,
            "footer": {"text": f"VWAP NoWick Bot • {datetime.now(timezone.utc).strftime('%H:%M UTC')}"}
        }]
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=embed, timeout=10)
        if r.status_code in (200, 204):
            print("    ✅ Discord: sent")
        else:
            print(f"    ❌ Discord error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"    ❌ Discord: {e}")


def send_gmail(subject, plain_body):
    """
    Gmail SMTP — 100% Free
    Setup:
      1. Google Account → Security → 2-Step Verification → ON karo
      2. App Passwords → App: Mail → Generate → 16-char code milega
    GitHub Secrets:
      GMAIL_SENDER   = aapka@gmail.com
      GMAIL_APP_PASS = xxxx xxxx xxxx xxxx
      GMAIL_RECEIVER = jahan alert bhejein (same ya alag email)
    """
    if not GMAIL_SENDER or not GMAIL_APP_PASS or not GMAIL_RECEIVER:
        return
    try:
        # HTML email body (mobile pe sundar dikhega)
        is_buy  = "BUY"  in subject
        color   = "#00E676" if is_buy else "#EF4444"
        icon    = "🟢" if is_buy else "🔴"
        html_body = f"""
        <html><body style="font-family:Arial,sans-serif;background:#0d1117;color:#e6edf3;padding:20px;">
          <div style="max-width:400px;margin:auto;background:#161b22;border-radius:12px;
                      border:1px solid {color};padding:20px;">
            <h2 style="color:{color};margin:0 0 12px">{icon} VWAP NoWick Signal</h2>
            <pre style="font-size:14px;line-height:1.7;white-space:pre-wrap;color:#e6edf3;">{plain_body}</pre>
            <hr style="border-color:#30363d;margin:12px 0">
            <small style="color:#8b949e;">VWAP NoWick Alert Bot — Auto Generated</small>
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
        print("    ✅ Gmail: sent")
    except Exception as e:
        print(f"    ❌ Gmail: {e}")


def send_all(symbol, sig):
    """Saare enabled channels pe alert bhejo"""
    html_msg  = fmt(symbol, sig)   # Telegram/Discord HTML format
    plain_msg = fmt_plain(symbol, sig)  # Gmail plain text

    send_telegram(html_msg)
    send_discord(html_msg)
    send_gmail(
        subject   = f"{sig['type']} — {symbol} @ {sig['price']:.5f}",
        plain_body= plain_msg
    )


def fmt(symbol, sig):
    """HTML format (Telegram / Discord)"""
    icons  = {'INSTANT_BUY':'🟢⚡','SWEEP_BUY':'🟢🔄','INSTANT_SELL':'🔴⚡','SWEEP_SELL':'🔴🔄'}
    labels = {
        'INSTANT_BUY':  'INSTANT BUY  — Opposite Wick ≥ Body',
        'SWEEP_BUY':    'SWEEP BUY    — No-Wick Low Swept',
        'INSTANT_SELL': 'INSTANT SELL — Opposite Wick ≥ Body',
        'SWEEP_SELL':   'SWEEP SELL   — No-Wick High Swept',
    }
    return (
        f"{icons.get(sig['type'],'🔔')} <b>VWAP NoWick Signal</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Symbol : <b>{symbol}</b>\n"
        f"📌 Signal : <b>{labels.get(sig['type'], sig['type'])}</b>\n"
        f"💰 Price  : <b>{sig['price']:.5f}</b>\n"
        f"📏 Level  : {sig['level']:.5f}\n"
        f"🕐 Time   : {str(sig['ts'])[:19]} UTC\n"
        f"⏱ TF     : {TIMEFRAME} | {EXCHANGE_ID.upper()}"
    )


def fmt_plain(symbol, sig):
    """Plain text format (Gmail)"""
    labels = {
        'INSTANT_BUY':  'INSTANT BUY  — Opposite Wick >= Body',
        'SWEEP_BUY':    'SWEEP BUY    — No-Wick Low Swept',
        'INSTANT_SELL': 'INSTANT SELL — Opposite Wick >= Body',
        'SWEEP_SELL':   'SWEEP SELL   — No-Wick High Swept',
    }
    return (
        f"Symbol  : {symbol}\n"
        f"Signal  : {labels.get(sig['type'], sig['type'])}\n"
        f"Price   : {sig['price']:.5f}\n"
        f"Level   : {sig['level']:.5f}\n"
        f"Time    : {str(sig['ts'])[:19]} UTC\n"
        f"TF      : {TIMEFRAME} | {EXCHANGE_ID.upper()}"
    )


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Check ke kam az kam ek channel configured ho
    has_telegram = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    has_discord  = bool(DISCORD_WEBHOOK)
    has_gmail    = bool(GMAIL_SENDER and GMAIL_APP_PASS and GMAIL_RECEIVER)

    if not any([has_telegram, has_discord, has_gmail]):
        print("❌ Koi bhi alert channel configured nahi!")
        print("   GitHub Secrets mein DISCORD_WEBHOOK ya GMAIL_SENDER etc. daalo.")
        sys.exit(1)

    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}] Scanning {len(SYMBOLS)} symbols...")
    print(f"  Channels: "
          f"{'✅ Telegram' if has_telegram else '⏭ Telegram'} | "
          f"{'✅ Discord'  if has_discord  else '⏭ Discord'}  | "
          f"{'✅ Gmail'    if has_gmail    else '⏭ Gmail'}")
    print("━" * 48)

    state = load_state()
    total = 0

    for symbol in SYMBOLS:
        key = symbol.replace('/', '_')
        try:
            df = fetch_ohlcv(symbol, limit=300)
            if df is None or len(df) < LEFT_WINDOW + BB_LOOKBACK + 10:
                print(f"  {symbol}: Insufficient data")
                continue

            signals, state[key] = detect_signals(df, state.get(key, {}))

            for sig in signals:
                print(f"\n  📡 {symbol}: {sig['type']} @ {sig['price']:.5f}")
                send_all(symbol, sig)
                total += 1

            if not signals:
                print(f"  ✓  {symbol}: No new signals")

        except Exception as e:
            print(f"  ⚠️  {symbol}: {e}")

    save_state(state)
    print(f"\n{'━'*48}")
    print(f"  Done. {total} signal(s) sent.")

