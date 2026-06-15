"""
도미넌스 플로우 + MEXC 알트 실시간 대시보드 + 텔레그램 봇
보안 강화 버전 (시크릿 키 + 환경변수 + IP 필터)
============================================================
필요 패키지: pip install flask requests flask-cors python-dotenv
실행: python app.py  →  http://localhost:5000
"""

from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
from dotenv import load_dotenv
import requests as req
import websocket          # pip install websocket-client
import json as _json
import queue
import os, time, threading, sqlite3, random
from datetime import datetime
from functools import wraps

# ── .env 파일 로드 ────────────────────────────────────
load_dotenv()

app = Flask(__name__, static_folder=".")
CORS(app)

# ══════════════════════════════════════════════════════
# 설정값 — 모두 .env 에서 읽음 (app.py에 직접 입력 금지)
# ══════════════════════════════════════════════════════
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET   = os.getenv("WEBHOOK_SECRET",   "")
DASH_USER        = os.getenv("DASH_USER", "admin")
DASH_PASS        = os.getenv("DASH_PASS", "")       # 비어있으면 인증 없음

# ── API 주소 ──────────────────────────────────────────
MEXC_FUTURES_URL  = "https://contract.mexc.com/api/v1/contract/ticker"
MEXC_SPOT_URL     = "https://api.mexc.com/api/v3/ticker/price"
COINGECKO_GLOBAL  = "https://api.coingecko.com/api/v3/global"
CMC_GLOBAL_URL    = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"
CMC_QUOTES_URL    = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
CMC_LISTING_URL   = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
TELEGRAM_API      = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
CMC_API_KEY       = os.getenv("CMC_API_KEY", "")

# ── 갱신 주기 ─────────────────────────────────────────
REFRESH_FUTURES = 15   # V1.7.0: WS 정상 시 REST 백업 부담 완화
REFRESH_DOMINANCE = 300   # 5분 (CoinMarketCap 도미넌스 갱신 주기)

# ── 필터 ─────────────────────────────────────────────
TOP_N         = 50    # 최대 50개 (클라이언트에서 10~50 선택)
MIN_VOLUME    = 500_000    # 최소 거래대금 $500,000
MIN_OI        = 300_000
MIN_ABS_PCT   = 1.0

# ── 동적 필터 설정 (MEXC) ────────────────────────
filter_config = {
    "use_major":   True,
    "use_stable":  True,
    "use_stock":   True,
    "use_scam":    True,
    "use_lev":     True,
    "use_vol":     True,
    "use_oi":      True,
    "use_pct":     True,
}


# ── 알트 유입 감지 임계값 ─────────────────────────────
ALT_INFLOW_TH = 0.3
BTC_DROP_TH   = 0.2
USDT_DROP_TH  = 0.1

# ── 텔레그램 쿨다운 ──────────────────────────────────
TELEGRAM_COOLDOWN       = 600
SURGE_COOLDOWN          = 300   # 단기 급등/급락 알림 쿨다운 5분
SURGE_PCT_TH            = 3.0   # 단기 급등/급락 임계값 (%)
SURGE_TOP_N             = 30    # 단기 급등/급락 표시 개수 (29-4I-4B-4)
PRICE_HIST_MINUTES      = 60    # 가격 히스토리 보관 (분)

# ══════════════════════════════════════════════════════
# 보안 ① — TradingView 공식 Webhook 발신 IP 허용 목록
# https://www.tradingview.com/support/solutions/43000529348
# ══════════════════════════════════════════════════════
TV_ALLOWED_IPS = {
    "52.89.214.238",
    "34.212.75.30",
    "54.218.53.128",
    "52.32.178.7",
    "127.0.0.1",       # 로컬 테스트용
    "::1",             # IPv6 로컬
}

# ── 요청 횟수 제한 (간단한 Rate Limit) ───────────────
request_counts = {}
RATE_LIMIT = 30   # 분당 최대 요청 수

def rate_limit_check(ip):
    now    = time.time()
    minute = int(now // 60)
    key    = f"{ip}:{minute}"
    request_counts[key] = request_counts.get(key, 0) + 1
    # 오래된 키 정리
    old_keys = [k for k in request_counts if int(k.split(":")[1]) < minute - 2]
    for k in old_keys:
        del request_counts[k]
    return request_counts[key] <= RATE_LIMIT

# ── 제외 목록 ─────────────────────────────────────────
EXCLUDE_MAJOR = {
    "BTC","ETH","WBTC","WETH",
    "USDT","USDC","BUSD","DAI","TUSD","FDUSD","USDE","PYUSD","FRAX",
    "CRVUSD","SUSD","LUSD","GUSD","USDP","PAXG","XAUT",
}

# 스테이블코인 별도 필터(use_stable) — 현물/스테이블 오출력 방지
EXCLUDE_STABLE = {
    "USDT","USDC","BUSD","DAI","TUSD","FDUSD","USDE","PYUSD","FRAX",
    "CRVUSD","SUSD","LUSD","GUSD","USDP","USDD","CUSD","USDJ","HUSD",
}
EXCLUDE_STOCK = {
    # ── 미국 주식 ──────────────────────────────────
    "TSLA","NVDA","AAPL","AMZN","GOOGL","META","MSFT","AMD","INTC","MU",
    "COIN","HOOD","GME","AMC","PLTR","MSTR","MCD","NFLX","BABA",
    "TSLAON","NVDAON","MSTRON","CRCLON","AAPLON",
    # 풀네임 형태
    "TESLA","NVIDIA","ROBINHOOD","COINBASE","AMAZON",
    # ── 지수 ────────────────────────────────────────
    "SPX","SPX500","NAS100","NDX","US30","US500","US100","VIX",
    "HK50","UK100","DE40","JP225","AU200","FR40",
    # ── 원자재 ──────────────────────────────────────
    "GOLD","SILVER","XAU","XAG","OIL","WTI","BRENT","CRUDE",
    "GAS","NATGAS","NGAS","UKOIL","USOIL",
    "COPPER","PALLADIUM","PLATINUM","XPT","XPD",
    # ── 금 연동 토큰 ─────────────────────────────────
    "XAUT","PAXG","CACHE","LBMA",
    # ── 외환 ────────────────────────────────────────
    "EUR","GBP","JPY","AUD","CAD","CHF","DXY",
}
EXCLUDE_SCAM = {
    "LUNA","LUNC","UST","USTC","SQUID","TITAN","SAFEMOON",
    "ELON","KISHU","AKITA","SAITAMA","HOGE","LADYS","TURBO","PEOPLE",
}
EXCLUDE_PAT = ["3L","3S","2L","2S","5L","5S","10L","10S","BULL","BEAR","UP","DOWN",
              "STOCK","XSTOCK"]   # 주식 토큰 접미사

# ── 캐시 ──────────────────────────────────────────────
cache = {
    # ── MEXC ─────────────────────────────────────────
    "gainers":[], "losers":[], "top_funding":[],
    "surge_up":[], "surge_dn":[],
    "gainers_major":[], "losers_major":[],
    "all_coins":[],
    "futures_total":0, "futures_updated":None,
    "oi_surge":    [],   # OI 급증 TOP

    "dominance":{
        "btc":0,"eth":0,"usdt":0,"usdc":0,"alts":0,
        "btc_chg":0,"eth_chg":0,"usdt_chg":0,"alts_chg":0,
        "total_mcap":0,"total_chg_pct":0,
    },
    "dom_updated":None,
    "alt_signal":{
        "active":False,"level":"없음","score":0,
        "reason":"","triggered_at":None,
    },
    "tv_signals":[],
    "security_log":[],   # 보안 이벤트 로그
    "error":None,
}
prev_dom        = None   # 직전 수집값 (변화량 계산용)
price_history     = {}   # MEXC 5분 단기 감지용
last_surge_sent   = {"time": 0}
last_oi_sent      = {}   # {코인명: 마지막 OI 알림 시각}
OI_VOL_COOLDOWN   = 300  # OI/볼륨 알림 쿨다운 5분
dom_history     = []     # 최근 30분치 저장
DOM_HIST_MAX    = 6      # 5분 × 6 = 30분치 보관
last_tg_sent    = {"level":"없음","time":0}
rank_history    = {}     # {prefix+코인명: 이전순위} — 순위 변동 계산용
oi_history      = {}     # {코인명: [OI값, ...]} OI 변화량 계산용
OI_HIST_MAX     = 6      # 최대 6개 (10초×6=1분) OI 변화량 기준
spot_cache      = {}     # {코인명: 현물가격} — MEXC 현물 가격 캐시

# MEXC contract/detail API 기준 실제 활성 선물 상장 심볼 셋
# 이 셋에 없는 심볼은 현물/비활성/비정규 심볼로 보고 리스트에서 제외
mexc_futures_detail_set = set()

# ── SSE / 웹소켓 실시간 출력 상태 ─────────────────────
sse_clients    = []
sse_lock       = threading.Lock()
ws_ticker_data = {}
ws_connected   = False
ws_last_recv   = 0
WS_URL         = "wss://contract.mexc.com/edge"
WS_RECONNECT   = 5
ws_app_ref     = None
# 29-4I-4B-8: 실시간 처리 최적화
# - 웹소켓 메시지마다 스레드를 만들지 않고 dirty 플래그만 세움
# - 워커가 0.25초 단위로 묶어서 처리해 CPU/브라우저 렉을 줄임
ws_dirty       = threading.Event()
ws_data_lock   = threading.Lock()
WS_RENDER_MIN_INTERVAL = 0.15  # V1.7.0: 소켓 체감 반응속도 향상
# 29-4I-4B-8-3: 브라우저로 보내는 대형 all_coins payload는 2초 단위로만 포함
WS_FULL_PAYLOAD_INTERVAL = 1.2  # V1.7.0: 전체 코인 동기화 주기 단축
ws_last_full_payload_at = 0
ws_last_log_at = 0


# ════════════════════════════════════════════════════════
# 보안 — 대시보드 기본 인증 (ID/비밀번호)
# ════════════════════════════════════════════════════════
from flask import Response

def require_login(f):
    """대시보드 페이지 기본 인증 — DASH_PASS 설정 시 활성화"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not DASH_PASS:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != DASH_USER or auth.password != DASH_PASS:
            return Response(
                "로그인이 필요합니다.",
                401,
                {"WWW-Authenticate": 'Basic realm="MEXC Dashboard"'}
            )
        return f(*args, **kwargs)
    return decorated


# ════════════════════════════════════════════════════════
# 보안 ② — 시크릿 키 검증 데코레이터
# ════════════════════════════════════════════════════════
def require_secret(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 시크릿 키가 .env에 설정된 경우에만 검증
        if WEBHOOK_SECRET:
            token = request.args.get("token", "") or \
                    request.headers.get("X-Webhook-Token", "")
            if token != WEBHOOK_SECRET:
                ip = request.headers.get("X-Forwarded-For",
                                         request.remote_addr)
                log_security(f"시크릿 키 불일치 — IP: {ip}")
                return jsonify({"status": "unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# 보안 ③ — TradingView IP 검증 데코레이터
def require_tv_ip(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # X-Forwarded-For: ngrok 등 프록시 통과 시 실제 IP
        forwarded = request.headers.get("X-Forwarded-For", "")
        remote    = request.remote_addr
        # 콤마로 구분된 경우 첫 번째 IP가 실제 클라이언트
        client_ip = forwarded.split(",")[0].strip() if forwarded else remote

        if client_ip not in TV_ALLOWED_IPS:
            log_security(f"허용되지 않은 IP 차단 — {client_ip}")
            return jsonify({"status": "forbidden"}), 403

        # Rate Limit 체크
        if not rate_limit_check(client_ip):
            log_security(f"Rate Limit 초과 — {client_ip}")
            return jsonify({"status": "rate_limited"}), 429

        return f(*args, **kwargs)
    return decorated


def log_security(msg):
    """보안 이벤트 기록"""
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(f"[보안] {entry}")
    cache["security_log"].append(entry)
    if len(cache["security_log"]) > 50:
        cache["security_log"] = cache["security_log"][-50:]


# ════════════════════════════════════════════════════════
# 텔레그램 전송
# ════════════════════════════════════════════════════════
def tg_send(text):
    if not TELEGRAM_TOKEN:
        print("[텔레그램] .env 에 TELEGRAM_TOKEN 미설정")
        return
    try:
        resp = req.post(
            f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=8
        )
        if resp.status_code == 200:
            print("[텔레그램] 전송 완료")
        else:
            print(f"[텔레그램] 오류: {resp.text[:200]}")
    except Exception as e:
        print(f"[텔레그램] 예외: {e}")


def build_tg_message(sig, gainers, losers):
    level = sig.get("level","없음")
    score = sig.get("score", 0)
    now   = datetime.now().strftime("%H:%M:%S")
    icon  = {"HIGH CONF":"🔥","강":"🚀","중":"📈","약":"🔵"}.get(level,"⚪")

    lines = [
        f"{icon} <b>알트 자금 유입 신호 [{level}]</b>",
        f"🕐 {now}  |  점수: {score}/9",
        f"📊 {sig.get('reason','')}",
        "",
    ]

    # 급반등 리스트
    if gainers:
        lines.append("🚀 <b>급반등 알트 TOP 5</b>")
        for i, c in enumerate(gainers[:5], 1):
            name = c['name']
            pct  = f"{c['pct']:+.2f}%"
            vol  = f"${c['volume']/1e6:.1f}M"
            lines.append(f"  {i}. <code>{name}</code>  {pct}  {vol}")
    else:
        lines.append("🚀 <b>급반등 알트</b> — 변동률 ±1% 이상 코인 없음")

    lines.append("")

    # 급하락 리스트
    if losers:
        lines.append("💥 <b>급하락 알트 TOP 5</b>")
        for i, c in enumerate(losers[:5], 1):
            name = c['name']
            pct  = f"{c['pct']:+.2f}%"
            vol  = f"${c['volume']/1e6:.1f}M"
            lines.append(f"  {i}. <code>{name}</code>  {pct}  {vol}")
    else:
        lines.append("💥 <b>급하락 알트</b> — 변동률 ±1% 이상 코인 없음")

    lines.append("")
    lines.append(f"📊 급반등 {len(gainers)}개 / 급하락 {len(losers)}개")
    lines.append("🌐 http://localhost:5000")
    return "\n".join(lines)


def maybe_send_telegram(sig):
    level = sig.get("level","없음")
    if level == "없음": return
    now   = time.time()
    level_order = {"약":1,"중":2,"강":3,"HIGH CONF":4}
    prev_ord = level_order.get(last_tg_sent["level"], 0)
    curr_ord = level_order.get(level, 0)
    if curr_ord > 0 and (curr_ord > prev_ord or
                         now - last_tg_sent["time"] >= TELEGRAM_COOLDOWN):
        last_tg_sent["level"] = level
        last_tg_sent["time"]  = now
        # 선물 데이터가 비어있으면 먼저 갱신 후 전송
        def send_with_fresh_data():
            if not cache["gainers"] and not cache["losers"]:
                print("[텔레그램] 선물 데이터 없음 — 먼저 갱신 후 전송")
                fetch_futures()
            msg = build_tg_message(sig, cache["gainers"], cache["losers"])
            tg_send(msg)
            # 전송 후 리스트가 비어있었는지 로그
            if not cache["gainers"]:
                print("[텔레그램] ⚠ 급반등 리스트 비어있음 — 시장 변동률 ±1% 미만")
        threading.Thread(target=send_with_fresh_data, daemon=True).start()


# ════════════════════════════════════════════════════════
# MEXC 선물 수집
# ════════════════════════════════════════════════════════
def is_pure_alt(name, vol, oi):
    if filter_config["use_major"] and name in EXCLUDE_MAJOR: return False
    if filter_config["use_stock"] and name in EXCLUDE_STOCK: return False
    if filter_config["use_scam"]  and name in EXCLUDE_SCAM:  return False
    if filter_config["use_lev"]:
        for p in EXCLUDE_PAT:
            if name.endswith(p) or name.startswith(p): return False
        if name.endswith("ON") and len(name) >= 5: return False
    if filter_config["use_vol"] and vol < MIN_VOLUME: return False
    if filter_config["use_oi"]  and oi  < MIN_OI:     return False
    return True


def fetch_futures():
    global rank_history
    try:
        r = req.get(MEXC_FUTURES_URL, timeout=10)
        r.raise_for_status()
        # ① JSON 한 번만 파싱해서 재사용
        raw_data = r.json().get("data", [])

        coins      = []   # 순수 알트 (스캠·증시·메이저 제외)
        major_pool = []   # 메이저 포함 (스캠·증시만 제외)
        raw_pool   = []   # 필터 없는 전체 (찜 코인용)

        for c in raw_data:
            sym = c.get("symbol","")
            if not sym.endswith("_USDT"): continue
            name = sym.replace("_USDT","")

            # 공통 제외 (스캠·레버리지·증시)
            if name in EXCLUDE_STOCK: continue
            if name in EXCLUDE_SCAM:  continue
            skip = any(name.endswith(p) or name.startswith(p) for p in EXCLUDE_PAT)
            if skip: continue
            if name.endswith("ON") and len(name) >= 5: continue

            # 개발자 수정 반영: MEXC 활성 선물 contract/detail에 있는 심볼만 허용
            # 현물만 있는 코인이 리스트에 섞여 나오는 현상 방지
            if mexc_futures_detail_set and name.upper() not in mexc_futures_detail_set:
                continue

            try:
                price   = float(c.get("lastPrice",   0) or 0)
                pct     = float(c.get("riseFallRate", 0) or 0) * 100
                volume  = float(c.get("amount24",     0) or 0)
                funding = float(c.get("fundingRate",  0) or 0) * 100
                oi      = float(c.get("holdVol",      0) or 0)
                fair    = float(c.get("fairPrice",    0) or 0)
                high24  = float(c.get("high24Price",  0) or 0)
                low24   = float(c.get("lower24Price", 0) or 0)
            except: continue

            if price <= 0: continue

            # 심볼 정규화로 현물 가격 조회 (TONCOIN→TON)
            spot_name = normalize_symbol(name)
            spot_p    = spot_cache.get(spot_name, spot_cache.get(name, 0))
            coin = dict(symbol=sym, name=name, price=price,
                        pct=round(pct,4), volume=volume,
                        funding=round(funding,6), oi=oi,
                        fair=fair, high24=high24, low24=low24,
                        spot=round(spot_p, 8))

            # 메이저 포함 풀 — 거래대금·OI 필터만 적용
            if volume >= MIN_VOLUME and oi >= MIN_OI:
                major_pool.append(coin)
            raw_pool.append(coin)

            # 순수 알트 풀 — 메이저 추가 제외
            if name not in EXCLUDE_MAJOR and volume >= MIN_VOLUME and oi >= MIN_OI:
                coins.append(coin)

        # ── 순수 알트 정렬 ──────────────────────────────
        g_sorted = sorted([c for c in coins if c["pct"] >= MIN_ABS_PCT],
                          key=lambda x: x["pct"], reverse=True)[:TOP_N]
        l_sorted = sorted([c for c in coins if c["pct"] <= -MIN_ABS_PCT],
                          key=lambda x: x["pct"])[:TOP_N]

        # ── 메이저 포함 정렬 ────────────────────────────
        gm_sorted = sorted(major_pool, key=lambda x: x["pct"], reverse=True)[:TOP_N]
        lm_sorted = sorted(major_pool, key=lambda x: x["pct"])[:TOP_N]

        # ── 순위 변동 계산 ───────────────────────────────
        def add_rank_change(lst, prefix):
            result = []
            for i, c in enumerate(lst):
                key      = prefix + c["name"]
                prev_rk  = rank_history.get(key)
                curr_rk  = i + 1
                if prev_rk is None:
                    chg = None          # 첫 등장
                else:
                    chg = prev_rk - curr_rk  # 양수=상승, 음수=하락
                rank_history[key] = curr_rk
                result.append({**c, "rank_chg": chg})
            return result

        # 웹소켓 연결 시 급등/급락 랭킹은 실시간 수신 데이터가 관리
        # REST는 백업/초기값 및 펀딩·전체코인 캐시를 담당
        if not ws_connected:
            cache["gainers"]       = add_rank_change(g_sorted,  "g_")
            cache["losers"]        = add_rank_change(l_sorted,  "l_")
            cache["gainers_major"] = add_rank_change(gm_sorted, "gm_")
            cache["losers_major"]  = add_rank_change(lm_sorted, "lm_")
        else:
            cache["all_coins"] = [c for c in raw_pool]
        cache["top_funding"]   = sorted(coins, key=lambda x: abs(x["funding"]),
                                        reverse=True)[:5]
        # 전체 코인 저장 (찜 코인 값 조회용) — major_pool 사용
        cache["all_coins"]     = raw_pool   # 필터 없는 전체 (찜 코인용)

        cache["futures_total"] = len(coins)
        cache["futures_updated"] = time.strftime("%H:%M:%S")

        # ── 5분 단기 급등/급락 감지 ──────────────────────
        detect_surge(coins)

        # ── OI 급증 + 거래대금 비율 분석 ─────────────────
        detect_oi_vol(coins)

        # ── 코인별 개별 알람 체크 ────────────────────────
        if coin_alarm_config:
            check_coin_alarms_server()

        print(f"[{cache['futures_updated']}] 선물 {len(coins)}개 | "
              f"급반등 {len(cache['gainers'])}개 / 급하락 {len(cache['losers'])}개 | "
              f"단기급등 {len(cache['surge_up'])}개 / 단기급락 {len(cache['surge_dn'])}개")
    except Exception as e:
        cache["error"] = f"선물: {e}"
        print(f"[선물 오류] {e}")


def detect_surge(coins):
    """5분 가격 히스토리 기반 단기 급등/급락 감지"""
    global price_history, last_surge_sent
    now = time.time()

    surge_up = []
    surge_dn = []

    for c in coins:
        name  = c["name"]
        price = c["price"]

        # 히스토리 초기화
        if name not in price_history:
            price_history[name] = []

        # 현재가 추가
        price_history[name].append((now, price))

        # 오래된 데이터 제거 (PRICE_HIST_MINUTES 분 이상)
        cutoff = now - PRICE_HIST_MINUTES * 60
        price_history[name] = [
            (t, p) for t, p in price_history[name] if t >= cutoff
        ]

        # 5분 전 가격 찾기
        target_time = now - 300   # 5분 전
        old_entries = [
            (t, p) for t, p in price_history[name]
            if t <= target_time + 30   # ±30초 허용
        ]

        if not old_entries:
            continue

        # 가장 최근의 5분 전 가격 사용
        old_price = old_entries[-1][1]
        if old_price <= 0:
            continue

        pct_5m = (price - old_price) / old_price * 100

        if pct_5m >= SURGE_PCT_TH:
            surge_up.append({**c, "pct_5m": round(pct_5m, 3)})
        elif pct_5m <= -SURGE_PCT_TH:
            surge_dn.append({**c, "pct_5m": round(pct_5m, 3)})

    # 변동률 기준 정렬
    cache["surge_up"] = sorted(surge_up, key=lambda x: x["pct_5m"], reverse=True)[:SURGE_TOP_N]
    cache["surge_dn"] = sorted(surge_dn, key=lambda x: x["pct_5m"])[:SURGE_TOP_N]

    # 단기 급등 텔레그램 알림
    if cache["surge_up"] or cache["surge_dn"]:
        if now - last_surge_sent["time"] >= SURGE_COOLDOWN:
            last_surge_sent["time"] = now
            msg = build_surge_message(cache["surge_up"], cache["surge_dn"])
            threading.Thread(target=tg_send, args=(msg,), daemon=True).start()


def detect_oi_vol(coins):
    """OI 변화량 + 평균 볼륨 대비 현재 볼륨 비율 계산"""
    global oi_history, last_oi_sent

    oi_surge_list  = []

    for c in coins:
        name = c["name"]
        oi   = c["oi"]
        vol  = c["volume"]

        # ── OI 히스토리 ──────────────────────────────
        if name not in oi_history:
            oi_history[name] = []
        oi_history[name].append(oi)
        if len(oi_history[name]) > OI_HIST_MAX:
            oi_history[name] = oi_history[name][-OI_HIST_MAX:]

        # OI 변화량 = 최신 - 가장 오래된 값
        if len(oi_history[name]) >= 2:   # 최소 2개 (20초) 이상이면 계산
            old_oi = oi_history[name][0]
            if old_oi > 0:
                oi_chg_pct = (oi - old_oi) / old_oi * 100
                if oi_chg_pct >= 10.0:  # OI 10% 이상 증가 (1분 기준)
                    oi_surge_list.append({
                        **c,
                        "oi_chg_pct": round(oi_chg_pct, 2),
                        "oi_chg_abs": round(oi - old_oi, 0),
                    })



    # 정렬 후 저장
    cache["oi_surge"]  = sorted(oi_surge_list,
                                key=lambda x: x["oi_chg_pct"], reverse=True)[:10]

    # ── 텔레그램 알림 ────────────────────────────────
    now = time.time()

    # OI 급증 알림
    new_oi = [c for c in cache["oi_surge"]
              if now - last_oi_sent.get(c["name"], 0) > OI_VOL_COOLDOWN]
    if new_oi:
        for c in new_oi:
            last_oi_sent[c["name"]] = now
        lines = [f"📊 <b>OI 급증 감지</b> ({len(new_oi)}개)",
                 f"🕐 {time.strftime('%H:%M:%S')}", ""]
        for c in new_oi[:5]:
            lines.append(f"  <code>{c['name']}</code>  OI+{c['oi_chg_pct']:.2f}%  {c['pct']:+.2f}%  ${c['volume']/1e6:.1f}M")
        msg = "\n".join(lines)
        threading.Thread(target=tg_send, args=(msg,), daemon=True).start()




def build_surge_message(surge_up, surge_dn):
    """단기 급등/급락 텔레그램 메시지 생성"""
    now   = datetime.now().strftime("%H:%M:%S")
    lines = [
        f"⚡ <b>단기 급등/급락 감지 (5분 기준)</b>",
        f"🕐 {now}",
        "",
    ]

    if surge_up:
        lines.append(f"🚀 <b>단기 급등 TOP {min(len(surge_up), 5)}</b>")
        for i, c in enumerate(surge_up[:5], 1):
            lines.append(
                f"  {i}. <code>{c['name']}</code>  "
                f"{c['pct_5m']:+.2f}%(5분)  "
                f"{c['pct']:+.2f}%(24H)  "
                f"${c['volume']/1e6:.1f}M"
            )

    if surge_up and surge_dn:
        lines.append("")

    if surge_dn:
        lines.append(f"💥 <b>단기 급락 TOP {min(len(surge_dn), 5)}</b>")
        for i, c in enumerate(surge_dn[:5], 1):
            lines.append(
                f"  {i}. <code>{c['name']}</code>  "
                f"{c['pct_5m']:+.2f}%(5분)  "
                f"{c['pct']:+.2f}%(24H)  "
                f"${c['volume']/1e6:.1f}M"
            )

    lines.append("")
    lines.append(f"📊 단기급등 {len(surge_up)}개 / 단기급락 {len(surge_dn)}개")
    lines.append("🌐 http://localhost:5000")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════
# OKX 선물 데이터 수집
# ════════════════════════════════════════════════════════

def fetch_spot_prices():
    """MEXC 현물 전체 가격 수집 + 선물-현물 심볼 불일치 탐지"""
    global spot_cache
    try:
        r = req.get(MEXC_SPOT_URL, timeout=10)
        if r.status_code == 200:
            new_cache = {}
            for item in r.json():
                sym = item.get("symbol","")
                if sym.endswith("USDT"):
                    name = sym.replace("USDT","")
                    new_cache[name] = float(item.get("price", 0) or 0)
            spot_cache = new_cache

            # 선물 코인 중 현물에 없는 코인 탐지
            futures_names = set()
            for c in cache.get("all_coins", []):
                futures_names.add(c.get("name",""))

            missing = []
            for name in futures_names:
                spot_name = normalize_symbol(name)
                if spot_name not in new_cache and name not in new_cache:
                    missing.append(name)

            if missing:
                print(f"[현물 미매핑] {len(missing)}개: {', '.join(sorted(missing)[:20])}")

            print(f"[현물] MEXC 현물 {len(spot_cache)}개 로드")
    except Exception as e:
        print(f"[현물] 오류: {e}")


def bg_spot():
    """현물 가격 백그라운드 갱신 (10초)"""
    while True:
        time.sleep(10)
        fetch_spot_prices()


# ══════════════════════════════════════════════════
# MEXC 선물 웹소켓 + SSE 실시간 출력
# ══════════════════════════════════════════════════

def sse_push(data):
    """모든 브라우저 SSE 클라이언트에 실시간 데이터 푸시
    29-4I-4B-8-4: 클라이언트별 최신 프레임 1개만 유지.
    브라우저가 느려도 오래된 프레임을 쌓지 않아 화면 지연을 막는다.
    """
    with sse_lock:
        if not sse_clients:
            return
        clients = list(sse_clients)
    msg = "data: " + _json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n\n"
    dead = []
    for q in clients:
        try:
            while q.full():
                try: q.get_nowait()
                except Exception: break
            q.put_nowait(msg)
        except Exception:
            dead.append(q)
    if dead:
        with sse_lock:
            for q in dead:
                if q in sse_clients:
                    sse_clients.remove(q)


def ws_on_message(ws_app, message):
    """MEXC 웹소켓 티커 메시지 수신"""
    global ws_ticker_data, ws_last_recv
    try:
        data = _json.loads(message)
        channel = data.get("channel", "")
        if data.get("msg") == "PONG" or channel == "pong":
            return
        if channel == "push.ticker":
            d = data.get("data", {})
            sym = d.get("symbol", "")
            if sym:
                with ws_data_lock:
                    ws_ticker_data[sym] = d
                ws_last_recv = time.time()
                ws_dirty.set()
        elif channel == "push.tickers":
            items = data.get("data", [])
            if isinstance(items, list) and items:
                with ws_data_lock:
                    for d in items:
                        sym = d.get("symbol", "")
                        if sym:
                            ws_ticker_data[sym] = d
                    total = len(ws_ticker_data)
                ws_last_recv = time.time()
                ws_dirty.set()
                # 너무 잦은 콘솔 출력도 렉 원인이어서 최소화
        elif channel in ("rs.sub.tickers", "rs.sub.ticker"):
            print(f"[WS] 구독 확인: {data}")
    except Exception as e:
        print(f"[WS] 메시지 처리 오류: {e}")


def ws_on_open(ws_app):
    global ws_connected
    ws_connected = True
    print("[WS] MEXC 선물 웹소켓 연결됨")
    threading.Thread(target=ws_ping_loop, args=(ws_app,), daemon=True).start()
    ws_app.send(_json.dumps({"method":"sub.tickers", "param":{}}))
    print("[WS] 전체 티커 구독 요청 완료")


def ws_on_error(ws_app, error):
    global ws_connected
    ws_connected = False
    print(f"[WS] 오류: {error}")


def ws_on_close(ws_app, close_status_code, close_msg):
    global ws_connected
    ws_connected = False
    print(f"[WS] 연결 종료 (코드={close_status_code})")


def ws_ping_loop(ws_app):
    while ws_connected:
        try:
            ws_app.send(_json.dumps({"method":"ping"}))
        except Exception:
            break
        time.sleep(20)


def bg_websocket():
    """웹소켓 백그라운드 자동 재연결"""
    global ws_connected, ws_app_ref
    while True:
        try:
            print("[WS] MEXC 선물 웹소켓 연결 시도...")
            ws_app = websocket.WebSocketApp(
                WS_URL,
                on_open=ws_on_open,
                on_message=ws_on_message,
                on_error=ws_on_error,
                on_close=ws_on_close,
            )
            ws_app_ref = ws_app
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[WS] 재연결 오류: {e}")
        ws_connected = False
        print(f"[WS] {WS_RECONNECT}초 후 재연결...")
        time.sleep(WS_RECONNECT)


_ws_processing = False

def process_ws_tickers():
    """웹소켓 티커를 기존 대시보드 캐시 구조로 변환"""
    global _ws_processing
    if _ws_processing:
        return
    _ws_processing = True
    try:
        _process_ws_tickers_inner()
    finally:
        _ws_processing = False


def _process_ws_tickers_inner():
    global ws_last_full_payload_at, ws_last_log_at
    with ws_data_lock:
        if not ws_ticker_data:
            return
        raw_data = list(ws_ticker_data.values())
    rest_pct_map = {c.get("symbol"): c.get("pct", 0) for c in cache.get("all_coins", []) if c.get("symbol")}
    try:
        coins, major_pool, raw_pool = [], [], []
        for c in raw_data:
            sym = c.get("symbol", "")
            if not sym.endswith("_USDT"):
                continue
            name = sym.replace("_USDT", "")
            alias = SYMBOL_ALIAS.get(name, name)
            if filter_config["use_stock"] and name in EXCLUDE_STOCK: continue
            if filter_config["use_scam"] and name in EXCLUDE_SCAM: continue
            if filter_config["use_lev"] and any(name.endswith(p) or name.startswith(p) for p in EXCLUDE_PAT): continue

            # 개발자 수정 반영: 실제 활성 MEXC 선물 심볼만 표시
            if mexc_futures_detail_set and name.upper() not in mexc_futures_detail_set:
                continue

            try:
                price = float(c.get("lastPrice", 0) or 0)
                fall = float(c.get("riseFallRate", 0) or 0)
                volume = float(c.get("amount24", 0) or 0)
                if volume == 0:
                    volume = float(c.get("volume24", 0) or 0) * price
                funding = float(c.get("fundingRate", 0) or 0) * 100
                oi = float(c.get("holdVol", 0) or 0)
                if oi == 0:
                    oi = volume
                high24 = float(c.get("high24Price", 0) or 0)
                low24 = float(c.get("lower24Price", 0) or c.get("low24Price", 0) or 0)
                fair = float(c.get("fairPrice", 0) or price)
            except Exception:
                continue
            if price <= 0:
                continue
            # 거래대금 0인 좀비/비정상 심볼 차단
            if volume <= 0:
                continue
            ws_pct = round(fall * 100, 4)
            pct = rest_pct_map.get(sym, ws_pct)
            spot_name = normalize_symbol(name)
            spot_p = spot_cache.get(spot_name, spot_cache.get(name, 0))
            coin = dict(symbol=sym, name=alias, price=price, pct=pct, volume=volume, funding=round(funding, 6), oi=oi, fair=fair, high24=high24, low24=low24, spot=round(spot_p, 8))
            raw_pool.append(coin)
            if filter_config["use_stable"] and name in EXCLUDE_STABLE: continue
            # 메이저 포함 풀도 최소 거래대금 이상만 허용
            if volume < MIN_VOLUME: continue
            major_pool.append(coin)
            if filter_config["use_major"] and alias in EXCLUDE_MAJOR: continue
            if filter_config["use_vol"] and volume < MIN_VOLUME: continue
            if filter_config["use_oi"] and oi < MIN_OI: continue
            if filter_config["use_pct"] and abs(pct) < MIN_ABS_PCT: continue
            coins.append(coin)
        if not coins:
            return
        g = sorted([c for c in coins if c["pct"] > 0], key=lambda x: x["pct"], reverse=True)[:TOP_N]
        l = sorted([c for c in coins if c["pct"] < 0], key=lambda x: x["pct"])[:TOP_N]
        gm = sorted(major_pool, key=lambda x: x["pct"], reverse=True)[:TOP_N]
        lm = sorted(major_pool, key=lambda x: x["pct"])[:TOP_N]
        def add_rank(lst, pfx):
            out=[]
            for i,c in enumerate(lst):
                key=pfx+c["name"]; prev=rank_history.get(key)
                out.append({**c, "rank_chg": (prev-(i+1)) if prev else 0, "_prev_rank": prev})
            return out
        for i,c in enumerate(g): rank_history["g_"+c["name"]] = i+1
        for i,c in enumerate(l): rank_history["l_"+c["name"]] = i+1
        now_str = datetime.now().strftime("%H:%M:%S")
        cache.update({
            "gainers": add_rank(g, "g_"),
            "losers": add_rank(l, "l_"),
            "gainers_major": add_rank(gm, "gm_"),
            "losers_major": add_rank(lm, "lm_"),
            "all_coins": raw_pool,
            "futures_total": len(coins),
            "futures_updated": now_str,
        })
        now_ts = time.time()
        include_full = (now_ts - ws_last_full_payload_at) >= WS_FULL_PAYLOAD_INTERVAL
        payload = {
            "gainers": cache.get("gainers", []),
            "losers": cache.get("losers", []),
            "gainers_major": cache.get("gainers_major", []),
            "losers_major": cache.get("losers_major", []),
            "surge_up": cache.get("surge_up", []),
            "surge_dn": cache.get("surge_dn", []),
            "oi_surge": cache.get("oi_surge", []),
            "futures_total": cache.get("futures_total", 0),
            "futures_updated": now_str,
            "ws_connected": True,
            "payload_mode": "full" if include_full else "light",
        }
        if include_full:
            payload["all_coins"] = cache.get("all_coins", [])
            ws_last_full_payload_at = now_ts
        sse_push(payload)
        if now_ts - ws_last_log_at >= 5:
            print(f"[WS/SSE V3] {now_str} 실시간 출력 | 급등 {len(g)} / 급락 {len(l)} | {payload['payload_mode']}")
            ws_last_log_at = now_ts
    except Exception as e:
        print(f"[WS] 처리 오류: {e}")
        import traceback; traceback.print_exc()


def bg_ws_processor():
    """29-4I-4B-8 고속 워커
    - WS 메시지를 0.25초 단위로 병합 처리
    - WS가 살아있으면 REST 호출을 줄이고, 끊겼을 때만 백업 호출
    - 단기급등/OI 감지는 기존 10초 단위 유지
    """
    last_surge_oi = 0
    last_rest_backup = 0
    last_render = 0
    while True:
        now = time.time()

        if ws_dirty.is_set() and (now - last_render) >= WS_RENDER_MIN_INTERVAL:
            ws_dirty.clear()
            process_ws_tickers()
            last_render = time.time()

        if (not ws_connected) or (time.time() - ws_last_recv > 20):
            if time.time() - last_rest_backup >= REFRESH_FUTURES:
                fetch_futures()
                last_rest_backup = time.time()

        if time.time() - last_surge_oi >= REFRESH_FUTURES:
            coins = [c for c in cache.get("all_coins", []) if c.get("price", 0) > 0]
            if coins:
                detect_surge(coins)
                detect_oi_vol(coins)
                if coin_alarm_config:
                    check_coin_alarms_server()
            last_surge_oi = time.time()

        time.sleep(0.05)


# ════════════════════════════════════════════════════════
# 도미넌스 수집 + 신호 판단
# CoinMarketCap 전용 엔진
# ════════════════════════════════════════════════════════
def _save_dom(btc, eth, usdt, usdc, alts, total_mcap, total_chg, source):
    """파싱된 도미넌스 저장 + 신호 판단
    - dom_history 에 수집값 누적
    - 변화량 = 현재값 - 30분 전 값 (없으면 직전값)
    """
    global prev_dom, dom_history

    # 히스토리에 현재값 추가
    dom_history.append({
        "btc": btc, "eth": eth, "usdt": usdt, "alts": alts,
        "ts": time.time()
    })
    # 최대 보관 개수 유지
    if len(dom_history) > DOM_HIST_MAX:
        dom_history = dom_history[-DOM_HIST_MAX:]

    # 비교 기준: 30분 전 값 (히스토리가 충분하면) or 직전값 or None
    if len(dom_history) >= 2:
        ref = dom_history[0]   # 가장 오래된 값 (최대 30분 전)
        btc_chg  = round(btc  - ref["btc"],  4)
        eth_chg  = round(eth  - ref["eth"],  4)
        usdt_chg = round(usdt - ref["usdt"], 4)
        alts_chg = round(alts - ref["alts"], 4)
        elapsed  = int((time.time() - ref["ts"]) / 60)
        period   = f"{elapsed}분전대비"
    else:
        btc_chg = eth_chg = usdt_chg = alts_chg = 0.0
        period  = "기준수집중"

    cache["dominance"] = dict(
        btc=btc, eth=eth, usdt=usdt, usdc=usdc, alts=alts,
        btc_chg=btc_chg, eth_chg=eth_chg,
        usdt_chg=usdt_chg, alts_chg=alts_chg,
        total_mcap=total_mcap, total_chg_pct=round(total_chg, 2),
        source=source, period=period,
    )
    cache["dom_updated"] = time.strftime("%H:%M:%S")
    cache["error"]       = None
    prev_dom = {"btc":btc,"eth":eth,"usdt":usdt,"alts":alts}

    detect_alt_signal(btc_chg, usdt_chg, alts_chg, alts, total_chg)
    print(f"[{cache['dom_updated']}] 도미({source}|{period}) "
          f"BTC={btc:.2f}% ETH={eth:.2f}% USDT={usdt:.2f}% ALT={alts:.2f}% "
          f"Δalts={alts_chg:+.3f} | 신호={cache['alt_signal']['level']}")


def _dom_from_coingecko():
    """CoinGecko에서 도미넌스 수집"""
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    r = req.get(COINGECKO_GLOBAL, headers=headers, timeout=15)
    if r.status_code == 429:
        raise Exception("CoinGecko 429 호출 제한")
    r.raise_for_status()
    data = r.json().get("data", {})
    pct  = data.get("market_cap_percentage", {})
    btc  = round(float(pct.get("btc",  0)), 3)
    eth  = round(float(pct.get("eth",  0)), 3)
    usdt = round(float(pct.get("usdt", 0)), 3)
    usdc = round(float(pct.get("usdc", 0)), 3)
    alts = round(max(0.0, 100.0 - btc - eth - usdt - usdc), 3)
    total_mcap = data.get("total_market_cap", {}).get("usd", 0)
    total_chg  = float(data.get("market_cap_change_percentage_24h_usd", 0))
    _save_dom(btc, eth, usdt, usdc, alts, total_mcap, total_chg,
              "✅ CoinGecko" if COINGECKO_API_KEY else "⚠ CoinGecko(키없음)")


def _dom_from_cmc():
    """TradingView CRYPTOCAP 방식과 동일하게 계산
    - 상위 200개 코인 시총 합산 → 분모로 사용
    - BTC/ETH/USDT/USDC 각각 시총 직접 조회
    - BTC.D = BTC시총 / 상위200개시총합 × 100
    """
    if not CMC_API_KEY:
        raise Exception("CMC_API_KEY 미설정")
    hdrs = {"X-CMC_PRO_API_KEY": CMC_API_KEY}

    # ① 상위 200개 코인 시총 수집 (TradingView는 상위 125개 기준)
    rl = req.get(CMC_LISTING_URL,
                 headers=hdrs,
                 params={"limit": 200, "convert": "USD", "sort": "market_cap"},
                 timeout=20)
    rl.raise_for_status()
    coins = rl.json().get("data", [])

    if not coins:
        raise Exception("CMC 리스팅 데이터 없음")

    # 상위 200개 시총 합산 (분모)
    total_mcap = sum(
        float(c.get("quote", {}).get("USD", {}).get("market_cap", 0) or 0)
        for c in coins
    )

    # 각 코인 시총 추출
    mcap_map = {}
    for c in coins:
        sym  = c.get("symbol", "")
        mcap = float(c.get("quote", {}).get("USD", {}).get("market_cap", 0) or 0)
        mcap_map[sym] = mcap

    btc_mcap  = mcap_map.get("BTC",  0)
    eth_mcap  = mcap_map.get("ETH",  0)
    usdt_mcap = mcap_map.get("USDT", 0)
    usdc_mcap = mcap_map.get("USDC", 0)

    denom = max(total_mcap, 1)
    btc  = round(btc_mcap  / denom * 100, 3)
    eth  = round(eth_mcap  / denom * 100, 3)
    usdt = round(usdt_mcap / denom * 100, 3)
    usdc = round(usdc_mcap / denom * 100, 3)
    alts = round(max(0.0, 100.0 - btc - eth - usdt - usdc), 3)

    # 24H 변화율은 글로벌 API에서 가져옴
    try:
        rg = req.get(CMC_GLOBAL_URL, headers=hdrs, timeout=10)
        rg.raise_for_status()
        q = rg.json().get("data", {}).get("quote", {}).get("USD", {})
        total_chg = float(q.get("total_market_cap_yesterday_percentage_change", 0))
    except:
        total_chg = 0.0

    _save_dom(btc, eth, usdt, usdc, alts, total_mcap, total_chg,
              "✅ CoinMarketCap")


def fetch_dominance():
    """CoinMarketCap 전용 도미넌스 수집.

    기존 CoinGecko 폴백을 제거해서 CMD에 "CoinGecko(키없음)" 경고가 뜨지 않게 함.
    CMC_API_KEY가 없거나 호출 실패 시 이전 캐시를 유지하고 에러만 기록한다.
    """
    if not CMC_API_KEY:
        cache["error"] = "도미넌스 수집 실패 | CMC_API_KEY 미설정"
        print("[도미 오류] CMC_API_KEY 미설정 — .env에 CMC_API_KEY를 넣어주세요")
        return

    try:
        _dom_from_cmc()
    except Exception as e_cmc:
        cache["error"] = f"도미넌스 수집 실패 | CMC:{e_cmc}"
        print(f"[도미 오류] CoinMarketCap 실패: {e_cmc}")


def detect_alt_signal(btc_chg, usdt_chg, alts_chg, alts_now, total_chg):
    score = 0
    reasons = []
    if alts_chg >= ALT_INFLOW_TH:
        score += 2; reasons.append(f"ALT.D {alts_chg:+.3f}%p↑↑")
    elif alts_chg > 0:
        score += 1; reasons.append(f"ALT.D {alts_chg:+.3f}%p↑")
    if btc_chg <= -BTC_DROP_TH:
        score += 2; reasons.append(f"BTC.D {btc_chg:+.3f}%p↓↓")
    elif btc_chg < 0:
        score += 1; reasons.append(f"BTC.D {btc_chg:+.3f}%p↓")
    if usdt_chg <= -USDT_DROP_TH:
        score += 2; reasons.append(f"USDT.D {usdt_chg:+.3f}%p↓(해소)")
    elif usdt_chg < 0:
        score += 1; reasons.append(f"USDT.D {usdt_chg:+.3f}%p↓")
    if total_chg > 2.0:
        score += 1; reasons.append(f"TOTAL {total_chg:+.1f}%")
    if alts_now > 30:
        score += 1; reasons.append(f"ALT.D {alts_now:.1f}%(시즌구간)")

    if   score >= 7: level, active = "HIGH CONF", True
    elif score >= 5: level, active = "강",         True
    elif score >= 3: level, active = "중",         True
    elif score >= 1: level, active = "약",         True
    else:            level, active = "없음",       False

    cache["alt_signal"] = {
        "active": active, "level": level, "score": score,
        "reason": " | ".join(reasons) if reasons else "조건 미충족",
        "triggered_at": time.strftime("%H:%M:%S") if active else None,
        "btc_chg": round(btc_chg,4), "usdt_chg": round(usdt_chg,4),
        "alts_chg": round(alts_chg,4), "alts_now": round(alts_now,2),
    }
    if active:
        maybe_send_telegram(cache["alt_signal"])


# ════════════════════════════════════════════════════════
# 백그라운드
# ════════════════════════════════════════════════════════
def bg_futures():
    """REST 백업 루프: 웹소켓이 정상일 때는 과도한 API 호출 방지"""
    while True:
        try:
            if (not ws_connected) or (time.time() - ws_last_recv > 20):
                fetch_futures()
            else:
                # WS 정상일 때도 60초마다 한 번 기준값 보정
                if int(time.time()) % 60 < 2:
                    fetch_futures()
        except Exception as e:
            print(f"[REST 백업] 오류: {e}")
        time.sleep(max(2, REFRESH_FUTURES))

def bg_dominance():
    while True:
        fetch_dominance()
        time.sleep(REFRESH_DOMINANCE)


# ════════════════════════════════════════════════════════
# API 엔드포인트
# ════════════════════════════════════════════════════════
@app.route("/api/dashboard")
@require_login
def dashboard():
    return jsonify({
        "gainers":         cache["gainers"],
        "losers":          cache["losers"],
        "gainers_major":   cache["gainers_major"],
        "losers_major":    cache["losers_major"],
        "all_coins":       cache["all_coins"],

        "top_funding":     cache["top_funding"],
        "surge_up":        cache["surge_up"],
        "surge_dn":        cache["surge_dn"],
        "oi_surge":        cache["oi_surge"],
        "futures_total":   cache["futures_total"],
        "futures_updated": cache["futures_updated"],
        "dominance":       cache["dominance"],
        "dom_updated":     cache["dom_updated"],
        "alt_signal":      cache["alt_signal"],
        "tv_signals":      cache["tv_signals"][-5:],
        "error":           cache["error"],
        "interval":        REFRESH_FUTURES,
        "ws_connected":    ws_connected,
        "ws_last_recv":    round(time.time() - ws_last_recv, 2) if ws_last_recv else None,
    })

# 코인 심볼 → CoinGecko ID / 아이콘 캐시
cg_id_cache   = {}
cg_icon_cache = {}   # {심볼소문자: 아이콘URL}

# ── 거래소 심볼 정규화 맵 ────────────────────────────
# MEXC 심볼 → 타 거래소 심볼 변환
# (MEXC 가 다른 거래소와 심볼명이 다른 경우)
SYMBOL_ALIAS = {
    # TON 계열
    "TONCOIN":   "TON",
    # 1000 배수 심볼
    "SHIB1000":  "SHIB",
    "PEPE1000":  "PEPE",
    "FLOKI1000": "FLOKI",
    "BONK1000":  "BONK",
    "SATS1000":  "SATS",
    "RATS1000":  "RATS",
    "NEIRO1000": "NEIRO",
    "LUNC":      "LUNC",
    "LUNA2":     "LUNA",
    # 기타 거래소별 명칭 차이
    "RNDR":        "RENDER",
    "WBTC":        "BTC",
    # MEXC 선물 → 현물 심볼 변경된 경우
    "TRUMPOFFICIAL": "TRUMP",     # 2025.01.18 변경
    "FILECOIN":      "FIL",       # 선물=FILECOIN, 현물=FIL
    "MELANIA":       "MELANIA",
}

def normalize_symbol(sym):
    """MEXC 심볼을 타 거래소 공통 심볼로 변환"""
    return SYMBOL_ALIAS.get(sym.upper(), sym.upper())


# ── 거래소별 상장 코인 캐시 ──────────────────────────
# 서버 시작 시 각 거래소 전체 심볼 목록을 한 번만 가져옴
# 코인 클릭 시 캐시에서 즉시 조회 (API 호출 없음)
# 거래소 심볼 캐시
exchange_symbols = {
    # ── 베이스 4개 (항상 있으면 표시) ─────────────
    "OKX":     set(),
    "Bitget":  set(),
    "BingX":   set(),
    "Binance": set(),
    # ── 5번째 우선 거래소 ──────────────────────────
    "Bybit":   set(),
    # ── 추가 대체 거래소 풀 ────────────────────────
    "Gate":    set(),
    "Phemex":  set(),
    "MEXC":    set(),
    "HTX":     set(),
}
exchange_loaded = {k: False for k in exchange_symbols}

# 베이스 4개 — 등록 여부 확인 후 있으면 표시 / 없으면 숨김
BASE_EXCHANGES   = ["OKX", "Bitget", "BingX", "Binance"]
# 5번째 우선 — Bybit 먼저, 없으면 대체 풀에서
FIFTH_PRIORITY   = "Bybit"
BACKUP_EXCHANGES = ["Gate", "Phemex", "MEXC", "HTX"]

def load_exchange_symbols():
    """서버 시작 시 각 거래소 전체 선물 심볼 목록 수집"""
    def load_okx():
        try:
            r = req.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP",
                        timeout=15)
            if r.status_code == 200:
                for d in r.json().get("data", []):
                    instId = d.get("instId","")
                    if instId.endswith("-USDT-SWAP"):
                        exchange_symbols["OKX"].add(
                            instId.replace("-USDT-SWAP","").upper())
                exchange_loaded["OKX"] = True
                print(f"[거래소] OKX {len(exchange_symbols['OKX'])}개 로드")
        except Exception as e:
            print(f"[거래소] OKX 실패: {e}")

    def load_bitget():
        try:
            r = req.get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
                        timeout=15)
            if r.status_code == 200:
                for d in r.json().get("data", []):
                    sym = d.get("symbol","")
                    if sym.endswith("USDT"):
                        exchange_symbols["Bitget"].add(sym.replace("USDT","").upper())
                exchange_loaded["Bitget"] = True
                print(f"[거래소] Bitget {len(exchange_symbols['Bitget'])}개 로드")
        except Exception as e:
            print(f"[거래소] Bitget 실패: {e}")

    def load_bingx():
        try:
            # 여러 엔드포인트 시도
            urls = [
                "https://open-api.bingx.com/openApi/swap/v2/quote/contracts",
                "https://open-api.bingx.com/openApi/swap/v1/market/all",
            ]
            loaded = False
            for url in urls:
                try:
                    r = req.get(url, timeout=15)
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    # data 구조가 다를 수 있음
                    items = data.get("data", data) if isinstance(data, dict) else data
                    if not isinstance(items, list):
                        items = []
                    for d in items:
                        sym = d.get("symbol", d.get("contractId",""))
                        if "-USDT" in sym:
                            exchange_symbols["BingX"].add(sym.replace("-USDT","").upper())
                        elif "USDT" in sym and not "-" in sym:
                            exchange_symbols["BingX"].add(sym.replace("USDT","").upper())
                    if exchange_symbols["BingX"]:
                        loaded = True
                        break
                except: continue

            if not loaded:
                # 직접 심볼 목록 확인 API
                r = req.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
                            timeout=15)
                if r.status_code == 200:
                    for d in r.json().get("data",[]):
                        sym = d.get("symbol","")
                        if sym.endswith("-USDT"):
                            exchange_symbols["BingX"].add(sym.replace("-USDT","").upper())

            exchange_loaded["BingX"] = True
            print(f"[거래소] BingX {len(exchange_symbols['BingX'])}개 로드")
        except Exception as e:
            print(f"[거래소] BingX 실패: {e}")

    def load_bybit():
        try:
            r = req.get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
                        timeout=15)
            if r.status_code == 200:
                for d in r.json().get("result",{}).get("list",[]):
                    sym = d.get("symbol","")
                    if sym.endswith("USDT"):
                        exchange_symbols["Bybit"].add(sym.replace("USDT","").upper())
                exchange_loaded["Bybit"] = True
                print(f"[거래소] Bybit {len(exchange_symbols['Bybit'])}개 로드")
        except Exception as e:
            print(f"[거래소] Bybit 실패: {e}")

    def load_gate():
        try:
            r = req.get("https://api.gateio.ws/api/v4/futures/usdt/contracts",
                        timeout=15)
            if r.status_code == 200:
                for d in r.json():
                    sym = d.get("name","")
                    if sym.endswith("_USDT"):
                        exchange_symbols["Gate"].add(sym.replace("_USDT","").upper())
                exchange_loaded["Gate"] = True
                print(f"[거래소] Gate {len(exchange_symbols['Gate'])}개 로드")
        except Exception as e:
            print(f"[거래소] Gate 실패: {e}")

    def load_binance():
        try:
            r = req.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=15)
            if r.status_code == 200:
                for d in r.json().get("symbols", []):
                    if d.get("quoteAsset") == "USDT" and d.get("contractType") == "PERPETUAL":
                        exchange_symbols["Binance"].add(d.get("baseAsset","").upper())
                exchange_loaded["Binance"] = True
                print(f"[거래소] Binance {len(exchange_symbols['Binance'])}개 로드")
        except Exception as e:
            print(f"[거래소] Binance 실패: {e}")

    def load_phemex():
        try:
            r = req.get("https://api.phemex.com/public/products", timeout=15)
            if r.status_code == 200:
                for d in r.json().get("data",{}).get("perpProductsV2",[]):
                    sym = d.get("symbol","")
                    if sym.endswith("USDT"):
                        exchange_symbols["Phemex"].add(sym.replace("USDT","").upper())
                exchange_loaded["Phemex"] = True
                print(f"[거래소] Phemex {len(exchange_symbols['Phemex'])}개 로드")
        except Exception as e:
            print(f"[거래소] Phemex 실패: {e}")

    def load_mexc():
        try:
            r = req.get("https://contract.mexc.com/api/v1/contract/detail", timeout=15)
            if r.status_code == 200:
                skipped = []
                for d in r.json().get("data",[]):
                    sym   = d.get("symbol","")
                    state = d.get("state", 0)   # 0=활성, 그 외 비활성/만기/결제
                    if not sym.endswith("_USDT"):
                        continue
                    name = sym.replace("_USDT","").upper()
                    if state != 0:
                        skipped.append(f"{name}(state={state})")
                        continue
                    exchange_symbols["MEXC"].add(name)
                    mexc_futures_detail_set.add(name)
                exchange_loaded["MEXC"] = True
                if skipped:
                    print(f"[거래소] MEXC 비활성 제외: {', '.join(skipped[:20])}")
                print(f"[거래소] MEXC {len(exchange_symbols['MEXC'])}개 로드 | 선물 필터셋 {len(mexc_futures_detail_set)}개")
        except Exception as e:
            print(f"[거래소] MEXC 실패: {e}")

    def load_htx():
        try:
            r = req.get("https://api.hbdm.com/linear-swap-api/v1/swap_contract_info",
                        timeout=15)
            if r.status_code == 200:
                for d in r.json().get("data",[]):
                    sym = d.get("contract_code","")
                    if sym.endswith("-USDT"):
                        exchange_symbols["HTX"].add(sym.replace("-USDT","").upper())
                exchange_loaded["HTX"] = True
                print(f"[거래소] HTX {len(exchange_symbols['HTX'])}개 로드")
        except Exception as e:
            print(f"[거래소] HTX 실패: {e}")

    # 전체 거래소 병렬 로드
    threads = [
        threading.Thread(target=load_okx,     daemon=True),
        threading.Thread(target=load_bitget,   daemon=True),
        threading.Thread(target=load_bingx,    daemon=True),
        threading.Thread(target=load_binance,  daemon=True),
        threading.Thread(target=load_bybit,    daemon=True),
        threading.Thread(target=load_gate,     daemon=True),
        threading.Thread(target=load_phemex,   daemon=True),
        threading.Thread(target=load_mexc,     daemon=True),
        threading.Thread(target=load_htx,      daemon=True),
    ]
    for t in threads: t.start()
    for t in threads: t.join(timeout=20)
    print(f"[거래소] 전체 로드 완료")


def check_exchanges(coin_name):
    """
    전체 거래소 등록 여부 반환
    - 심볼 정규화: TONCOIN→TON 등 자동 변환
    - 미로드 거래소는 None 반환 (브라우저에서 숨김 처리)
    """
    sym     = coin_name.upper()
    sym_alt = normalize_symbol(sym)
    result  = {}

    for ex, syms in exchange_symbols.items():
        if not exchange_loaded[ex]:
            result[ex] = None   # 미로드 — 브라우저에서 숨김
        else:
            result[ex] = (sym in syms) or (sym_alt in syms)

    return result


def get_cg_id(symbol):
    """심볼로 CoinGecko ID + 아이콘 URL 조회 (캐시 적용)"""
    sym_lower = symbol.lower()
    if sym_lower in cg_id_cache:
        return cg_id_cache[sym_lower]
    try:
        cg_key = os.getenv("COINGECKO_API_KEY", "")
        hdrs   = {"x-cg-demo-api-key": cg_key} if cg_key else {}
        r = req.get("https://api.coingecko.com/api/v3/search",
                    params={"query": symbol}, headers=hdrs, timeout=8)
        if r.status_code == 200:
            coins = r.json().get("coins", [])
            exact = next((c for c in coins
                         if c.get("symbol","").lower() == sym_lower), None)
            if exact:
                cg_id_cache[sym_lower]   = exact["id"]
                # 아이콘 URL도 캐시 (thumb 크기)
                cg_icon_cache[sym_lower] = exact.get("thumb", "")
                return exact["id"]
    except: pass
    return None



@app.route("/api/coin_chart/<symbol>")
def coin_chart_fast(symbol):
    """V1.5 STEP1: 상세 팝업용 차트 전용 빠른 API.
    CoinGecko/거래소 전체조회 없이 MEXC 캔들만 가져와서 1~3초 안에 차트 영역만 채운다.
    """
    try:
        sym = symbol.upper().replace("USDT","").replace("_","") + "_USDT"
        base = "https://contract.mexc.com/api/v1/contract"
        now_ts = int(time.time())
        start_ts = now_ts - 86400
        r_kline = req.get(f"{base}/kline/{sym}",
                          params={"interval":"Min60","start":start_ts,"end":now_ts},
                          timeout=4)
        if r_kline.status_code != 200:
            return jsonify({"error":"kline request failed"}), 200
        kline = r_kline.json().get("data", {})
        if not isinstance(kline, dict):
            return jsonify({"error":"kline data empty"}), 200

        times   = kline.get("time", []) or []
        opens   = kline.get("open", []) or []
        highs   = kline.get("high", []) or []
        lows    = kline.get("low", []) or []
        closes  = kline.get("close", []) or []
        amounts = kline.get("amount", []) or []

        candles = []
        oi_history_data = []
        n = min(len(times), len(closes))
        for i in range(n):
            amount = float(amounts[i] if i < len(amounts) else 0 or 0)
            candles.append({
                "t": times[i],
                "o": float(opens[i] if i < len(opens) else closes[i] or 0),
                "h": float(highs[i] if i < len(highs) else closes[i] or 0),
                "l": float(lows[i] if i < len(lows) else closes[i] or 0),
                "c": float(closes[i] or 0),
                "v": amount,
            })
            oi_history_data.append({"t": times[i], "v": amount})

        pct_changes = []
        for i in range(1, n):
            prev = float(closes[i-1] or 0)
            cur  = float(closes[i] or 0)
            if prev > 0:
                pct_changes.append((cur - prev) / prev * 100)
        avg_pct = sum(pct_changes)/len(pct_changes) if pct_changes else 0
        max_pct = max(pct_changes) if pct_changes else 0
        min_pct = min(pct_changes) if pct_changes else 0
        vol_avg = sum(float(x or 0) for x in amounts)/len(amounts) if amounts else 0
        vol_max = max([float(x or 0) for x in amounts], default=0)

        return jsonify({
            "symbol": sym,
            "name": sym.replace("_USDT", ""),
            "candles": candles,
            "oi_history": oi_history_data,
            "avg_hourly_pct": round(avg_pct, 6),
            "max_hourly_pct": round(max_pct, 6),
            "min_hourly_pct": round(min_pct, 6),
            "vol_avg_hourly": vol_avg,
            "vol_max_hourly": vol_max,
            "candle_count": len(candles),
            "fast_chart": True,
        })
    except Exception as e:
        print(f"[coin_chart_fast 오류] {symbol}: {e}")
        return jsonify({"error": str(e)}), 200


@app.route("/api/coin/<symbol>")
def coin_detail(symbol):
    """개별 코인 상세 통계 분석"""
    try:
        sym = symbol.upper().replace("USDT","").replace("_","") + "_USDT"
        base = "https://contract.mexc.com/api/v1/contract"
        import time as t

        # V1.4 DETAIL SPEED ENGINE
        # 기본 상세 요청은 외부 API를 새로 기다리지 않고, 이미 대시보드가 수집한 캐시로 즉시 반환한다.
        # 예전처럼 외부 상세 수집이 필요하면 /api/coin/<symbol>?full=1 로 호출 가능.
        if request.args.get("full") != "1":
            coin_name_fast = sym.replace("_USDT", "")
            coin = next((c for c in cache.get("all_coins", [])
                         if str(c.get("name", "")).upper() == coin_name_fast), None)
            if coin:
                price_fast = float(coin.get("price", 0) or 0)
                spot_fast = float(coin.get("spot", 0) or price_fast or 0)
                high_fast = float(coin.get("high24", 0) or price_fast or 0)
                low_fast  = float(coin.get("low24", 0) or price_fast or 0)
                funding_fast = float(coin.get("funding", 0) or 0)
                long_ratio = short_ratio = None
                if funding_fast != 0:
                    scale = min(abs(funding_fast) * 500, 30)
                    if funding_fast > 0:
                        long_ratio, short_ratio = round(50 + scale, 1), round(50 - scale, 1)
                    else:
                        long_ratio, short_ratio = round(50 - scale, 1), round(50 + scale, 1)
                return jsonify({
                    "symbol": sym,
                    "name": coin_name_fast,
                    "cg_info": {},
                    "icon_url": cg_icon_cache.get(coin_name_fast.lower(), ""),
                    "spot_price": spot_fast,
                    "exchanges": None,
                    "price": price_fast,
                    "pct_24h": round(float(coin.get("pct", 0) or 0), 4),
                    "volume": float(coin.get("volume", 0) or 0),
                    "funding": round(funding_fast, 6),
                    "oi": float(coin.get("oi", 0) or 0),
                    "oi_chg_pct": coin.get("oi_chg_pct"),
                    "oi_chg_abs": coin.get("oi_chg_abs"),
                    "fair": float(coin.get("fair", 0) or price_fast or 0),
                    "long_ratio": long_ratio,
                    "short_ratio": short_ratio,
                    "high24": high_fast,
                    "low24": low_fast,
                    "range_pct": round((high_fast-low_fast)/low_fast*100, 2) if low_fast > 0 else 0,
                    "avg_hourly_pct": 0,
                    "max_hourly_pct": 0,
                    "min_hourly_pct": 0,
                    "vol_avg_hourly": 0,
                    "vol_max_hourly": 0,
                    "fund_history": [],
                    "candles": [],
                    "candle_count": 0,
                    "oi_history": [],
                    "fast_cache": True,
                })

        # ① 현재 티커
        r_tick = req.get(f"{base}/ticker", params={"symbol": sym}, timeout=8)
        tick   = {}
        if r_tick.status_code == 200:
            raw = r_tick.json().get("data", {})
            if isinstance(raw, list):
                tick = next((d for d in raw if d.get("symbol") == sym), {})
            elif isinstance(raw, dict):
                tick = raw
            else:
                tick = {}

        # ② 펀딩비 히스토리 (최근 10회)
        r_fund = req.get(f"{base}/funding_rate/{sym}", timeout=8)
        fund_hist = []
        if r_fund.status_code == 200:
            raw_fund = r_fund.json().get("data", [])
            # 응답이 리스트인지 확인
            if isinstance(raw_fund, list):
                fund_hist = raw_fund[:10]
            elif isinstance(raw_fund, dict):
                # 딕셔너리면 리스트로 변환
                fund_hist = [raw_fund]

        # ③ 캔들 데이터 (1시간봉 24개 — 24시간)
        now_ts   = int(t.time())
        start_ts = now_ts - 86400
        r_kline  = req.get(f"{base}/kline/{sym}",
                           params={"interval":"Min60","start":start_ts,"end":now_ts},
                           timeout=8)
        kline = {}
        if r_kline.status_code == 200:
            raw_kline = r_kline.json().get("data", {})
            kline = raw_kline if isinstance(raw_kline, dict) else {}

        # ④ OI 히스토리 — 캔들에서 거래량(vol) 기반으로 OI 근사값 사용
        # MEXC는 OI 히스토리 별도 API 없음 → 캔들 amount(거래대금) 사용
        oi_history_data = []
        if kline:
            times   = kline.get("time",   [])
            amounts = kline.get("amount", [])  # 시간당 거래대금
            for i in range(len(times)):
                oi_history_data.append({
                    "t": times[i] if i < len(times)   else 0,
                    "v": amounts[i] if i < len(amounts) else 0,
                })

        # ⑤ 캔들로 통계 계산
        closes  = kline.get("close",  [])
        vols    = kline.get("vol",    [])
        highs   = kline.get("high",   [])
        lows    = kline.get("low",    [])
        amounts = kline.get("amount", [])
        times   = kline.get("time",   [])

        price   = float(tick.get("lastPrice",  0) or 0)
        pct_24h = float(tick.get("riseFallRate",0) or 0) * 100
        volume  = float(tick.get("amount24",   0) or 0)
        funding = float(tick.get("fundingRate",0) or 0) * 100
        oi      = float(tick.get("holdVol",    0) or 0)
        fair    = float(tick.get("fairPrice",  0) or 0)
        high24  = float(tick.get("high24Price",0) or 0)
        low24   = float(tick.get("lower24Price",0) or 0)

        # 변동률 통계
        pct_changes = []
        if len(closes) >= 2:
            for i in range(1, len(closes)):
                if closes[i-1] > 0:
                    pct_changes.append((closes[i]-closes[i-1])/closes[i-1]*100)

        avg_pct   = sum(pct_changes)/len(pct_changes) if pct_changes else 0
        max_pct   = max(pct_changes) if pct_changes else 0
        min_pct   = min(pct_changes) if pct_changes else 0
        vol_avg   = sum(amounts)/len(amounts) if amounts else 0
        vol_max   = max(amounts) if amounts else 0

        # 캔들 차트 데이터 (브라우저에서 그리기용)
        candles = []
        if times and closes:
            for i in range(len(times)):
                candles.append({
                    "t": times[i],
                    "o": kline.get("open",  [])[i] if i < len(kline.get("open",[])) else 0,
                    "h": highs[i]  if i < len(highs)  else 0,
                    "l": lows[i]   if i < len(lows)   else 0,
                    "c": closes[i] if i < len(closes)  else 0,
                    "v": amounts[i] if i < len(amounts) else 0,
                })

        # ⑤ CoinGecko 코인 정보 (시가총액·설명·링크 등)
        cg_info = {}
        cg_id   = get_cg_id(symbol.replace("_","").replace("USDT",""))
        if cg_id:
            try:
                cg_key = os.getenv("COINGECKO_API_KEY","")
                hdrs   = {"x-cg-demo-api-key": cg_key} if cg_key else {}
                r_cg   = req.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization":"false","tickers":"false",
                            "market_data":"true","community_data":"true",
                            "developer_data":"false","sparkline":"false"},
                    headers=hdrs, timeout=10)
                if r_cg.status_code == 200:
                    cg_raw  = r_cg.json()
                    md      = cg_raw.get("market_data", {})
                    links   = cg_raw.get("links", {})
                    desc_ko = cg_raw.get("description",{}).get("ko","")
                    desc_en = cg_raw.get("description",{}).get("en","")
                    desc    = desc_ko if desc_ko else desc_en
                    # 너무 길면 500자 자르기
                    if len(desc) > 500:
                        desc = desc[:500].rsplit(" ",1)[0] + "..."
                    cg_info = {
                        "id":           cg_id,
                        "full_name":    cg_raw.get("name",""),
                        "desc":         desc,
                        "market_cap":   md.get("market_cap",{}).get("usd",0),
                        "rank":         cg_raw.get("market_cap_rank",0),
                        "ath":          md.get("ath",{}).get("usd",0),
                        "ath_date":     md.get("ath_date",{}).get("usd",""),
                        "atl":          md.get("atl",{}).get("usd",0),
                        "total_supply": md.get("total_supply",0),
                        "circ_supply":  md.get("circulating_supply",0),
                        "website":      (links.get("homepage",[""]) or [""])[0],
                        "twitter":      links.get("twitter_screen_name",""),
                        "reddit":       links.get("subreddit_url",""),
                        "github":       (links.get("repos_url",{}).get("github",[""]) or [""])[0],
                        "categories":   cg_raw.get("categories",[])[:3],
                        "sentiment_up": cg_raw.get("sentiment_votes_up_percentage",0),
                        "image":        cg_raw.get("image",{}).get("small",""),
                    }
            except Exception as e_cg:
                print(f"[CoinGecko] {symbol} 조회 실패: {e_cg}")

        # 아이콘 캐시에서 가져오기
        name_lower = symbol.lower().replace("usdt","").replace("_","")
        icon_url   = cg_icon_cache.get(name_lower, "")
        if not icon_url and cg_id:
            icon_url = cg_info.get("image", "")

        # ⑥ 롱/숏 비율 — 펀딩비 기반 추정
        # 펀딩비 > 0: 롱 과열 / 펀딩비 < 0: 숏 과열
        long_ratio  = None
        short_ratio = None
        try:
            if funding != 0:
                # 펀딩비를 롱/숏 비율로 근사 변환
                # 펀딩비 0.01% = 롱 55% / 숏 45% 수준
                base = 50.0
                scale = min(abs(funding) * 500, 30)  # 최대 ±30%
                if funding > 0:
                    long_ratio  = round(base + scale, 1)
                    short_ratio = round(base - scale, 1)
                else:
                    long_ratio  = round(base - scale, 1)
                    short_ratio = round(base + scale, 1)
        except: pass

        # ⑦ OI 변화량 계산 (대시보드 캐시에서)
        coin_name_oi = sym.replace("_USDT","")
        oi_chg_pct   = None
        oi_chg_abs   = None
        if coin_name_oi in oi_history and len(oi_history[coin_name_oi]) >= 2:
            old_oi_val = oi_history[coin_name_oi][0]
            if old_oi_val > 0:
                oi_chg_abs = oi - old_oi_val
                oi_chg_pct = round((oi - old_oi_val) / old_oi_val * 100, 2)

        # ⑦ MEXC 현물 가격 조회
        spot_price = 0.0
        try:
            coin_name_spot = normalize_symbol(sym.replace("_USDT",""))
            r_spot = req.get(MEXC_SPOT_URL,
                             params={"symbol": coin_name_spot + "USDT"},
                             timeout=5)
            if r_spot.status_code == 200:
                spot_price = float(r_spot.json().get("price", 0) or 0)
        except: pass

        # ⑦ 거래소 등록 여부 확인
        coin_name = sym.replace("_USDT","")
        exchanges = check_exchanges(coin_name)

        return jsonify({
            "symbol":   sym,
            "name":     sym.replace("_USDT",""),
            "cg_info":  cg_info,
            "icon_url":   icon_url,
            "spot_price": spot_price,
            "exchanges":  exchanges,
            "price":    price,
            "pct_24h":  round(pct_24h, 4),
            "volume":   volume,
            "funding":  round(funding, 6),
            "oi":        oi,
            "oi_chg_pct": oi_chg_pct,
            "oi_chg_abs": oi_chg_abs,
            "fair":       fair,
            "long_ratio":  long_ratio,
            "short_ratio": short_ratio,
            "high24":   high24,
            "low24":    low24,
            "range_pct": round((high24-low24)/low24*100, 2) if low24 > 0 else 0,
            "avg_hourly_pct": round(avg_pct, 4),
            "max_hourly_pct": round(max_pct, 4),
            "min_hourly_pct": round(min_pct, 4),
            "vol_avg_hourly": vol_avg,
            "vol_max_hourly": vol_max,
            "fund_history":   fund_hist,
            "candles":        candles,
            "candle_count":   len(candles),
            "oi_history":     oi_history_data,   # 시간당 거래대금 (OI 근사값)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/refresh")
def manual_refresh():
    fetch_futures()
    fetch_dominance()
    return jsonify({"status": "ok",
                    "futures_updated": cache["futures_updated"],
                    "dom_updated":     cache["dom_updated"]})

@app.route("/api/icons")
def get_icons():
    """전체 캐시된 코인 아이콘 반환 + 미캐시 코인 배치 조회"""
    # 현재 캐시 반환
    return jsonify({"icons": cg_icon_cache})


@app.route("/api/icons/fetch/<symbol>")
def fetch_icon(symbol):
    """개별 코인 아이콘 조회 (없으면 CG에서 가져옴)"""
    sym_lower = symbol.lower()
    if sym_lower not in cg_icon_cache:
        get_cg_id(sym_lower)   # 조회 시 아이콘도 캐시됨
    return jsonify({
        "symbol": sym_lower,
        "icon":   cg_icon_cache.get(sym_lower, "")
    })


@app.route("/api/refresh_dom")
def refresh_dom():
    """도미넌스만 즉시 갱신 — 브라우저 3분 타이머에서 호출"""
    fetch_dominance()
    return jsonify({
        "status":      "ok",
        "dom_updated": cache["dom_updated"],
        "dominance":   cache["dominance"],
        "alt_signal":  cache["alt_signal"],
    })

@app.route("/api/reload_exchanges")
def reload_exchanges():
    """거래소 심볼 목록 수동 새로고침"""
    threading.Thread(target=load_exchange_symbols, daemon=True).start()
    return jsonify({"status": "reloading"})


@app.route("/api/filter_config", methods=["GET"])
def get_filter_config():
    """현재 필터 설정 조회"""
    return jsonify({
        "filter_config":     filter_config,
        "min_volume":        MIN_VOLUME,
        "min_oi":            MIN_OI,
        "min_abs_pct":       MIN_ABS_PCT,
    })


@app.route("/api/filter_config", methods=["POST"])
def set_filter_config():
    """필터 설정 변경 — exchange 파라미터로 MEXC/OKX 구분"""
    global filter_config, MIN_VOLUME, MIN_OI, MIN_ABS_PCT
    body     = request.get_json(force=True) or {}
    cfg = filter_config
    for key in list(cfg.keys()):
        if key in body:
            cfg[key] = bool(body[key])

    if "min_volume" in body:
        MIN_VOLUME = float(body["min_volume"])
    if "min_oi" in body:
        MIN_OI = float(body["min_oi"])
    if "min_abs_pct" in body:
        MIN_ABS_PCT = float(body["min_abs_pct"])

    threading.Thread(target=fetch_futures, daemon=True).start()

    return jsonify({
        "status":            "ok",
        "filter_config":     filter_config,
        "min_volume":        MIN_VOLUME,
        "min_oi":            MIN_OI,
        "min_abs_pct":       MIN_ABS_PCT,
    })


# 코인별 알람 설정 저장 (서버에서 체크)
coin_alarm_config = {}    # {코인명: {priceOn, priceDir, priceVal, pctOn, pctDir, pctVal}}
coin_alarm_last   = {}    # {코인명: 마지막 알림 시각}
COIN_ALARM_COOL   = 60    # 1분 쿨다운


@app.route("/api/coin_alarms", methods=["GET"])
def get_coin_alarms():
    return jsonify({"alarms": coin_alarm_config})


@app.route("/api/coin_alarms", methods=["POST"])
def set_coin_alarms():
    """브라우저에서 알람 설정 저장 시 서버에도 동기화"""
    global coin_alarm_config
    body = request.get_json(force=True) or {}
    coin_alarm_config = body.get("alarms", {})
    return jsonify({"status": "ok", "count": len(coin_alarm_config)})


def check_coin_alarms_server():
    """서버에서 코인 알람 체크 — 텔레그램 전송"""
    global coin_alarm_last
    now = time.time()

    for coin_name, alarm in coin_alarm_config.items():
        # 쿨다운 체크
        if now - coin_alarm_last.get(coin_name, 0) < COIN_ALARM_COOL:
            continue

        # 현재 코인 데이터 찾기
        coin = next((c for c in cache.get("all_coins", [])
                     if c.get("name") == coin_name), None)
        if not coin:
            continue

        price = coin.get("price", 0)
        pct   = coin.get("pct", 0)
        triggered = False
        msg = ""

        # 가격 알람
        if alarm.get("priceOn") and alarm.get("priceVal"):
            pv = float(alarm["priceVal"])
            if alarm.get("priceDir") == "above" and price >= pv:
                triggered = True
                msg = f"💰 <b>{coin_name}</b> 가격 <code>${price:.4f}</code> ↑목표가 <code>${pv:.4f}</code> 도달"
            elif alarm.get("priceDir") == "below" and price <= pv:
                triggered = True
                msg = f"💰 <b>{coin_name}</b> 가격 <code>${price:.4f}</code> ↓목표가 <code>${pv:.4f}</code> 도달"

        # 변동률 알람
        if not triggered and alarm.get("pctOn") and alarm.get("pctVal"):
            pv = float(alarm["pctVal"])
            if alarm.get("pctDir") == "above" and pct >= pv:
                triggered = True
                msg = f"📈 <b>{coin_name}</b> 변동률 <code>{pct:+.2f}%</code> ↑{pv}% 도달"
            elif alarm.get("pctDir") == "below" and pct <= -abs(pv):
                triggered = True
                msg = f"📉 <b>{coin_name}</b> 변동률 <code>{pct:+.2f}%</code> ↓{pv}% 도달"

        if triggered:
            coin_alarm_last[coin_name] = now
            ts       = time.strftime('%H:%M:%S')
            full_msg = f"🔔 <b>코인 알람</b>\n{msg}\n🕐 {ts}"
            threading.Thread(target=tg_send, args=(full_msg,), daemon=True).start()
            print(f"[코인알람] {coin_name}: {msg}")


@app.route("/api/sounds")
def get_sounds():
    """sounds 폴더의 mp3 파일 목록 반환"""
    import os, glob
    sounds_dir = os.path.join(os.path.dirname(__file__), "sounds")
    os.makedirs(sounds_dir, exist_ok=True)
    files = glob.glob(os.path.join(sounds_dir, "*.mp3"))
    names = [os.path.basename(f) for f in sorted(files)]
    return jsonify({"sounds": names})


@app.route("/sounds/<filename>")
def serve_sound(filename):
    """sounds 폴더의 mp3 파일 서빙"""
    import os
    from flask import send_from_directory
    sounds_dir = os.path.join(os.path.dirname(__file__), "sounds")
    return send_from_directory(sounds_dir, filename)


@app.route("/api/test_telegram")
def test_telegram():
    tg_send("✅ MEXC 알트 대시보드 텔레그램 연결 테스트 성공!")
    return jsonify({"status": "sent"})

@app.route("/api/security_log")
def security_log():
    """보안 이벤트 로그 확인"""
    return jsonify({"logs": cache["security_log"]})


# ── Webhook: 보안 ②③ 데코레이터 동시 적용 ────────────
@app.route("/webhook/tv", methods=["POST"])
@require_tv_ip       # ③ IP 필터
@require_secret      # ② 시크릿 키
def tv_webhook():
    try:
        body = request.get_json(force=True) or {}
        sig  = {
            "time":    datetime.now().strftime("%H:%M:%S"),
            "signal":  body.get("signal", "unknown"),
            "score":   body.get("score", ""),
            "message": body.get("message", str(body)),
        }
        cache["tv_signals"].append(sig)
        if len(cache["tv_signals"]) > 20:
            cache["tv_signals"] = cache["tv_signals"][-20:]

        if "ALT" in sig["signal"].upper() or "BUY" in sig["signal"].upper():
            threading.Thread(target=fetch_futures, daemon=True).start()
            tg_msg = (f"📡 <b>TradingView 신호 수신</b>\n"
                      f"🕐 {sig['time']}\n"
                      f"📊 {sig['signal']} {sig['score']}\n"
                      f"💬 {sig['message']}")
            threading.Thread(target=tg_send, args=(tg_msg,), daemon=True).start()

        log_security(f"TV Webhook 수신 OK — {sig['signal']}")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 400


# ════════════════════════════════════════════════════════
# CryptoBreaker Ecosystem 2차 — CB Point / 채굴 / 미션 / 랭킹 API
# ════════════════════════════════════════════════════════
DB_PATH = os.getenv("CBP_DB_PATH", "cryptobreaker_cbp.db")
DEFAULT_USER_ID = os.getenv("CBP_DEFAULT_USER", "guest")

REWARD_TABLE = {
    "attendance": 10,
    "mining_10m": 5,
    "dashboard_10m": 5,
    "telegram": 30,
    "mission": 50,
    "invite": 100,
    "alt_direction": 50,
    "top3": 100,
    "surge_pick": 70,
}
CLAIM_ONCE_PER_DAY = {"attendance", "dashboard_10m", "telegram", "invite", "mission"}


def cbp_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_cbp_db():
    with cbp_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                nickname TEXT NOT NULL,
                balance INTEGER NOT NULL DEFAULT 0,
                total_earned INTEGER NOT NULL DEFAULT 0,
                streak INTEGER NOT NULL DEFAULT 0,
                last_attendance TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cbp_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                reason TEXT NOT NULL,
                action_type TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cbp_claims (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                claim_date TEXT NOT NULL,
                UNIQUE(user_id, action_type, claim_date)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cbp_game_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                game_type TEXT NOT NULL,
                pick TEXT NOT NULL,
                direction TEXT,
                entry_price REAL,
                fee INTEGER NOT NULL DEFAULT 0,
                burn_amount INTEGER NOT NULL DEFAULT 0,
                reward INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'open',
                result TEXT,
                created_at TEXT NOT NULL,
                settled_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cbp_burn_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO users(user_id, nickname, balance, total_earned, created_at)
            VALUES (?, ?, 0, 0, ?)
        """, (DEFAULT_USER_ID, "CryptoBreaker", datetime.now().isoformat(timespec="seconds")))


def today_key():
    return datetime.now().strftime("%Y-%m-%d")


def add_cbp(user_id, amount, reason, action_type):
    now = datetime.now().isoformat(timespec="seconds")
    with cbp_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO users(user_id, nickname, balance, total_earned, created_at)
            VALUES (?, ?, 0, 0, ?)
        """, (user_id, user_id, now))
        conn.execute("""
            UPDATE users
               SET balance = balance + ?, total_earned = total_earned + ?
             WHERE user_id = ?
        """, (amount, amount, user_id))
        conn.execute("""
            INSERT INTO cbp_transactions(user_id, amount, reason, action_type, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, amount, reason, action_type, now))
    return amount


def deduct_cbp(user_id, amount, reason, action_type):
    """CBP 차감. 실제 현금 결제가 아니라 내부 테스트 포인트 차감용."""
    now = datetime.now().isoformat(timespec="seconds")
    with cbp_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO users(user_id, nickname, balance, total_earned, created_at)
            VALUES (?, ?, 0, 0, ?)
        """, (user_id, user_id, now))
        row = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row or row["balance"] < amount:
            return False
        conn.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))
        conn.execute("""
            INSERT INTO cbp_transactions(user_id, amount, reason, action_type, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, -amount, reason, action_type, now))
    return True


def add_burn_event(user_id, amount, reason):
    now = datetime.now().isoformat(timespec="seconds")
    with cbp_conn() as conn:
        conn.execute("""
            INSERT INTO cbp_burn_events(user_id, amount, reason, created_at)
            VALUES (?, ?, ?, ?)
        """, (user_id, amount, reason, now))


def find_coin(name):
    if not name:
        return None
    name = str(name).upper().replace('_USDT','')
    pools = [cache.get('all_coins', []), cache.get('gainers', []), cache.get('losers', [])]
    for pool in pools:
        for c in pool:
            if c.get('name') == name or c.get('symbol') == f'{name}_USDT':
                return c
    return None


def get_top3_names():
    return [c.get('name') for c in (cache.get('gainers') or [])[:3]]

def get_surge_top_names(n=5):
    # 5분 단기 급등 우선, 없으면 24H 상승률 상위 사용
    pool = cache.get('surge_up') or cache.get('gainers') or []
    names = []
    for c in pool:
        name = c.get('name')
        if name and name not in names:
            names.append(name)
        if len(names) >= n:
            break
    return names


def get_profile(user_id=DEFAULT_USER_ID):
    init_cbp_db()
    with cbp_conn() as conn:
        user = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not user:
            conn.execute("""
                INSERT INTO users(user_id, nickname, balance, total_earned, created_at)
                VALUES (?, ?, 0, 0, ?)
            """, (user_id, user_id, datetime.now().isoformat(timespec="seconds")))
            user = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        logs = conn.execute("""
            SELECT amount, reason, action_type, created_at
              FROM cbp_transactions
             WHERE user_id=?
             ORDER BY id DESC LIMIT 30
        """, (user_id,)).fetchall()
        game_entries = conn.execute("""
            SELECT id, game_type, pick, direction, entry_price, fee, burn_amount, reward, status, result, created_at, settled_at
              FROM cbp_game_entries
             WHERE user_id=?
             ORDER BY id DESC LIMIT 20
        """, (user_id,)).fetchall()
        burn_total = conn.execute("SELECT COALESCE(SUM(amount),0) AS v FROM cbp_burn_events").fetchone()["v"]
        today = today_key()
        today_mined = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS v
              FROM cbp_transactions
             WHERE user_id=? AND substr(created_at,1,10)=?
        """, (user_id, today)).fetchone()["v"]
        ranking = conn.execute("""
            SELECT nickname, total_earned
              FROM users
             ORDER BY total_earned DESC, balance DESC
             LIMIT 10
        """).fetchall()
        claimed = [r["action_type"] for r in conn.execute(
            "SELECT action_type FROM cbp_claims WHERE user_id=? AND claim_date=?",
            (user_id, today)
        ).fetchall()]
    return {
        "user_id": user["user_id"],
        "nickname": user["nickname"],
        "balance": user["balance"],
        "total_earned": user["total_earned"],
        "today_mined": today_mined,
        "streak": user["streak"],
        "claimed_today": claimed,
        "logs": [dict(r) for r in logs],
        "ranking": [dict(r) for r in ranking],
        "game_entries": [dict(r) for r in game_entries],
        "burn_total": burn_total,
    }


@app.route("/api/cbp/profile")
def api_cbp_profile():
    return jsonify(get_profile(request.args.get("user_id", DEFAULT_USER_ID)))


@app.route("/api/cbp/claim", methods=["POST"])
def api_cbp_claim():
    init_cbp_db()
    body = request.get_json(silent=True) or {}
    user_id = body.get("user_id") or DEFAULT_USER_ID
    action_type = body.get("type") or "mission"

    if action_type == "roulette":
        amount = random.choice([10, 30, 50, 100, 200])
        reason = "룰렛 게임 보상"
    elif action_type == "box":
        amount = random.randint(20, 200)
        reason = "박스 열기 보상"
    else:
        amount = int(body.get("amount") or REWARD_TABLE.get(action_type, 0))
        reason = body.get("reason") or {
            "attendance": "출석 체크",
            "mining_10m": "자동 채굴 10분 보상",
            "dashboard_10m": "대시보드 10분 사용",
            "telegram": "텔레그램 참여 보상",
            "invite": "친구 초대 보상",
            "alt_direction": "알트 방향 게임 보상",
            "top3": "급등 TOP3 게임 보상",
            "surge_pick": "급등 코인 예측 보상",
            "mission": "오늘 미션 완료 보상",
        }.get(action_type, "CBP 보상")

    if amount <= 0:
        return jsonify({"status": "error", "msg": "보상 금액이 올바르지 않습니다."}), 400

    today = today_key()
    if action_type in CLAIM_ONCE_PER_DAY:
        try:
            with cbp_conn() as conn:
                conn.execute(
                    "INSERT INTO cbp_claims(user_id, action_type, claim_date) VALUES (?, ?, ?)",
                    (user_id, action_type, today)
                )
        except sqlite3.IntegrityError:
            return jsonify({
                "status": "already_claimed",
                "msg": "오늘 이미 완료한 보상입니다.",
                "profile": get_profile(user_id)
            })

    if action_type == "attendance":
        with cbp_conn() as conn:
            row = conn.execute("SELECT last_attendance, streak FROM users WHERE user_id=?", (user_id,)).fetchone()
            streak = (row["streak"] or 0) + 1
            conn.execute("UPDATE users SET streak=?, last_attendance=? WHERE user_id=?", (streak, today, user_id))

    add_cbp(user_id, amount, reason, action_type)
    return jsonify({"status": "ok", "amount": amount, "reason": reason, "profile": get_profile(user_id)})


@app.route("/api/cbp/game/config")
def api_cbp_game_config():
    init_cbp_db()
    coins = cache.get("all_coins") or cache.get("gainers") or []
    # 게임은 BTC/ETH/스테이블이 아니라 실제 알트 중심으로 노출
    game_pool = []
    for c in coins:
        name = (c.get("name") or "").upper()
        if not name or name in EXCLUDE_MAJOR:
            continue
        game_pool.append(c)
    game_pool = sorted(game_pool, key=lambda x: abs(float(x.get("pct") or 0)), reverse=True)
    coin_list = [
        {"name": c.get("name"), "price": c.get("price"), "pct": c.get("pct"), "volume": c.get("volume")}
        for c in game_pool[:120]
    ]
    return jsonify({
        "status": "ok",
        "coins": coin_list,
        "top3_now": get_top3_names(),
        "surge_top5_now": get_surge_top_names(5),
        "surge_time_sec": 300,
        "fees": {
            "alt_direction": 20,
            "surge_pick": 25,
            "max_multiplier": 5,
            "top3": 30,
            "roulette": 50,
            "box": 0
        },
        "notice": "급등 코인 예측은 5분 기준입니다. 현재는 실제 현금/토큰 결제가 아닌 CBP 테스트 포인트입니다."
    })


@app.route("/api/cbp/game/play", methods=["POST"])
def api_cbp_game_play():
    init_cbp_db()
    body = request.get_json(silent=True) or {}
    user_id = body.get("user_id") or DEFAULT_USER_ID
    game_type = body.get("game_type") or "roulette"
    now = datetime.now().isoformat(timespec="seconds")
    today = today_key()

    if game_type == "roulette":
        # 7차: 참가비 50 CBP / 확률형 보상 0~500 CBP
        # 확률: 0=40%, 20=30%, 50=20%, 100=8%, 200=1.5%, 500=0.5%
        fee = int(body.get("fee") or 50)
        if not deduct_cbp(user_id, fee, "룰렛 참가비", "game_fee"):
            return jsonify({"status":"error", "msg":"CBP 잔액이 부족합니다.", "profile":get_profile(user_id)}), 400
        burn = max(1, int(fee * 0.30))
        roll = random.random() * 100
        if roll < 0.5:
            reward = 500
        elif roll < 2.0:
            reward = 200
        elif roll < 10.0:
            reward = 100
        elif roll < 30.0:
            reward = 50
        elif roll < 60.0:
            reward = 20
        else:
            reward = 0
        add_burn_event(user_id, burn, "룰렛 참가비 일부 소각 적립")
        if reward > 0:
            add_cbp(user_id, reward, "룰렛 당첨 보상", "roulette_reward")
        with cbp_conn() as conn:
            conn.execute("""
                INSERT INTO cbp_game_entries(user_id, game_type, pick, fee, burn_amount, reward, status, result, created_at, settled_at)
                VALUES (?, 'roulette', 'SPIN', ?, ?, ?, 'settled', ?, ?, ?)
            """, (user_id, fee, burn, reward, f"보상 {reward} CBP", now, now))
        return jsonify({"status":"ok", "msg":f"룰렛 결과: {reward} CBP", "reward":reward, "burn":burn, "roulette_reward": reward, "profile":get_profile(user_id)})

    if game_type == "box":
        try:
            with cbp_conn() as conn:
                conn.execute("INSERT INTO cbp_claims(user_id, action_type, claim_date) VALUES (?, 'daily_box', ?)", (user_id, today))
        except sqlite3.IntegrityError:
            return jsonify({"status":"already_claimed", "msg":"랜덤 박스는 하루 1번만 열 수 있습니다.", "profile":get_profile(user_id)})
        reward = random.randint(20, 200)
        add_cbp(user_id, reward, "일일 랜덤 박스 보상", "daily_box")
        with cbp_conn() as conn:
            conn.execute("""
                INSERT INTO cbp_game_entries(user_id, game_type, pick, fee, burn_amount, reward, status, result, created_at, settled_at)
                VALUES (?, 'box', 'DAILY_BOX', 0, 0, ?, 'settled', ?, ?, ?)
            """, (user_id, reward, f"박스 보상 {reward} CBP", now, now))
        return jsonify({"status":"ok", "msg":f"랜덤 박스 보상: {reward} CBP", "reward":reward, "profile":get_profile(user_id)})

    if game_type == "alt_direction":
        coin_name = (body.get("coin") or "").upper()
        direction = (body.get("direction") or "up").lower()
        multiplier = max(1, min(5, int(body.get("multiplier") or 1)))
        base_fee = 20
        fee = base_fee * multiplier
        coin = find_coin(coin_name)
        if not coin:
            return jsonify({"status":"error", "msg":"코인을 찾을 수 없습니다. 먼저 대시보드를 갱신해줘."}), 400
        if direction not in {"up", "down"}:
            return jsonify({"status":"error", "msg":"방향은 up/down만 가능합니다."}), 400
        if not deduct_cbp(user_id, fee, "알트 방향 맞히기 참가비", "game_fee"):
            return jsonify({"status":"error", "msg":"CBP 잔액이 부족합니다.", "profile":get_profile(user_id)}), 400
        burn = max(1, int(fee * 0.30))
        add_burn_event(user_id, burn, "알트 방향 게임 참가비 일부 소각 적립")
        with cbp_conn() as conn:
            conn.execute("""
                INSERT INTO cbp_game_entries(user_id, game_type, pick, direction, entry_price, fee, burn_amount, status, result, created_at)
                VALUES (?, 'alt_direction', ?, ?, ?, ?, ?, 'open', ?, ?)
            """, (user_id, coin_name, direction, float(coin.get("price") or 0), fee, burn, f"{multiplier}x 배율 참가", now))
        return jsonify({"status":"ok", "msg":f"{coin_name} {'상승' if direction=='up' else '하락'} 예측 {multiplier}배 참가 완료", "burn":burn, "multiplier":multiplier, "fee":fee, "profile":get_profile(user_id)})

    if game_type == "surge_pick":
        coin_name = (body.get("coin") or "").upper()
        multiplier = max(1, min(5, int(body.get("multiplier") or 1)))
        base_fee = 25
        fee = base_fee * multiplier
        coin = find_coin(coin_name)
        if not coin:
            return jsonify({"status":"error", "msg":"코인을 찾을 수 없습니다. 먼저 대시보드를 갱신해줘."}), 400
        if not deduct_cbp(user_id, fee, "급등 코인 예측 참가비", "game_fee"):
            return jsonify({"status":"error", "msg":"CBP 잔액이 부족합니다.", "profile":get_profile(user_id)}), 400
        burn = max(1, int(fee * 0.30))
        add_burn_event(user_id, burn, "급등 코인 예측 참가비 일부 소각 적립")
        with cbp_conn() as conn:
            conn.execute("""
                INSERT INTO cbp_game_entries(user_id, game_type, pick, entry_price, fee, burn_amount, status, result, created_at)
                VALUES (?, 'surge_pick', ?, ?, ?, ?, 'open', ?, ?)
            """, (user_id, coin_name, float(coin.get("pct") or 0), fee, burn, f"{multiplier}x 배율 참가", now))
        return jsonify({"status":"ok", "msg":f"{coin_name} 급등 코인 예측 {multiplier}배 참가 완료", "burn":burn, "multiplier":multiplier, "fee":fee, "profile":get_profile(user_id)})

    if game_type == "top3":
        picks = body.get("picks") or []
        if isinstance(picks, str):
            picks = [x.strip().upper() for x in picks.split(',') if x.strip()]
        picks = [str(x).upper() for x in picks][:3]
        fee = int(body.get("fee") or 30)
        if len(set(picks)) != 3:
            return jsonify({"status":"error", "msg":"급등 TOP3는 서로 다른 알트 3개를 선택해야 합니다."}), 400
        if not deduct_cbp(user_id, fee, "급등 TOP3 맞히기 참가비", "game_fee"):
            return jsonify({"status":"error", "msg":"CBP 잔액이 부족합니다.", "profile":get_profile(user_id)}), 400
        burn = max(1, int(fee * 0.30))
        add_burn_event(user_id, burn, "TOP3 게임 참가비 일부 소각 적립")
        with cbp_conn() as conn:
            conn.execute("""
                INSERT INTO cbp_game_entries(user_id, game_type, pick, fee, burn_amount, status, created_at)
                VALUES (?, 'top3', ?, ?, ?, 'open', ?)
            """, (user_id, ','.join(picks), fee, burn, now))
        return jsonify({"status":"ok", "msg":"급등 TOP3 예측 참가 완료", "burn":burn, "profile":get_profile(user_id)})

    return jsonify({"status":"error", "msg":"지원하지 않는 게임입니다."}), 400


@app.route("/api/cbp/game/settle", methods=["POST"])
def api_cbp_game_settle():
    init_cbp_db()
    body = request.get_json(silent=True) or {}
    user_id = body.get("user_id") or DEFAULT_USER_ID
    entry_id = int(body.get("entry_id") or 0)
    now = datetime.now().isoformat(timespec="seconds")

    with cbp_conn() as conn:
        entry = conn.execute("SELECT * FROM cbp_game_entries WHERE id=? AND user_id=?", (entry_id, user_id)).fetchone()
        if not entry:
            return jsonify({"status":"error", "msg":"게임 내역을 찾을 수 없습니다."}), 404
        if entry["status"] != "open":
            return jsonify({"status":"already_settled", "msg":"이미 정산된 게임입니다.", "profile":get_profile(user_id)})

    win = False
    result_msg = ""
    reward = 0
    if entry["game_type"] == "alt_direction":
        coin = find_coin(entry["pick"])
        if not coin:
            return jsonify({"status":"error", "msg":"현재 가격을 찾을 수 없습니다. 대시보드 갱신 후 다시 눌러줘."}), 400
        now_price = float(coin.get("price") or 0)
        entry_price = float(entry["entry_price"] or 0)
        actual = "up" if now_price > entry_price else "down" if now_price < entry_price else "flat"
        win = actual == entry["direction"]
        reward = int(entry["fee"] * 2) if win else 0
        result_msg = f"진입 {entry_price} → 현재 {now_price} / 결과 {actual}"
    elif entry["game_type"] == "surge_pick":
        # 5분 기준: 참가 후 최소 5분이 지난 뒤 현재 단기급등 TOP5 안에 들어가면 성공
        coin = find_coin(entry["pick"])
        if not coin:
            return jsonify({"status":"error", "msg":"현재 데이터를 찾을 수 없습니다. 대시보드 갱신 후 다시 눌러줘."}), 400
        try:
            elapsed = (datetime.now() - datetime.fromisoformat(entry["created_at"])).total_seconds()
        except Exception:
            elapsed = 999999
        if elapsed < 300:
            remain = int(300 - elapsed)
            return jsonify({"status":"wait", "msg":f"급등 코인 예측은 5분 기준입니다. {remain}초 후 정산 가능합니다.", "remain":remain, "profile":get_profile(user_id)})
        now_pct = float(coin.get("pct") or 0)
        entry_pct = float(entry["entry_price"] or 0)
        top5 = get_surge_top_names(5)
        win = entry["pick"] in set(top5)
        reward = int(entry["fee"] * 3) if win else 0
        result_msg = f"5분 기준 후보 TOP5: {', '.join(top5)} / 참가시 {entry_pct:.2f}% → 현재 {now_pct:.2f}%"
    elif entry["game_type"] == "top3":
        picks = set((entry["pick"] or "").split(','))
        actual_top3 = set(get_top3_names())
        matched = len(picks & actual_top3)
        win = matched >= 2
        reward = int(entry["fee"] * (4 if matched == 3 else 2 if matched == 2 else 0))
        result_msg = f"현재 TOP3: {', '.join(get_top3_names())} / 적중 {matched}개"
    else:
        return jsonify({"status":"error", "msg":"이 게임은 수동 정산 대상이 아닙니다."}), 400

    if reward > 0:
        add_cbp(user_id, reward, f"게임 정산 보상 - {entry['game_type']}", "game_reward")
    with cbp_conn() as conn:
        conn.execute("""
            UPDATE cbp_game_entries
               SET status='settled', result=?, reward=?, settled_at=?
             WHERE id=? AND user_id=?
        """, (result_msg + (" / 성공" if win else " / 실패"), reward, now, entry_id, user_id))
    return jsonify({"status":"ok", "win":win, "reward":reward, "msg":result_msg, "profile":get_profile(user_id)})


@app.route("/api/cbp/reset", methods=["POST"])
def api_cbp_reset():
    user_id = (request.get_json(silent=True) or {}).get("user_id") or DEFAULT_USER_ID
    with cbp_conn() as conn:
        conn.execute("DELETE FROM cbp_transactions WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM cbp_claims WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM cbp_game_entries WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM cbp_burn_events WHERE user_id=?", (user_id,))
        conn.execute("UPDATE users SET balance=0,total_earned=0,streak=0,last_attendance=NULL WHERE user_id=?", (user_id,))
    return jsonify({"status": "ok", "profile": get_profile(user_id)})

@app.route("/")
@require_login
def index():
    return send_from_directory(".", "index.html")




# ════════════════════════════════════════════════════════
# 시작 시 보안 설정 점검
# ════════════════════════════════════════════════════════
def check_security():
    print("\n[ 보안 설정 점검 ]")
    ok = True

    if not TELEGRAM_TOKEN:
        print("  ⚠  TELEGRAM_TOKEN 미설정 — 텔레그램 알림 비활성화")
        ok = False
    else:
        print("  ✅ TELEGRAM_TOKEN 설정됨")

    if not WEBHOOK_SECRET:
        print("  ⚠  WEBHOOK_SECRET 미설정 — Webhook 인증 비활성화")
        ok = False
    else:
        print("  ✅ WEBHOOK_SECRET 설정됨")

    if WEBHOOK_SECRET and len(WEBHOOK_SECRET) < 16:
        print("  ⚠  WEBHOOK_SECRET 가 너무 짧습니다 (16자 이상 권장)")

    print(f"  ✅ TV IP 필터 활성화 — {len(TV_ALLOWED_IPS) - 2}개 IP 허용")
    print(f"  ✅ Rate Limit 활성화 — 분당 최대 {RATE_LIMIT}회")
    print()
    return ok


# ════════════════════════════════════════════════════════
# 실행
# ════════════════════════════════════════════════════════



@app.route("/api/stream")
def sse_stream():
    """브라우저로 실시간 대시보드 데이터를 밀어주는 SSE 엔드포인트"""
    def generate():
        q = queue.Queue(maxsize=1)  # V4: 최신 프레임만 유지
        with sse_lock:
            sse_clients.append(q)
        print(f"[SSE] 클라이언트 연결 (총 {len(sse_clients)}개)")
        try:
            yield "retry: 1200\n"
            yield "data: " + _json.dumps({"connected": True, "ws_connected": ws_connected}, ensure_ascii=False, separators=(",", ":")) + "\n\n"
            while True:
                try:
                    yield q.get(timeout=30)
                except queue.Empty:
                    yield ": ping\n\n"
        except GeneratorExit:
            pass
        finally:
            with sse_lock:
                if q in sse_clients:
                    sse_clients.remove(q)
            print(f"[SSE] 클라이언트 해제 (총 {len(sse_clients)}개)")
    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control":"no-cache, no-transform", "Connection":"keep-alive", "X-Accel-Buffering":"no", "Access-Control-Allow-Origin":"*"})

# 28-2D: 프론트 출석/CBK 보상 버튼 404 방지용 더미 엔드포인트
@app.route('/api/cbk/claim', methods=['POST'])
def api_cbk_claim():
    return jsonify({"ok": True, "success": True, "message": "CBK 보상 기능은 준비중입니다."})

if __name__ == "__main__":
    print("=" * 62)
    print("  도미넌스 플로우 + MEXC 알트 + 텔레그램 봇 [보안 강화]")
    print(f"  선물 갱신    : {REFRESH_FUTURES}초")
    print(f"  도미 갱신    : {REFRESH_DOMINANCE}초")
    print(f"  접속 주소    : http://localhost:5000")
    print(f"  보안 로그    : http://localhost:5000/api/security_log")
    print("=" * 62)

    check_security()

    try:
        print("[시작] 거래소 심볼 로드 중...")
        threading.Thread(target=load_exchange_symbols, daemon=True).start()
        print("[시작] 도미넌스 수집 중...")
        fetch_dominance()
        print("[시작] 현물 가격 수집 중...")
        fetch_spot_prices()
        print("[시작] 선물 데이터 최초 수집 중... (REST)")
        fetch_futures()
        print("[시작] 백그라운드 스레드 시작...")
        threading.Thread(target=bg_dominance, daemon=True).start()
        threading.Thread(target=bg_websocket, daemon=True).start()
        threading.Thread(target=bg_ws_processor, daemon=True).start()
        threading.Thread(target=bg_futures,   daemon=True).start()
        threading.Thread(target=bg_spot,      daemon=True).start()
        print("[시작] 완료 ✅ (웹소켓/SSE 실시간 출력 활성화)")
    except Exception as e:
        import traceback
        print(f"[시작 오류] {e}")
        traceback.print_exc()
    # 127.0.0.1 = 내 PC에서만 접근 가능 (같은 네트워크 차단)
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)


# ── 28-2D FIX: 프론트 보조 API (대시보드 캐시 기반) ─────────────
@app.route("/api/dominance")
def api_dominance_compat():
    return jsonify({"ok": True, "dominance": cache.get("dominance", {}), "dom_updated": cache.get("dom_updated")})

@app.route("/api/oi-flow")
def api_oi_flow_compat():
    oi = cache.get("oi_surge", []) or []
    up = cache.get("surge_up", []) or []
    dn = cache.get("surge_dn", []) or []
    return jsonify({
        "ok": True,
        "oi_surge": oi,
        "plus_oi_price_up": len(up),
        "plus_oi_price_down": len(dn),
        "new_entry": len(oi),
        "liquidation_warning": 0,
    })

@app.route("/api/fear-greed")
def api_fear_greed_compat():
    return jsonify({"ok": True, "value": 68, "status": "Greed", "note": "fallback"})
