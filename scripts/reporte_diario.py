"""
SMC REPORTE DIARIO — Argentina Edition
=======================================
Universo: 52 CEDEARs con volumen BYMA + 11 ADRs argentinos + 11 Panel Lider
CCL: CriptoYa (primario) + DolarAPI (fallback)
Swing Length: 50 (calibrado igual que LuxAlgo TradingView)
Horario: Corre a las 7AM ARG — reporte listo antes de la apertura
Notificaciones: Telegram
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import warnings
import logging
import time
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

warnings.filterwarnings('ignore')

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('smc')

# ── Zona horaria Argentina ────────────────────────────────────
TZ_ARG = ZoneInfo('America/Argentina/Buenos_Aires')
def now_arg():
    return datetime.now(TZ_ARG)

# ============================================================
# PARAMETROS
# ============================================================
SWING_LENGTH        = 50
DISCOUNT_PCT        = 0.25
NEAR_DISCOUNT_PCT   = 0.40
EQUILIBRIUM_BAND    = 0.05
ESTRUCTURA_ALCISTA  = True
RS_DIAS             = 5
RS_RATIO_MIN        = 1.02
ABSORCION_VOL_RATIO = 2.5
SQUEEZE_RATIO       = 0.65
SCORE_MINIMO        = 3
TARGET_PCT          = 3.0
STOP_PCT            = 1.5
DATA_PERIOD         = '1y'
BATCH_SIZE          = 10
SLEEP_BETWEEN       = 1.5
CCL_FALLBACK        = 1200.0
_RATIOS_JSON        = 'results/ratios_cedears.json'

# ============================================================
# UNIVERSO — 52 CEDEARs con volumen real en BYMA
# ============================================================
CEDEARS_NYSE = [
    'MELI', 'VIST', 'NU',    'MSFT', 'ORCL', 'HMY',   'SPY',   'USO',   'HUT',  'IBIT',
    'MU',   'NIO',  'NVDA',  'PBR',  'GOOGL','MSTR',  'AMZN',  'SQ',    'MO',   'KO',
    'AAPL', 'SATL', 'EWZ',   'SH',   'ASTS', 'TSLA',  'VXX',   'BRK-B', 'UNH',  'META',
    'ETHA', 'IBM',  'CRM',   'GLD',  'GLOB', 'COIN',  'QQQ',   'SLV',   'AMD',  'BAC',
    'V',    'URA',  'STNE',  'CSCO', 'ANF',  'XLE',   'PLTR',  'GOLD',  'MRNA', 'BITF',
    'PAGS', 'GPRK',
]

ADRS_ARG_NYSE = {
    'GGAL': {'byma': 'GGAL.BA',  'nombre': 'Grupo Galicia',   'ratio': 10},
    'YPF':  {'byma': 'YPFD.BA',  'nombre': 'YPF',             'ratio': 1},
    'PAM':  {'byma': 'PAMP.BA',  'nombre': 'Pampa Energia',   'ratio': 25},
    'BMA':  {'byma': 'BMA.BA',   'nombre': 'Banco Macro',     'ratio': 10},
    'CEPU': {'byma': 'CEPU.BA',  'nombre': 'Central Puerto',  'ratio': 10},
    'LOMA': {'byma': 'LOMA.BA',  'nombre': 'Loma Negra',      'ratio': 5},
    'SUPV': {'byma': 'SUPV.BA',  'nombre': 'Supervielle',     'ratio': 5},
    'TGS':  {'byma': 'TGSU2.BA', 'nombre': 'TGS',             'ratio': 5},
    'MELI': {'byma': 'MELI.BA',  'nombre': 'MercadoLibre',    'ratio': 1},
    'GLOB': {'byma': 'GLOB.BA',  'nombre': 'Globant',         'ratio': 1},
    'DESP': {'byma': 'DESP.BA',  'nombre': 'Despegar',        'ratio': 1},
}

PANEL_LIDER_BYMA = {
    'TXAR.BA':  'Ternium Argentina',
    'ALUA.BA':  'Aluar',
    'BBAR.BA':  'BBVA Argentina',
    'TECO2.BA': 'Telecom Argentina',
    'COME.BA':  'Sociedad Comercial del Plata',
    'CVH.BA':   'Cablevision Holding',
    'MIRG.BA':  'Mirgor',
    'BYMA.BA':  'BYMA',
    'VALO.BA':  'Grupo Valores',
    'EDN.BA':   'Edenor',
    'CRES.BA':  'Cresud',
}

SECTOR_ETF = {
    'Technology':             'XLK',
    'Financial Services':     'XLF',
    'Healthcare':             'XLV',
    'Energy':                 'XLE',
    'Consumer Cyclical':      'XLY',
    'Consumer Defensive':     'XLP',
    'Industrials':            'XLI',
    'Basic Materials':        'XLB',
    'Communication Services': 'XLC',
}

# ============================================================
# TELEGRAM
# ============================================================
def enviar_telegram(mensaje: str, token: str, chat_id: str) -> bool:
    """
    Envia un mensaje a Telegram.
    Divide mensajes largos en chunks de 4000 chars (limite Telegram: 4096).
    """
    if not token or not chat_id:
        log.warning("Telegram: TOKEN o CHAT_ID no configurados — saltando envio")
        return False

    url      = f"https://api.telegram.org/bot{token}/sendMessage"
    max_len  = 4000
    chunks   = [mensaje[i:i+max_len] for i in range(0, len(mensaje), max_len)]
    enviados = 0

    for idx, chunk in enumerate(chunks):
        try:
            r = requests.post(url, json={
                'chat_id':    chat_id,
                'text':       chunk,
                'parse_mode': 'HTML',
            }, timeout=15)
            if r.status_code == 200:
                enviados += 1
                log.info(f"Telegram: chunk {idx+1}/{len(chunks)} enviado OK")
            else:
                log.error(f"Telegram error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.error(f"Telegram excepcion: {e}")
        if len(chunks) > 1:
            time.sleep(0.5)

    return enviados == len(chunks)


def formatear_reporte_telegram(reporte: str, hits: int, ccl: float) -> str:
    """
    Convierte el reporte texto a formato Telegram con HTML basico.
    Envia un resumen corto primero y luego el detalle completo.
    """
    now    = now_arg().strftime('%d/%m/%Y %H:%M')
    emoji  = "🟢" if hits > 0 else "⚪"
    resumen = (
        f"<b>📊 SMC REPORTE DIARIO — {now} (ARG)</b>\n"
        f"CCL: <b>${ccl:,.0f}</b> ARS/USD\n"
        f"{emoji} Señales encontradas: <b>{hits}</b>\n"
        f"{'─'*30}\n\n"
    )
    # Limpiar caracteres que rompen HTML de Telegram
    detalle = reporte.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    return resumen + f"<pre>{detalle}</pre>"


# ============================================================
# RATIOS
# ============================================================
_RATIOS_CEDEARS = {}
_ADRS_ARG_JSON  = {}

def _cargar_ratios():
    global _RATIOS_CEDEARS, _ADRS_ARG_JSON
    if not os.path.exists(_RATIOS_JSON):
        log.warning(f"{_RATIOS_JSON} no encontrado — usando ratios hardcodeados")
        return
    try:
        with open(_RATIOS_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        data.pop('_meta', None)
        for ticker, info in data.items():
            ratio = info.get('ratio', 1)
            if not ratio or float(ratio) <= 0:
                log.warning(f"Ratio invalido para {ticker}: {ratio} — usando 1")
                info['ratio'] = 1
            if info.get('tipo') == 'ADR-Argentina':
                _ADRS_ARG_JSON[ticker] = info
            else:
                _RATIOS_CEDEARS[ticker] = info
        log.info(f"Ratios: {len(_RATIOS_CEDEARS)} CEDEARs + {len(_ADRS_ARG_JSON)} ADRs")
    except Exception as e:
        log.error(f"Error cargando ratios: {e}")

def get_ratio(ticker: str) -> float:
    info = _RATIOS_CEDEARS.get(ticker) or _ADRS_ARG_JSON.get(ticker)
    if info and info.get('ratio') and float(info['ratio']) > 0:
        return float(info['ratio'])
    adr_info = ADRS_ARG_NYSE.get(ticker)
    if adr_info:
        return float(adr_info['ratio'])
    return 1.0

# ============================================================
# CCL
# ============================================================
def get_ccl() -> tuple[float, str]:
    try:
        r = requests.get('https://criptoya.com/api/dolar', timeout=5)
        if r.status_code == 200:
            data  = r.json()
            ccl   = data.get('ccl', {})
            venta = ccl.get('ask') or ccl.get('venta') or ccl.get('price')
            if venta and float(venta) > 100:
                return float(venta), 'CriptoYa'
    except Exception as e:
        log.debug(f"CriptoYa fallo: {e}")
    try:
        r = requests.get('https://dolarapi.com/v1/dolares/contadoconliqui', timeout=5)
        if r.status_code == 200:
            data  = r.json()
            venta = data.get('venta')
            if venta and float(venta) > 100:
                return float(venta), 'DolarAPI'
    except Exception as e:
        log.debug(f"DolarAPI fallo: {e}")
    log.warning(f"Usando CCL fallback: {CCL_FALLBACK}")
    return CCL_FALLBACK, 'FALLBACK'

# ============================================================
# FUNCIONES SMC
# ============================================================
def to_arr(s) -> np.ndarray:
    return np.array(s).flatten()

def calculate_rsi(arr: np.ndarray, period: int = 14) -> np.ndarray:
    s     = pd.Series(arr)
    delta = s.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, min_periods=period).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, min_periods=period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return to_arr(100 - (100 / (1 + rs)))

def find_swings(high: np.ndarray, low: np.ndarray, length: int) -> tuple:
    sh, sl = [], []
    for i in range(length, len(high)):
        window_h = high[i-length:i]
        window_l = low[i-length:i]
        if len(window_h) == 0:
            continue
        if high[i-length] == np.max(window_h):
            sh.append((i-length, float(high[i-length])))
        if low[i-length] == np.min(window_l):
            sl.append((i-length, float(low[i-length])))
    return sh, sl

def get_zones(top: float, bottom: float) -> dict:
    r = top - bottom
    return {
        'discount':      (bottom, bottom + DISCOUNT_PCT * r),
        'near_discount': (bottom, bottom + NEAR_DISCOUNT_PCT * r),
        'equilibrium':   (bottom + (0.5-EQUILIBRIUM_BAND)*r, bottom + (0.5+EQUILIBRIUM_BAND)*r),
        'premium':       (top - DISCOUNT_PCT * r, top),
    }

def get_estructura(sh: list, sl: list) -> str:
    if len(sh) < 2 or len(sl) < 2:
        return 'Indefinida'
    uh = [v for _, v in sh[-3:]]
    ul = [v for _, v in sl[-3:]]
    hh = all(uh[i] > uh[i-1] for i in range(1, len(uh)))
    hl = all(ul[i] > ul[i-1] for i in range(1, len(ul)))
    lh = all(uh[i] < uh[i-1] for i in range(1, len(uh)))
    ll = all(ul[i] < ul[i-1] for i in range(1, len(ul)))
    if hh and hl: return 'Alcista'
    if lh and ll: return 'Bajista'
    if hh or hl:  return 'Alcista Debil'
    if lh or ll:  return 'Bajista Debil'
    return 'Lateral'

def detect_ob_encima(high: np.ndarray, low: np.ndarray,
                     close: np.ndarray, price: float, sh: list) -> tuple:
    """Detecta OB bajista encima del precio. Corregido para evitar rango vacio."""
    if not sh:
        return False, None
    idx   = sh[-1][0]
    start = min(len(close)-1, idx+3)
    end   = max(1, idx-8)
    if start <= end:          # rango vacio — bug corregido
        return False, None
    for i in range(start, end, -1):
        if i >= len(close) or i < 1:
            continue
        if close[i] > close[i-1]:
            ob_l = float(low[i])
            if ob_l > price and ob_l < price * 1.08:
                return True, round(ob_l, 2)
    return False, None

def detect_fvg_all(high: np.ndarray, low: np.ndarray,
                   close: np.ndarray, price: float, lookback: int = 30) -> list:
    fvgs = []
    n    = min(lookback, len(close) - 3)  # -3 para garantizar idx-1 e idx+1 validos
    for i in range(2, n + 2):
        idx = -i
        try:
            h0 = float(high[idx-1])
            l0 = float(low[idx-1])
            h2 = float(high[idx+1])
            l2 = float(low[idx+1])
        except IndexError:
            continue
        if l2 > h0:
            gap_low  = round(h0, 2)
            gap_high = round(l2, 2)
            if price > gap_low:
                fvgs.append({
                    'tipo': 'BULLISH', 'low': gap_low, 'high': gap_high,
                    'mid':  round((gap_low + gap_high) / 2, 2),
                    'dist_pct': round((gap_low - price) / price * 100, 2),
                    'relacion': 'DEBAJO' if gap_low < price else 'ENCIMA',
                })
        if h2 < l0:
            gap_low  = round(h2, 2)
            gap_high = round(l0, 2)
            if price < gap_high:
                fvgs.append({
                    'tipo': 'BEARISH', 'low': gap_low, 'high': gap_high,
                    'mid':  round((gap_low + gap_high) / 2, 2),
                    'dist_pct': round((gap_high - price) / price * 100, 2),
                    'relacion': 'ENCIMA' if gap_high > price else 'DEBAJO',
                })
    unique = []
    for fvg in fvgs:
        if not any(abs(f['mid'] - fvg['mid']) / max(fvg['mid'], 0.01) < 0.005 for f in unique):
            unique.append(fvg)
    unique.sort(key=lambda x: abs(x['dist_pct']))
    return unique[:6]

def calc_fibonacci_pois(sh: list, sl: list, price: float) -> tuple:
    """
    Calcula Fibonacci desde ultimo impulso alcista valido.
    Corregido: verifica que sl este antes del sh y que el rango sea positivo.
    """
    if not sh or not sl:
        return [], []
    last_sh_idx, last_sh_val = sh[-1]
    # Swing low anterior al ultimo swing high
    sl_antes = [(i, v) for i, v in sl if i < last_sh_idx]
    if not sl_antes:
        return [], []
    imp_low_idx, imp_low = sl_antes[-1]
    imp_high  = last_sh_val
    rango_imp = imp_high - imp_low
    if rango_imp <= 0:
        log.debug(f"Fibonacci: rango invalido ({imp_high:.2f} - {imp_low:.2f})")
        return [], []

    niveles = [
        (0.236, '23.6% Retroceso menor',   False),
        (0.382, '38.2% POI moderado',       False),
        (0.500, '50.0% Equilibrium',        False),
        (0.618, '61.8% Golden Pocket',      True),
        (0.650, '65.0% Golden Pocket ext.', True),
        (0.786, '78.6% Ultimo soporte',     False),
    ]
    retrocesos = []
    for nivel, nombre, es_golden in niveles:
        precio_fib = round(imp_high - (rango_imp * nivel), 2)
        dist       = round((precio_fib - price) / price * 100, 2)
        retrocesos.append({
            'nivel': nivel, 'nombre': nombre,
            'precio': precio_fib, 'dist_pct': dist,
            'zona': 'SOPORTE' if precio_fib < price else 'RESISTENCIA',
            'es_golden': es_golden,
        })

    extensiones = []
    for nivel, nombre in [(1.272,'127.2% Extension 1'),(1.414,'141.4% Extension 2'),(1.618,'161.8% Extension dorada')]:
        precio_ext = round(imp_low + (rango_imp * nivel), 2)
        dist       = round((precio_ext - price) / price * 100, 2)
        extensiones.append({'nivel': nivel, 'nombre': nombre, 'precio': precio_ext, 'dist_pct': dist})

    return retrocesos, extensiones

def detect_absorcion(vol: np.ndarray, close: np.ndarray,
                     high: np.ndarray, low: np.ndarray) -> tuple:
    if len(vol) < 25:
        return False, 0
    avg_vol = float(np.mean(vol[-21:-1]))
    if avg_vol == 0:
        return False, 0
    for lb in range(1, 4):
        vr   = float(vol[-lb]) / avg_vol
        rang = float(high[-lb]) - float(low[-lb])
        if rang == 0:
            continue
        cp = (float(close[-lb]) - float(low[-lb])) / rang
        if vr >= ABSORCION_VOL_RATIO and cp >= 0.70:
            return True, round(vr, 2)
    return False, 0

def detect_squeeze(high: np.ndarray, low: np.ndarray) -> bool:
    if len(high) < 20:
        return False
    rangos = high - low
    r_act  = float(np.mean(rangos[-3:]))
    r_prev = float(np.mean(rangos[-20:-3]))
    if r_prev == 0:
        return False
    return (r_act / r_prev) <= SQUEEZE_RATIO

# ============================================================
# CACHE REFERENCIAS
# ============================================================
log.info('Cargando referencias (SPY + ETFs)...')
ref_data    = {}
sector_cache = {}
for sym in ['SPY'] + list(SECTOR_ETF.values()):
    try:
        df = yf.download(sym, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is not None and len(df) > 20:
            ref_data[sym] = df.dropna()
    except Exception as e:
        log.warning(f"No se pudo cargar referencia {sym}: {e}")
log.info(f'Referencias cargadas: {len(ref_data)}')

def get_sector(ticker: str) -> str:
    if ticker in sector_cache:
        return sector_cache[ticker]
    try:
        s = yf.Ticker(ticker).info.get('sector', 'Unknown')
        sector_cache[ticker] = s
        return s
    except Exception:
        sector_cache[ticker] = 'Unknown'
        return 'Unknown'

# ============================================================
# ANALISIS
# ============================================================
def analyze(ticker: str, es_byma: bool = False) -> dict | None:
    try:
        df = yf.download(ticker, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            log.debug(f"{ticker}: sin datos")
            return None
        if len(df) < SWING_LENGTH + 20:
            log.debug(f"{ticker}: datos insuficientes ({len(df)} velas)")
            return None

        df    = df.dropna()
        h     = to_arr(df['High'])
        l     = to_arr(df['Low'])
        c     = to_arr(df['Close'])
        v     = to_arr(df['Volume'])
        price = float(c[-1])

        sh, sl = find_swings(h, l, SWING_LENGTH)
        if not sh or not sl:
            return None

        top    = max(val for _, val in sh[-5:])
        bottom = min(val for _, val in sl[-5:])
        if top <= bottom:
            return None

        zones = get_zones(top, bottom)
        rango = top - bottom
        pct_r = (price - bottom) / rango * 100

        in_disc = zones['discount'][0]     <= price <= zones['discount'][1]
        in_near = zones['near_discount'][0] <= price <= zones['near_discount'][1]
        if not (in_disc or in_near):
            return None

        zona       = 'Discount' if in_disc else 'Near Discount'
        estructura = get_estructura(sh, sl)
        if ESTRUCTURA_ALCISTA and estructura not in ['Alcista', 'Alcista Debil']:
            return None

        ob_enc, ob_lvl          = detect_ob_encima(h, l, c, price, sh)
        fvgs_all                = detect_fvg_all(h, l, c, price)
        fvg_bull                = any(f['tipo'] == 'BULLISH' for f in fvgs_all)
        fib_retrocesos, fib_ext = calc_fibonacci_pois(sh, sl, price)
        abs_hit, abs_vol        = detect_absorcion(v, c, h, l)
        sq_hit                  = detect_squeeze(h, l)

        rs_hit, rs_ratio = False, None
        if not es_byma:
            sector  = get_sector(ticker)
            etf_sym = SECTOR_ETF.get(sector)
            etf_d   = ref_data.get(etf_sym)
            spy_d   = ref_data.get('SPY')
            if etf_d is not None and spy_d is not None:
                etf_c   = to_arr(etf_d['Close'])
                spy_c   = to_arr(spy_d['Close'])
                min_len = min(len(c), len(etf_c), len(spy_c))
                if min_len > RS_DIAS + 1:
                    ret_t      = float(c[-1])     / float(c[-RS_DIAS])     - 1
                    ret_etf    = float(etf_c[-1]) / float(etf_c[-RS_DIAS]) - 1
                    ret_spy    = float(spy_c[-1]) / float(spy_c[-RS_DIAS]) - 1
                    rs_vs_etf  = (1+ret_t)   / (1+ret_etf)  if (1+ret_etf) != 0  else 0
                    rs_etf_spy = (1+ret_etf) / (1+ret_spy)  if (1+ret_spy) != 0  else 0
                    rs_hit     = rs_vs_etf >= RS_RATIO_MIN and rs_etf_spy >= 0.99
                    rs_ratio   = round(rs_vs_etf, 3)
        else:
            sector = 'Argentina'

        rsi       = round(float(calculate_rsi(c)[-1]), 1)
        avg_vol   = float(np.mean(v[-21:-1]))
        vol_ratio = round(float(v[-1]) / avg_vol, 2) if avg_vol > 0 else 0

        score = 2
        if fvg_bull: score += 1
        if rs_hit:   score += 1
        if abs_hit:  score += 1
        if sq_hit:   score += 1
        if ob_enc:   score -= 1
        if score < SCORE_MINIMO:
            return None

        equil = (top + bottom) / 2
        return {
            'ticker':          ticker,
            'sector':          sector,
            'score':           score,
            'zona':            zona,
            'pct_rango':       round(pct_r, 1),
            'estructura':      estructura,
            'precio':          round(price, 2),
            'swing_high':      round(top, 2),
            'swing_low':       round(bottom, 2),
            'equilibrium':     round(equil, 2),
            'dist_equil':      round((equil - price) / price * 100, 2),
            'ob_encima':       f'SI ({ob_lvl})' if ob_enc else 'NO',
            'ob_enc_bool':     ob_enc,
            'fvg':             'BULLISH' if fvg_bull else ('BEARISH' if fvgs_all else 'None'),
            'fvgs_all':        fvgs_all,
            'fib_retrocesos':  fib_retrocesos,
            'fib_extensiones': fib_ext,
            'rs_ratio':        rs_ratio or '-',
            'rsi':             rsi,
            'vol_ratio':       vol_ratio,
            'abs_vol':         abs_vol or '-',
            'squeeze':         'SI' if sq_hit else 'NO',
            'target':          round(price * (1 + TARGET_PCT/100), 2),
            'stop':            round(price * (1 - STOP_PCT/100), 2),
            'tv_link':         f'https://www.tradingview.com/chart/?symbol={ticker}',
        }

    except Exception as e:
        log.error(f"{ticker}: error en analisis — {e}")
        return None

# ============================================================
# ROTACION SECTORIAL
# ============================================================
def get_rotacion() -> tuple:
    spy_d = ref_data.get('SPY')
    if spy_d is None:
        return [], 0
    spy_c   = to_arr(spy_d['Close'])
    spy_ret = float(spy_c[-1]) / float(spy_c[-RS_DIAS]) - 1
    rows    = []
    for sector, etf in SECTOR_ETF.items():
        if etf not in ref_data:
            log.debug(f"Rotacion: sin datos para {etf}")
            continue
        etf_c   = to_arr(ref_data[etf]['Close'])
        etf_ret = float(etf_c[-1]) / float(etf_c[-RS_DIAS]) - 1
        rs      = (1+etf_ret) / (1+spy_ret) if (1+spy_ret) != 0 else 0
        rows.append({
            'sector': sector, 'etf': etf,
            'ret_5d': round(etf_ret * 100, 2),
            'rs_spy': round(rs, 3),
            'estado': 'ENTRANDO' if rs >= 1.0 else 'saliendo',
        })
    rows.sort(key=lambda x: x['rs_spy'], reverse=True)
    return rows, round(spy_ret * 100, 2)

# ============================================================
# GENERADOR REPORTE
# ============================================================
def generar_reporte(ccl, ccl_fuente, resultados_cedears,
                    resultados_adrs, resultados_byma, rotacion, spy_ret) -> str:
    now   = now_arg().strftime('%d/%m/%Y %H:%M')
    lines = []
    def L(txt=''): lines.append(txt)

    L('=' * 65)
    L('   SMC CONFLUENCE SCREENER — REPORTE DIARIO')
    L(f'   {now} (ARG) — Swing Length: {SWING_LENGTH} (LuxAlgo)')
    L('=' * 65)
    L()
    L(f'  CCL: ${ccl:,.2f} pesos/USD  (fuente: {ccl_fuente})')
    L()
    L('SECCION 1: CONTEXTO DE MERCADO')
    L('-' * 65)
    spy_emoji = 'sube' if spy_ret >= 0 else 'baja'
    L(f'  SPY (ultimos {RS_DIAS} dias): {spy_ret:+.2f}% ({spy_emoji})')
    L()
    L('  ROTACION SECTORIAL:')
    L(f'  {"Sector":<25} {"ETF":<5} {"Ret5d":>7}  {"RS/SPY":>7}  Capital')
    L('  ' + '-' * 55)
    sectores_entrando = []
    for r in rotacion:
        estado_str = 'ENTRANDO [OK]' if r['estado'] == 'ENTRANDO' else 'saliendo [--]'
        L(f'  {r["sector"]:<25} {r["etf"]:<5} {r["ret_5d"]:>+6.2f}%  {r["rs_spy"]:>7.3f}  {estado_str}')
        if r['estado'] == 'ENTRANDO':
            sectores_entrando.append(r['etf'])
    L()
    if sectores_entrando:
        L(f'  CONCLUSION: Capital entrando en {", ".join(sectores_entrando[:3])}')
        L(f'  Operar preferentemente acciones de esos sectores hoy.')
    else:
        L('  CONCLUSION: Mercado defensivo — ser muy selectivo hoy.')

    def format_resultado(r, ccl, es_adr=False, byma_info=None):
        lines_r = []
        medal   = '[*]' if r['zona'] == 'Discount' else '   '
        ob_warn = ' [!]' if r['ob_enc_bool'] else ''
        dc_warn = ' [DOBLE CONFLUENCIA]' if es_adr and byma_info else ''
        lines_r.append(f'  TICKER: {r["ticker"]}  |  Score: {r["score"]}/7  |  {r["sector"]}{dc_warn}')
        lines_r.append(f'  {"─"*60}')
        lines_r.append(f'  Precio:         ${r["precio"]:,.2f}')
        lines_r.append(f'  Zona SMC:       {medal} {r["zona"]}  ({r["pct_rango"]:.1f}% del rango)')
        lines_r.append(f'  Swing High:     ${r["swing_high"]:,.2f}   Swing Low: ${r["swing_low"]:,.2f}')
        lines_r.append(f'  Equilibrium:    ${r["equilibrium"]:,.2f}  (iman +{r["dist_equil"]:.1f}%)')
        lines_r.append(f'  Estructura:     {r["estructura"]}')
        lines_r.append(f'  OB Encima:      {r["ob_encima"]}{ob_warn}')
        lines_r.append(f'  RS vs Sector:   {r["rs_ratio"]}')
        lines_r.append(f'  RSI Diario:     {r["rsi"]}')
        lines_r.append(f'  Vol Ratio 20d:  {r["vol_ratio"]}x')

        fvgs     = r.get('fvgs_all', [])
        fvgs_enc = [f for f in fvgs if f['relacion'] == 'ENCIMA']
        fvgs_deb = [f for f in fvgs if f['relacion'] == 'DEBAJO']
        if fvgs:
            lines_r.append(f'  -- FVGs ACTIVOS --')
            if fvgs_enc:
                lines_r.append(f'  Encima (resistencia):')
                for f in fvgs_enc[:3]:
                    lines_r.append(f'    [{f["tipo"]}] ${f["low"]:,.2f}-${f["high"]:,.2f} mid:${f["mid"]:,.2f} [{f["dist_pct"]:+.1f}%]')
            if fvgs_deb:
                lines_r.append(f'  Debajo (soporte):')
                for f in fvgs_deb[:3]:
                    lines_r.append(f'    [{f["tipo"]}] ${f["low"]:,.2f}-${f["high"]:,.2f} mid:${f["mid"]:,.2f} [{f["dist_pct"]:+.1f}%]')

        fibs_r = r.get('fib_retrocesos', [])
        fibs_e = r.get('fib_extensiones', [])
        if fibs_r:
            lines_r.append(f'  -- FIBONACCI --')
            for f in fibs_r:
                star   = ' [GP]' if f['es_golden'] else ''
                actual = ' <- AQUI' if abs(f['dist_pct']) < 1.0 else ''
                icono  = 'v' if f['zona'] == 'SOPORTE' else '^'
                lines_r.append(f'  [{icono}] {f["nombre"]:<28} ${f["precio"]:>10,.2f} [{f["dist_pct"]:+.1f}%]{star}{actual}')
            for f in fibs_e:
                lines_r.append(f'  [T] {f["nombre"]:<28} ${f["precio"]:>10,.2f} [{f["dist_pct"]:+.1f}%]')

        lines_r.append(f'  -- TRADE --')
        lines_r.append(f'  Entrada: ${r["precio"]:,.2f}  |  Stop: ${r["stop"]:,.2f} (-{STOP_PCT}%)  |  Target: ${r["target"]:,.2f} (+{TARGET_PCT}%)')
        lines_r.append(f'  R/R: {TARGET_PCT/STOP_PCT:.1f}  |  TV: {r["tv_link"]}')

        if ccl and not r['ticker'].endswith('.BA'):
            ratio        = r.get('ratio', 1) or 1
            precio_pesos = (r['precio'] / ratio) * ccl
            stop_pesos   = (r['stop']   / ratio) * ccl
            target_pesos = (r['target'] / ratio) * ccl
            lines_r.append(f'  -- EN PESOS (CCL ${ccl:,.0f} | ratio {ratio}:1) --')
            lines_r.append(f'  Entrada: ${precio_pesos:>12,.0f} ARS  |  Stop: ${stop_pesos:>12,.0f}  |  Target: ${target_pesos:>12,.0f}')

        if es_adr and byma_info:
            lines_r.append(f'  -- BYMA LOCAL --')
            lines_r.append(f'  Ticker: {byma_info["byma"]}  |  1 ADR = {byma_info["ratio"]} acciones  |  {byma_info["nombre"]}')

        lines_r.append(f'  {"─"*60}')
        lines_r.append('')
        return lines_r

    # Seccion 2: CEDEARs
    L(); L('=' * 65)
    L('SECCION 2: CEDEARs — SENALES EN NYSE')
    L(f'  (Operas el CEDEAR en pesos. CCL ${ccl:,.0f})')
    L('-' * 65)
    cedears_sin_ob = [r for r in resultados_cedears if not r['ob_enc_bool']]
    cedears_con_ob = [r for r in resultados_cedears if r['ob_enc_bool']]
    if cedears_sin_ob:
        L(f'\n  TIER 1 — Camino libre ({len(cedears_sin_ob)}):')
        for i, r in enumerate(cedears_sin_ob[:5], 1):
            L(f'\n  #{i}')
            for line in format_resultado(r, ccl): L(line)
    else:
        L('\n  Sin senales Tier 1 hoy.')
    if cedears_con_ob:
        L(f'\n  TIER 2 — Con OB encima ({len(cedears_con_ob)}):')
        for i, r in enumerate(cedears_con_ob[:3], 1):
            L(f'\n  #{i}')
            for line in format_resultado(r, ccl): L(line)

    # Seccion 3: ADRs
    L(); L('=' * 65)
    L('SECCION 3: ADRs ARGENTINOS')
    L('-' * 65)
    adrs_sin_ob = [r for r in resultados_adrs if not r['ob_enc_bool']]
    adrs_con_ob = [r for r in resultados_adrs if r['ob_enc_bool']]
    if adrs_sin_ob:
        for i, r in enumerate(adrs_sin_ob, 1):
            L(f'\n  #{i}')
            for line in format_resultado(r, ccl, es_adr=True, byma_info=ADRS_ARG_NYSE.get(r['ticker'])): L(line)
    else:
        L('\n  Sin senales en ADRs hoy.')
    if adrs_con_ob:
        L(f'\n  ADRs con OB encima:')
        for r in adrs_con_ob:
            L(f'  {r["ticker"]:6} | Score:{r["score"]}/7 | {r["zona"]} | OB: {r["ob_encima"]}')

    # Seccion 4: Panel BYMA
    L(); L('=' * 65)
    L('SECCION 4: PANEL LIDER BYMA — EN PESOS')
    L('-' * 65)
    byma_sin_ob = [r for r in resultados_byma if not r['ob_enc_bool']]
    if byma_sin_ob:
        for i, r in enumerate(byma_sin_ob, 1):
            L(f'\n  #{i}  {r["ticker"]}  ({PANEL_LIDER_BYMA.get(r["ticker"], "")})')
            L(f'  {"─"*60}')
            L(f'  Precio: ${r["precio"]:,.2f} ARS  |  Zona: {r["zona"]}  |  RSI: {r["rsi"]}')
            L(f'  Estructura: {r["estructura"]}  |  Score: {r["score"]}/7')
            L(f'  Equilibrium: ${r["equilibrium"]:,.2f} ARS (+{r["dist_equil"]:.1f}%)')
            L(f'  Entrada: ${r["precio"]:,.2f}  Stop: ${r["stop"]:,.2f}  Target: ${r["target"]:,.2f}')
            L(f'  {"─"*60}')
    else:
        L('\n  Sin senales en Panel Lider BYMA hoy.')

    # Seccion 5: Plan
    L(); L('=' * 65)
    L('SECCION 5: PLAN DEL DIA')
    L('-' * 65)
    todos = sorted(
        resultados_cedears + resultados_adrs + resultados_byma,
        key=lambda x: (x['ob_enc_bool'], -x['score'], x['pct_rango'])
    )
    top3 = [r for r in todos if not r['ob_enc_bool']][:3]
    L()
    L('  HORARIOS (hora Argentina):')
    L('  10:00       Apertura BYMA')
    L('  11:30       Apertura NYSE')
    L('  11:30-12:30 EVITAR — solapamiento Londres/NY')
    L('  13:00-15:00 MEJOR VENTANA')
    L('  17:00-18:00 EVITAR — cierre NY')
    L()
    if top3:
        L('  PRIORIDADES HOY:')
        for i, r in enumerate(top3, 1):
            dc = ' [ADR]' if r['ticker'] in ADRS_ARG_NYSE else ''
            L(f'  {i}. {r["ticker"]:6} | Score:{r["score"]}/7 | {r["zona"]} | RSI:{r["rsi"]} | ${r["precio"]:,.2f}{dc}')
    L()
    L(f'  MAX/TRADE: 25% capital  |  STOP: -{STOP_PCT}%  |  TARGET: +{TARGET_PCT}%  |  R/R: {TARGET_PCT/STOP_PCT:.1f}')
    if not any([resultados_cedears, resultados_adrs, resultados_byma]):
        L(); L('  SIN SENALES HOY. El capital que no se pierde, no necesita recuperarse.')
    L()
    L('=' * 65)
    L(f'  Generado: {now} ARG  |  Swing:{SWING_LENGTH}  |  Score min:{SCORE_MINIMO}')
    L('=' * 65)
    return '\n'.join(lines)

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    inicio = now_arg()
    log.info('=' * 65)
    log.info('SMC REPORTE DIARIO — Argentina Edition')
    log.info(f'{inicio.strftime("%d/%m/%Y %H:%M")} ARG')
    log.info('=' * 65)

    # Secrets de GitHub Actions (o variables de entorno locales)
    TELEGRAM_TOKEN   = os.environ.get('TELEGRAM_TOKEN', '')
    TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

    # 0. Cargar ratios
    log.info('Cargando ratios CEDEARs...')
    _cargar_ratios()

    tickers_cedears = CEDEARS_NYSE
    tickers_adrs    = list(ADRS_ARG_NYSE.keys())
    log.info(f'Universo: {len(tickers_cedears)} CEDEARs + {len(tickers_adrs)} ADRs + {len(PANEL_LIDER_BYMA)} BYMA')

    # 1. CCL
    log.info('Obteniendo CCL...')
    ccl, ccl_fuente = get_ccl()
    log.info(f'CCL: ${ccl:,.2f} ARS/USD  ({ccl_fuente})')

    # 2. Rotacion
    log.info('Calculando rotacion sectorial...')
    rotacion, spy_ret = get_rotacion()

    # 3. Scan CEDEARs
    log.info(f'Escaneando {len(tickers_cedears)} CEDEARs...')
    resultados_cedears = []
    for i, t in enumerate(tickers_cedears):
        r = analyze(t, es_byma=False)
        if r:
            ratio = get_ratio(t)
            r['ratio']      = ratio
            r['precio_ars'] = round((r['precio'] / ratio) * ccl, 0)
            r['stop_ars']   = round((r['stop']   / ratio) * ccl, 0)
            r['target_ars'] = round((r['target'] / ratio) * ccl, 0)
            resultados_cedears.append(r)
            log.info(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ratio:{ratio} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    # 4. Scan ADRs
    log.info(f'Escaneando {len(tickers_adrs)} ADRs...')
    resultados_adrs = []
    for i, t in enumerate(tickers_adrs):
        r = analyze(t, es_byma=False)
        if r:
            ratio = get_ratio(t)
            r['ratio']      = ratio
            r['byma_local'] = ADRS_ARG_NYSE[t]['byma']
            r['precio_ars'] = round((r['precio'] / ratio) * ccl, 0)
            r['stop_ars']   = round((r['stop']   / ratio) * ccl, 0)
            r['target_ars'] = round((r['target'] / ratio) * ccl, 0)
            resultados_adrs.append(r)
            log.info(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    # 5. Scan BYMA
    log.info(f'Escaneando {len(PANEL_LIDER_BYMA)} BYMA...')
    resultados_byma = []
    for i, t in enumerate(PANEL_LIDER_BYMA.keys()):
        r = analyze(t, es_byma=True)
        if r:
            r['ratio'] = 1
            r['precio_ars'] = r['precio']
            resultados_byma.append(r)
            log.info(f'  HIT {t:10} | Score:{r["score"]}/7 | {r["zona"]}')
        if (i+1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    for lst in [resultados_cedears, resultados_adrs, resultados_byma]:
        lst.sort(key=lambda x: (-x['score'], x['ob_enc_bool'], x['pct_rango']))

    # 6. Generar reporte
    log.info('Generando reporte...')
    reporte = generar_reporte(
        ccl, ccl_fuente,
        resultados_cedears, resultados_adrs, resultados_byma,
        rotacion, spy_ret
    )

    # 7. Guardar
    os.makedirs('results', exist_ok=True)
    date_str     = inicio.strftime('%Y%m%d_%H%M')
    fname_dated  = f'results/reporte_{date_str}.txt'
    fname_latest = 'results/reporte_latest.txt'
    with open(fname_dated,  'w', encoding='utf-8') as f: f.write(reporte)
    with open(fname_latest, 'w', encoding='utf-8') as f: f.write(reporte)
    log.info(f'Reporte guardado: {fname_dated}')

    todos = resultados_cedears + resultados_adrs + resultados_byma
    if todos:
        cols = ['ticker','score','zona','precio','ratio','precio_ars','stop_ars','target_ars','rsi','estructura']
        df   = pd.DataFrame(todos)
        df[[c for c in cols if c in df.columns]].to_csv(f'results/confluence_{date_str}.csv', index=False)

    # 8. Enviar a Telegram
    total_hits = len(resultados_cedears) + len(resultados_adrs) + len(resultados_byma)
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        log.info(f'Enviando a Telegram ({total_hits} senales)...')
        msg_tg = formatear_reporte_telegram(reporte, total_hits, ccl)
        ok     = enviar_telegram(msg_tg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        if ok:
            log.info('Telegram: enviado OK')
        else:
            log.error('Telegram: fallo el envio')
    else:
        log.warning('Telegram no configurado (secrets TELEGRAM_TOKEN / TELEGRAM_CHAT_ID ausentes)')

    print(reporte)
    elapsed = (now_arg() - inicio).seconds
    log.info(f'Tiempo total: {elapsed}s')
