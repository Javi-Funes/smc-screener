"""
SMC REPORTE DIARIO — Argentina Edition
=======================================
Universo: CEDEARs liquidos + Panel Lider BYMA + ADRs argentinos
CCL: CriptoYa (primario) + DolarAPI (fallback)
Swing Length: 50 (calibrado igual que LuxAlgo TradingView)
Horario: Corre a las 7AM ARG — reporte listo antes de la apertura
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import warnings
import time
import os
import json
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================
# === PARAMETROS AJUSTABLES ===
# ============================================================
SWING_LENGTH        = 50       # Igual que LuxAlgo en TradingView
DISCOUNT_PCT        = 0.25     # 25% inferior del rango
NEAR_DISCOUNT_PCT   = 0.40     # Hasta 40% del rango
EQUILIBRIUM_BAND    = 0.05     # 50% +/- 5%
ESTRUCTURA_ALCISTA  = True     # Solo HH + HL
RS_DIAS             = 5        # Ventana Relative Strength
RS_RATIO_MIN        = 1.02     # 2% mejor que su sector
ABSORCION_VOL_RATIO = 2.5      # Volumen 2.5x promedio
SQUEEZE_RATIO       = 0.65     # Compresion de volatilidad
SCORE_MINIMO        = 3        # Minimo señales (max 7)
TARGET_PCT          = 3.0      # Target %
STOP_PCT            = 1.5      # Stop %
DATA_PERIOD         = '1y'
DATA_PERIOD_H       = '1mo'
BATCH_SIZE          = 10
SLEEP_BETWEEN       = 1.5
CCL_FALLBACK        = 1200.0   # Solo si ambas APIs fallan

# ============================================================
# UNIVERSOS
# ============================================================

# CEDEARs con volumen real en Argentina
# Ticker NYSE → para analisis SMC (precio real, sin delay)
CEDEARS_NYSE = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'AMD',
    'MELI', 'GLOB', 'BABA', 'DIS', 'KO', 'PEP', 'JPM', 'WMT',
    'GOLD', 'NEM', 'XOM', 'CVX', 'BA', 'GE', 'IBM', 'CSCO',
    'INTC', 'QCOM', 'AVGO', 'CRM', 'ADBE', 'ORCL', 'UBER', 'ABNB',
    'COST', 'SBUX', 'NKE', 'MCD', 'V', 'MA', 'PYPL', 'SQ',
    'COIN', 'SHOP', 'NFLX', 'VALE', 'FCX', 'X',
]

# ADRs argentinos en NYSE — los mas importantes
# Tienen doble cotizacion: NYSE (analisis) + BYMA en pesos (ejecucion)
ADRS_ARG_NYSE = {
    'GGAL':  {'byma': 'GGAL.BA',  'nombre': 'Grupo Galicia',    'ratio': 10},
    'YPF':   {'byma': 'YPFD.BA',  'nombre': 'YPF',              'ratio': 1},
    'PAM':   {'byma': 'PAMP.BA',  'nombre': 'Pampa Energia',    'ratio': 25},
    'BMA':   {'byma': 'BMA.BA',   'nombre': 'Banco Macro',      'ratio': 10},
    'CEPU':  {'byma': 'CEPU.BA',  'nombre': 'Central Puerto',   'ratio': 10},
    'LOMA':  {'byma': 'LOMA.BA',  'nombre': 'Loma Negra',       'ratio': 5},
    'SUPV':  {'byma': 'SUPV.BA',  'nombre': 'Supervielle',      'ratio': 5},
    'TGS':   {'byma': 'TGSU2.BA', 'nombre': 'TGS',              'ratio': 5},
    'MELI':  {'byma': 'MELI.BA',  'nombre': 'MercadoLibre',     'ratio': 1},
    'GLOB':  {'byma': 'GLOB.BA',  'nombre': 'Globant',          'ratio': 1},
    'DESP':  {'byma': 'DESP.BA',  'nombre': 'Despegar',         'ratio': 5},
}

# Panel Lider BYMA — solo pesos, sin ADR en NYSE
# Usamos tickers .BA directamente
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

# ETFs sectoriales para RS
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
# CCL — CriptoYa + DolarAPI fallback
# ============================================================

def get_ccl():
    """
    Obtiene el CCL en tiempo real.
    Primero intenta CriptoYa, luego DolarAPI, luego fallback manual.
    """
    # Intento 1: CriptoYa
    try:
        r = requests.get('https://criptoya.com/api/dolar', timeout=5)
        if r.status_code == 200:
            data = r.json()
            ccl  = data.get('ccl', {})
            venta = ccl.get('ask') or ccl.get('venta') or ccl.get('price')
            if venta and float(venta) > 100:
                return float(venta), 'CriptoYa'
    except Exception:
        pass

    # Intento 2: DolarAPI
    try:
        r = requests.get(
            'https://dolarapi.com/v1/dolares/contadoconliqui', timeout=5)
        if r.status_code == 200:
            data  = r.json()
            venta = data.get('venta')
            if venta and float(venta) > 100:
                return float(venta), 'DolarAPI'
    except Exception:
        pass

    # Fallback manual
    return CCL_FALLBACK, 'FALLBACK (manual)'

# ============================================================
# FUNCIONES SMC
# ============================================================

def to_arr(s):
    return np.array(s).flatten()

def calculate_rsi(arr, period=14):
    s     = pd.Series(arr)
    delta = s.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, min_periods=period).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, min_periods=period).mean()
    rs    = gain / loss
    return to_arr(100 - (100 / (1 + rs)))

def find_swings(high, low, length):
    """Replica leg() de LuxAlgo con swing length configurable"""
    sh, sl = [], []
    for i in range(length, len(high)):
        if high[i-length] == max(high[i-length:i]):
            sh.append((i-length, float(high[i-length])))
        if low[i-length] == min(low[i-length:i]):
            sl.append((i-length, float(low[i-length])))
    return sh, sl

def get_zones(top, bottom):
    r = top - bottom
    return {
        'discount':      (bottom,                          bottom + DISCOUNT_PCT * r),
        'near_discount': (bottom,                          bottom + NEAR_DISCOUNT_PCT * r),
        'equilibrium':   (bottom + (0.5-EQUILIBRIUM_BAND)*r, bottom + (0.5+EQUILIBRIUM_BAND)*r),
        'premium':       (top    - DISCOUNT_PCT * r,       top),
    }

def get_estructura(sh, sl):
    if len(sh) < 2 or len(sl) < 2:
        return 'Indefinida'
    uh = [v for _, v in sh[-3:]]
    ul = [v for _, v in sl[-3:]]
    hh = all(uh[i] > uh[i-1] for i in range(1, len(uh)))
    hl = all(ul[i] > ul[i-1] for i in range(1, len(ul)))
    lh = all(uh[i] < uh[i-1] for i in range(1, len(uh)))
    ll = all(ul[i] < ul[i-1] for i in range(1, len(ul)))
    if hh and hl:   return 'Alcista'
    if lh and ll:   return 'Bajista'
    if hh or hl:    return 'Alcista Debil'
    if lh or ll:    return 'Bajista Debil'
    return 'Lateral'

def detect_ob_encima(high, low, close, price, sh):
    """Detecta OB bajista inmediatamente encima del precio actual"""
    if not sh:
        return False, None
    idx = sh[-1][0]
    for i in range(min(len(close)-1, idx+3), max(1, idx-8), -1):
        if close[i] > close[i-1]:
            ob_l = float(low[i])
            if ob_l > price and ob_l < price * 1.08:
                return True, round(ob_l, 2)
    return False, None

def detect_fvg_all(high, low, close, price, lookback=30):
    """
    Detecta TODOS los FVGs activos en las ultimas N velas.
    Un FVG se considera 'activo' si el precio no lo ha llenado todavia.
    Retorna lista de dicts con tipo, zona y distancia al precio.
    """
    fvgs = []
    n = min(lookback, len(close) - 2)
    for i in range(2, n + 2):
        idx = -(i)  # vela central del patron de 3
        try:
            h0 = float(high[idx-1])   # vela izquierda
            l0 = float(low[idx-1])
            h2 = float(high[idx+1])   # vela derecha
            l2 = float(low[idx+1])
        except IndexError:
            continue

        # Bullish FVG: gap entre high de vela izquierda y low de vela derecha
        if l2 > h0:
            gap_low  = round(h0, 2)
            gap_high = round(l2, 2)
            # Solo si el precio no llenó el gap
            if price > gap_low:
                dist_pct = round((gap_low - price) / price * 100, 2)
                fvgs.append({
                    'tipo':     'BULLISH',
                    'low':      gap_low,
                    'high':     gap_high,
                    'mid':      round((gap_low + gap_high) / 2, 2),
                    'dist_pct': dist_pct,
                    'relacion': 'DEBAJO' if gap_low < price else 'ENCIMA',
                })

        # Bearish FVG: gap entre low de vela izquierda y high de vela derecha
        if h2 < l0:
            gap_low  = round(h2, 2)
            gap_high = round(l0, 2)
            if price < gap_high:
                dist_pct = round((gap_high - price) / price * 100, 2)
                fvgs.append({
                    'tipo':     'BEARISH',
                    'low':      gap_low,
                    'high':     gap_high,
                    'mid':      round((gap_low + gap_high) / 2, 2),
                    'dist_pct': dist_pct,
                    'relacion': 'ENCIMA' if gap_high > price else 'DEBAJO',
                })

    # Eliminar duplicados por nivel similar (diferencia < 0.5%)
    unique = []
    for fvg in fvgs:
        es_dup = any(
            abs(f['mid'] - fvg['mid']) / fvg['mid'] < 0.005
            for f in unique
        )
        if not es_dup:
            unique.append(fvg)

    # Ordenar: primero los más cercanos al precio
    unique.sort(key=lambda x: abs(x['dist_pct']))
    return unique[:6]  # max 6 FVGs en el reporte


def calc_fibonacci_pois(sh, sl, price):
    """
    Calcula niveles Fibonacci desde el ultimo impulso relevante.
    Usa el ultimo swing low -> swing high para retrocesos (compra en pullback).
    Usa el ultimo swing high -> swing low para extensiones (targets).
    Incluye: 0.236, 0.382, 0.5, 0.618, 0.65, 0.786
    """
    if not sh or not sl:
        return [], []

    # --- RETROCESOS (zonas de compra) ---
    # Tomar el ultimo impulso alcista: swing low reciente → swing high reciente
    # Buscamos el SL mas reciente que este POR DEBAJO del ultimo SH
    last_sh_idx, last_sh_val = sh[-1]
    # Encontrar el SL anterior al ultimo SH
    sl_antes = [(i, v) for i, v in sl if i < last_sh_idx]
    if not sl_antes:
        return [], []

    imp_low_idx, imp_low = sl_antes[-1]
    imp_high = last_sh_val
    rango_imp = imp_high - imp_low

    niveles_fib = [0.236, 0.382, 0.5, 0.618, 0.65, 0.786]
    nombres_fib = {
        0.236: '23.6% — Retroceso menor',
        0.382: '38.2% — POI moderado',
        0.500: '50.0% — Equilibrium',
        0.618: '61.8% — Golden Pocket ⭐',
        0.650: '65.0% — Golden Pocket ext.',
        0.786: '78.6% — Ultimo soporte',
    }

    retrocesos = []
    for nivel in niveles_fib:
        precio_fib = round(imp_high - (rango_imp * nivel), 2)
        dist       = round((precio_fib - price) / price * 100, 2)
        zona       = 'SOPORTE' if precio_fib < price else 'RESISTENCIA'
        retrocesos.append({
            'nivel':      nivel,
            'nombre':     nombres_fib[nivel],
            'precio':     precio_fib,
            'dist_pct':   dist,
            'zona':       zona,
            'es_golden':  nivel in [0.618, 0.65],
        })

    # --- EXTENSIONES (targets de precio) ---
    # Desde el ultimo SL hacia arriba: 1.272, 1.414, 1.618
    nombres_ext = {
        1.272: '127.2% — Extension 1',
        1.414: '141.4% — Extension 2',
        1.618: '161.8% — Extension dorada ⭐',
    }
    extensiones = []
    for nivel in [1.272, 1.414, 1.618]:
        precio_ext = round(imp_low + (rango_imp * nivel), 2)
        dist       = round((precio_ext - price) / price * 100, 2)
        extensiones.append({
            'nivel':    nivel,
            'nombre':   nombres_ext[nivel],
            'precio':   precio_ext,
            'dist_pct': dist,
        })

    return retrocesos, extensiones

def detect_absorcion(vol, close, high, low):
    """Absorcion institucional: vela con volumen anomalo + cierre fuerte"""
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
        cp   = (float(close[-lb]) - float(low[-lb])) / rang
        if vr >= ABSORCION_VOL_RATIO and cp >= 0.70:
            return True, round(vr, 2)
    return False, 0

def detect_squeeze(high, low):
    """Compresion de volatilidad — explosion inminente"""
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
print('Cargando referencias (SPY + ETFs)...')
ref_data = {}
for sym in ['SPY'] + list(SECTOR_ETF.values()):
    try:
        df = yf.download(sym, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is not None and len(df) > 20:
            ref_data[sym] = df.dropna()
    except Exception:
        pass
print(f'  OK: {len(ref_data)} referencias cargadas')

sector_cache = {}
def get_sector(ticker):
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
# ANALISIS POR TICKER
# ============================================================

def analyze(ticker, es_byma=False):
    """
    Analisis SMC completo para un ticker.
    es_byma=True para tickers .BA (pesos argentinos)
    """
    try:
        df = yf.download(ticker, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < SWING_LENGTH + 20:
            return None

        df    = df.dropna()
        h     = to_arr(df['High'])
        l     = to_arr(df['Low'])
        c     = to_arr(df['Close'])
        v     = to_arr(df['Volume'])
        price = float(c[-1])

        # ── 1. Zonas SMC ───────────────────────────────────────
        sh, sl = find_swings(h, l, SWING_LENGTH)
        if not sh or not sl:
            return None

        top    = max(val for _, val in sh[-5:])
        bottom = min(val for _, val in sl[-5:])
        if top == bottom:
            return None

        zones  = get_zones(top, bottom)
        rango  = top - bottom
        pct_r  = (price - bottom) / rango * 100

        in_disc = zones['discount'][0]      <= price <= zones['discount'][1]
        in_near = zones['near_discount'][0]  <= price <= zones['near_discount'][1]

        if not (in_disc or in_near):
            return None  # No esta en zona de valor → descartado

        zona = 'Discount' if in_disc else 'Near Discount'

        # ── 2. Estructura ──────────────────────────────────────
        estructura = get_estructura(sh, sl)
        if ESTRUCTURA_ALCISTA and estructura not in ['Alcista', 'Alcista Debil']:
            return None  # Markdown → descartado

        # ── 3. OB de oferta encima ─────────────────────────────
        ob_enc, ob_lvl = detect_ob_encima(h, l, c, price, sh)

        # ── 4. FVG — todos los activos ─────────────────────────
        fvgs_all = detect_fvg_all(h, l, c, price)
        fvg_bull  = any(f['tipo'] == 'BULLISH' for f in fvgs_all)
        fvg_label = 'BULLISH' if fvg_bull else ('BEARISH' if fvgs_all else None)

        # ── 4b. Fibonacci POIs ─────────────────────────────────
        fib_retrocesos, fib_extensiones = calc_fibonacci_pois(sh, sl, price)

        # ── 5. Absorcion ───────────────────────────────────────
        abs_hit, abs_vol = detect_absorcion(v, c, h, l)

        # ── 6. Squeeze ─────────────────────────────────────────
        sq_hit = detect_squeeze(h, l)

        # ── 7. RS Sectorial (solo para tickers NYSE) ───────────
        rs_hit    = False
        rs_ratio  = None
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
                    ret_t   = float(c[-1])     / float(c[-RS_DIAS])     - 1
                    ret_etf = float(etf_c[-1]) / float(etf_c[-RS_DIAS]) - 1
                    ret_spy = float(spy_c[-1]) / float(spy_c[-RS_DIAS]) - 1
                    rs_vs_etf  = (1+ret_t)   / (1+ret_etf) if (1+ret_etf) != 0 else 0
                    rs_etf_spy = (1+ret_etf) / (1+ret_spy) if (1+ret_spy) != 0 else 0
                    rs_hit   = rs_vs_etf >= RS_RATIO_MIN and rs_etf_spy >= 0.99
                    rs_ratio = round(rs_vs_etf, 3)
        else:
            sector = 'Argentina'

        # ── RSI y Volumen ──────────────────────────────────────
        rsi       = round(float(calculate_rsi(c)[-1]), 1)
        avg_vol   = float(np.mean(v[-21:-1]))
        vol_ratio = round(float(v[-1]) / avg_vol, 2) if avg_vol > 0 else 0

        # ── SCORE ──────────────────────────────────────────────
        score = 2  # base: zona + estructura
        if fvg_bull:   score += 1
        if rs_hit:     score += 1
        if abs_hit:    score += 1
        if sq_hit:     score += 1
        if ob_enc:     score -= 1  # penalizacion

        if score < SCORE_MINIMO:
            return None

        # Señales como lista
        senales = [zona, estructura]
        if fvg_bull:   senales.append('FVG-Bull')
        if rs_hit:     senales.append(f'RS({rs_ratio})')
        if abs_hit:    senales.append(f'Absorcion({abs_vol}x)')
        if sq_hit:     senales.append('Squeeze')
        if ob_enc:     senales.append(f'OB-ENCIMA({ob_lvl})')

        equil = (top + bottom) / 2

        return {
            'ticker':           ticker,
            'sector':           sector,
            'score':            score,
            'zona':             zona,
            'pct_rango':        round(pct_r, 1),
            'estructura':       estructura,
            'precio':           round(price, 2),
            'swing_high':       round(top, 2),
            'swing_low':        round(bottom, 2),
            'equilibrium':      round(equil, 2),
            'dist_equil':       round((equil - price) / price * 100, 2),
            'ob_encima':        f'SI ({ob_lvl})' if ob_enc else 'NO',
            'ob_enc_bool':      ob_enc,
            'fvg':              fvg_label or 'None',
            'fvgs_all':         fvgs_all,
            'fib_retrocesos':   fib_retrocesos,
            'fib_extensiones':  fib_extensiones,
            'rs_ratio':         rs_ratio or '-',
            'rsi':              rsi,
            'vol_ratio':        vol_ratio,
            'abs_vol':          abs_vol or '-',
            'squeeze':          'SI' if sq_hit else 'NO',
            'senales':          ' | '.join(senales),
            'target':           round(price * (1 + TARGET_PCT/100), 2),
            'stop':             round(price * (1 - STOP_PCT/100), 2),
            'tv_link':          f'https://www.tradingview.com/chart/?symbol={ticker}',
        }

    except Exception as e:
        return None

# ============================================================
# ROTACION SECTORIAL
# ============================================================

def get_rotacion():
    spy_d = ref_data.get('SPY')
    if spy_d is None:
        return [], 0
    spy_c   = to_arr(spy_d['Close'])
    spy_ret = float(spy_c[-1]) / float(spy_c[-RS_DIAS]) - 1

    rows = []
    for sector, etf in SECTOR_ETF.items():
        if etf in ref_data:
            etf_c   = to_arr(ref_data[etf]['Close'])
            etf_ret = float(etf_c[-1]) / float(etf_c[-RS_DIAS]) - 1
            rs      = (1+etf_ret) / (1+spy_ret) if (1+spy_ret) != 0 else 0
            rows.append({
                'sector':  sector,
                'etf':     etf,
                'ret_5d':  round(etf_ret * 100, 2),
                'rs_spy':  round(rs, 3),
                'estado':  'ENTRANDO' if rs >= 1.0 else 'saliendo',
            })

    rows.sort(key=lambda x: x['rs_spy'], reverse=True)
    return rows, round(spy_ret * 100, 2)

# ============================================================
# GENERADOR DEL REPORTE
# ============================================================

def generar_reporte(ccl, ccl_fuente, resultados_cedears,
                    resultados_adrs, resultados_byma,
                    rotacion, spy_ret):

    now   = datetime.now().strftime('%d/%m/%Y %H:%M')
    lines = []

    def L(txt=''):
        lines.append(txt)

    L('=' * 65)
    L('   SMC CONFLUENCE SCREENER — REPORTE DIARIO')
    L(f'   {now} (ARG) — Swing Length: {SWING_LENGTH} (LuxAlgo)')
    L('=' * 65)

    # ── CCL ──────────────────────────────────────────────────
    L()
    L(f'  CCL: ${ccl:,.2f} pesos/USD  (fuente: {ccl_fuente})')

    # ── Contexto de mercado ───────────────────────────────────
    L()
    L('SECCION 1: CONTEXTO DE MERCADO')
    L('-' * 65)
    spy_emoji = '📈' if spy_ret >= 0 else '📉'
    L(f'  SPY (ultimos {RS_DIAS} dias): {spy_ret:+.2f}% {spy_emoji}')
    L()
    L('  ROTACION SECTORIAL:')
    L(f'  {"Sector":<25} {"ETF":<5} {"Ret5d":>7}  {"RS/SPY":>7}  Capital')
    L('  ' + '-' * 55)

    sectores_entrando = []
    for r in rotacion:
        estado_str = 'ENTRANDO ✅' if r['estado'] == 'ENTRANDO' else 'saliendo ❌'
        L(f'  {r["sector"]:<25} {r["etf"]:<5} {r["ret_5d"]:>+6.2f}%  {r["rs_spy"]:>7.3f}  {estado_str}')
        if r['estado'] == 'ENTRANDO':
            sectores_entrando.append(r['etf'])

    if sectores_entrando:
        L()
        L(f'  CONCLUSION: Capital entrando en {", ".join(sectores_entrando[:3])}')
        L(f'  Operar preferentemente acciones de esos sectores hoy.')
    else:
        L()
        L('  CONCLUSION: Mercado en modo defensivo — ser muy selectivo hoy.')

    # ── Funcion para formatear un resultado ───────────────────
    def format_resultado(r, ccl, es_adr=False, byma_info=None):
        lines_r = []
        medal   = '⭐' if r['zona'] == 'Discount' else '  '
        ob_warn = ' ⚠' if r['ob_enc_bool'] else ''
        dc_warn = '⭐⭐ DOBLE CONFLUENCIA' if es_adr and byma_info else ''
        etiqueta = r['ticker'] if not r['ticker'].endswith('.BA') else r['ticker']

        lines_r.append(f'  TICKER: {etiqueta}  |  Score: {r["score"]}/7  |  {r["sector"]}  {dc_warn}')
        lines_r.append(f'  {"─"*60}')
        lines_r.append(f'  Precio:            ${r["precio"]:,.2f}')
        lines_r.append(f'  Zona SMC:          {medal} {r["zona"]}  ({r["pct_rango"]:.1f}% del rango)')
        lines_r.append(f'  Swing High:        ${r["swing_high"]:,.2f}   Swing Low: ${r["swing_low"]:,.2f}')
        lines_r.append(f'  Equilibrium:       ${r["equilibrium"]:,.2f}  (imán → +{r["dist_equil"]:.1f}%)')
        lines_r.append(f'  Estructura:        {r["estructura"]}')
        lines_r.append(f'  OB Encima:         {r["ob_encima"]}{ob_warn}')
        lines_r.append(f'  RS vs Sector:      {r["rs_ratio"]}')
        lines_r.append(f'  Absorcion:         {"SI (" + str(r["abs_vol"]) + "x)" if r["abs_vol"] != "-" else "NO"}')
        lines_r.append(f'  Squeeze:           {r["squeeze"]}')
        lines_r.append(f'  RSI Diario:        {r["rsi"]}')
        lines_r.append(f'  Vol Ratio 20d:     {r["vol_ratio"]}x')

        # ── FVGs con niveles exactos ───────────────────────────
        fvgs = r.get('fvgs_all', [])
        if fvgs:
            lines_r.append(f'  {"─"*25} FVGs ACTIVOS {"─"*22}')
            fvgs_enc   = [f for f in fvgs if f['relacion'] == 'ENCIMA']
            fvgs_deb   = [f for f in fvgs if f['relacion'] == 'DEBAJO']

            if fvgs_enc:
                lines_r.append(f'  FVGs ENCIMA (resistencia / targets):')
                for f in fvgs_enc[:3]:
                    tipo_icono = '🔴' if f['tipo'] == 'BEARISH' else '🟢'
                    lines_r.append(
                        f'    {tipo_icono} {f["tipo"]:7}  ${f["low"]:,.2f} — ${f["high"]:,.2f}'
                        f'  (mid: ${f["mid"]:,.2f})  [{f["dist_pct"]:+.1f}%]'
                    )
            if fvgs_deb:
                lines_r.append(f'  FVGs DEBAJO (soporte / ordenes de compra):')
                for f in fvgs_deb[:3]:
                    tipo_icono = '🟢' if f['tipo'] == 'BULLISH' else '🔴'
                    lines_r.append(
                        f'    {tipo_icono} {f["tipo"]:7}  ${f["low"]:,.2f} — ${f["high"]:,.2f}'
                        f'  (mid: ${f["mid"]:,.2f})  [{f["dist_pct"]:+.1f}%]'
                    )
        else:
            lines_r.append(f'  FVG:               Sin gaps activos recientes')

        # ── Fibonacci POIs ─────────────────────────────────────
        fibs_r = r.get('fib_retrocesos', [])
        fibs_e = r.get('fib_extensiones', [])
        if fibs_r:
            lines_r.append(f'  {"─"*22} FIBONACCI POIs {"─"*23}')
            lines_r.append(f'  Retrocesos (zonas de compra):')
            for f in fibs_r:
                star    = ' ⭐' if f['es_golden'] else ''
                actual  = ' ← PRECIO AQUI' if abs(f['dist_pct']) < 1.0 else ''
                zona_ic = '🟢' if f['zona'] == 'SOPORTE' else '🔴'
                lines_r.append(
                    f'    {zona_ic} {f["nombre"]:<30}  ${f["precio"]:>10,.2f}  [{f["dist_pct"]:+.1f}%]{star}{actual}'
                )
            lines_r.append(f'  Extensiones (targets de precio):')
            for f in fibs_e:
                lines_r.append(
                    f'    🎯 {f["nombre"]:<30}  ${f["precio"]:>10,.2f}  [{f["dist_pct"]:+.1f}%]'
                )

        # ── Trade ──────────────────────────────────────────────
        lines_r.append(f'  {"─"*35} TRADE {"─"*18}')
        lines_r.append(f'  Entrada:           ${r["precio"]:,.2f}')
        lines_r.append(f'  Stop:              ${r["stop"]:,.2f}  (-{STOP_PCT}%)')
        lines_r.append(f'  Target:            ${r["target"]:,.2f}  (+{TARGET_PCT}%)')
        lines_r.append(f'  R/R:               {TARGET_PCT/STOP_PCT:.1f}')
        lines_r.append(f'  Ver en TV:         {r["tv_link"]}')

        # CEDEAR en pesos via CCL
        if ccl and not r['ticker'].endswith('.BA'):
            precio_pesos = r['precio'] * ccl
            stop_pesos   = r['stop']   * ccl
            target_pesos = r['target'] * ccl
            lines_r.append(f'  {"─"*35} CEDEAR EN PESOS {"─"*9}')
            lines_r.append(f'  CCL utilizado:     ${ccl:,.2f} ARS/USD')
            lines_r.append(f'  Entrada aprox:     ${precio_pesos:>12,.0f} ARS')
            lines_r.append(f'  Stop aprox:        ${stop_pesos:>12,.0f} ARS')
            lines_r.append(f'  Target aprox:      ${target_pesos:>12,.0f} ARS')

        # Info ADR argentino
        if es_adr and byma_info:
            lines_r.append(f'  {"─"*35} DOBLE CONFLUENCIA {"─"*7}')
            lines_r.append(f'  BYMA local:        {byma_info["byma"]}  ({byma_info["nombre"]})')
            lines_r.append(f'  Ratio conversion:  1 ADR = {byma_info["ratio"]} acciones locales')
            lines_r.append(f'  NOTA: Señal confirmada tanto en NYSE como en BYMA')

        lines_r.append(f'  {"─"*60}')
        lines_r.append('')
        return lines_r

    # ── SECCION 2: CEDEARs NYSE ───────────────────────────────
    L()
    L('=' * 65)
    L('SECCION 2: CEDEARs — SEÑALES EN NYSE')
    L(f'  (Operás el CEDEAR en pesos. Precio referencia: CCL ${ccl:,.0f})')
    L('-' * 65)

    cedears_sin_ob  = [r for r in resultados_cedears if not r['ob_enc_bool']]
    cedears_con_ob  = [r for r in resultados_cedears if r['ob_enc_bool']]

    if cedears_sin_ob:
        L(f'\n  TIER 1 — Camino libre ({len(cedears_sin_ob)} acciones):')
        for i, r in enumerate(cedears_sin_ob[:5], 1):
            L(f'\n  #{i}')
            for line in format_resultado(r, ccl):
                L(line)
    else:
        L('\n  Sin señales Tier 1 hoy en CEDEARs.')

    if cedears_con_ob:
        L(f'\n  TIER 2 — Con OB encima, mayor riesgo ({len(cedears_con_ob)} acciones):')
        for i, r in enumerate(cedears_con_ob[:3], 1):
            L(f'\n  #{i}')
            for line in format_resultado(r, ccl):
                L(line)

    # ── SECCION 3: ADRs Argentinos ────────────────────────────
    L()
    L('=' * 65)
    L('SECCION 3: ADRs ARGENTINOS — NYSE + BYMA')
    L('  (Doble confluencia = señal en NY confirmada en mercado local)')
    L('-' * 65)

    adrs_sin_ob = [r for r in resultados_adrs if not r['ob_enc_bool']]
    adrs_con_ob = [r for r in resultados_adrs if r['ob_enc_bool']]

    if adrs_sin_ob:
        for i, r in enumerate(adrs_sin_ob, 1):
            byma_info = ADRS_ARG_NYSE.get(r['ticker'])
            L(f'\n  #{i}')
            for line in format_resultado(r, ccl, es_adr=True, byma_info=byma_info):
                L(line)
    else:
        L('\n  Sin señales en ADRs argentinos hoy.')

    if adrs_con_ob:
        L(f'\n  ADRs con OB encima ({len(adrs_con_ob)}):')
        for r in adrs_con_ob:
            L(f'  {r["ticker"]:6} | Score:{r["score"]}/7 | {r["zona"]} | OB: {r["ob_encima"]}')

    # ── SECCION 4: Panel Lider BYMA ───────────────────────────
    L()
    L('=' * 65)
    L('SECCION 4: PANEL LIDER BYMA — EN PESOS')
    L('  (Analisis directo en pesos. Sin conversion CCL.)')
    L('-' * 65)

    byma_sin_ob = [r for r in resultados_byma if not r['ob_enc_bool']]
    byma_con_ob = [r for r in resultados_byma if r['ob_enc_bool']]

    if byma_sin_ob:
        for i, r in enumerate(byma_sin_ob, 1):
            L(f'\n  #{i}  {r["ticker"]}  ({PANEL_LIDER_BYMA.get(r["ticker"], "")})')
            L(f'  {"─"*60}')
            L(f'  Precio:      ${r["precio"]:,.2f} ARS')
            L(f'  Zona SMC:    {r["zona"]}  ({r["pct_rango"]:.1f}% del rango)')
            L(f'  Estructura:  {r["estructura"]}')
            L(f'  OB Encima:   {r["ob_encima"]}')
            L(f'  Equilibrium: ${r["equilibrium"]:,.2f} ARS  (→ +{r["dist_equil"]:.1f}%)')
            L(f'  RSI:         {r["rsi"]}')
            L(f'  Score:       {r["score"]}/7')
            L(f'  Entrada:     ${r["precio"]:,.2f}  Stop: ${r["stop"]:,.2f}  Target: ${r["target"]:,.2f}')
            L(f'  {"─"*60}')
            L()
    else:
        L('\n  Sin señales en Panel Lider BYMA hoy.')

    # ── SECCION 5: PLAN DEL DIA ───────────────────────────────
    L()
    L('=' * 65)
    L('SECCION 5: PLAN DEL DIA')
    L('-' * 65)

    todos = sorted(
        resultados_cedears + resultados_adrs + resultados_byma,
        key=lambda x: (x['ob_enc_bool'], -x['score'], x['pct_rango'])
    )
    top3 = [r for r in todos if not r['ob_enc_bool']][:3]

    L()
    L('  HORARIOS CLAVE (hora Argentina):')
    L('  10:00  Apertura BYMA — revisar Panel Lider')
    L('  11:30  Apertura NYSE — revisar CEDEARs y ADRs')
    L('  11:30-12:30  EVITAR OPERAR — solapamiento Londres/NY')
    L('  13:00-15:00  MEJOR VENTANA DE ENTRADA')
    L('  17:00-18:00  EVITAR — manipulacion de cierre NY')
    L()

    if top3:
        L('  PRIORIDADES HOY:')
        for i, r in enumerate(top3, 1):
            es_adr   = r['ticker'] in ADRS_ARG_NYSE
            dc_label = ' ⭐⭐ DOBLE CONFLUENCIA' if es_adr else ''
            L(f'  {i}. {r["ticker"]:6} | Score:{r["score"]}/7 | {r["zona"]} | '
              f'RSI:{r["rsi"]} | Entrada:${r["precio"]:,.2f}{dc_label}')

    L()
    L(f'  MAX POR OPERACION: 25% del capital disponible')
    L(f'  STOP DURO: -{STOP_PCT}% sin excepcion')
    L(f'  TARGET:    +{TARGET_PCT}% (R/R {TARGET_PCT/STOP_PCT:.1f})')

    if not any([resultados_cedears, resultados_adrs, resultados_byma]):
        L()
        L('  SIN SEÑALES HOY — El mercado esta fuera de zona de valor.')
        L('  Mejor esperar. El capital que no se pierde, no necesita recuperarse.')

    L()
    L('=' * 65)
    L(f'  Generado: {now}  |  Swing:{SWING_LENGTH}  |  Score min:{SCORE_MINIMO}')
    L('=' * 65)

    return '\n'.join(lines)


# ============================================================
# RATIOS: cargar desde JSON y helpers
# ============================================================
_RATIOS_JSON    = 'results/ratios_cedears.json'
_RATIOS_CEDEARS = {}
_ADRS_ARG       = {}

def _cargar_ratios():
    global _RATIOS_CEDEARS, _ADRS_ARG
    if not os.path.exists(_RATIOS_JSON):
        print(f"  AVISO: {_RATIOS_JSON} no encontrado — sin ratios")
        return
    with open(_RATIOS_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data.pop('_meta', None)
    for ticker, info in data.items():
        if info.get('tipo') == 'ADR-Argentina':
            _ADRS_ARG[ticker] = info
        else:
            _RATIOS_CEDEARS[ticker] = info
    print(f"  Ratios: {len(_RATIOS_CEDEARS)} CEDEARs + {len(_ADRS_ARG)} ADRs argentinos")

def get_ratio(ticker):
    info = _RATIOS_CEDEARS.get(ticker) or _ADRS_ARG.get(ticker)
    return float(info['ratio']) if info and info.get('ratio') else 1.0

def get_byma(ticker):
    info = _ADRS_ARG.get(ticker)
    return info.get('byma', ticker) if info else ticker

def get_nombre_cedear(ticker):
    info = _RATIOS_CEDEARS.get(ticker) or _ADRS_ARG.get(ticker)
    return info.get('nombre', ticker) if info else ticker

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    inicio = datetime.now()
    print(f'\n{"="*65}')
    print(f'  SMC REPORTE DIARIO v2 — Argentina Edition (con ratios)')
    print(f'  {inicio.strftime("%d/%m/%Y %H:%M")}')
    print(f'{"="*65}\n')

    # 0. Cargar ratios
    print('Cargando ratios CEDEARs...')
    _cargar_ratios()

    # Universo desde JSON
    tickers_cedears = [t for t, d in _RATIOS_CEDEARS.items()
                       if d.get('tipo') not in ('ETF', 'Brasil')
                       and not t.endswith('.BA')]
    tickers_adrs    = list(_ADRS_ARG.keys())

    print(f'  Universo: {len(tickers_cedears)} CEDEARs + {len(tickers_adrs)} ADRs arg + {len(PANEL_LIDER_BYMA)} BYMA')

    # 1. CCL
    print('\nObteniendo CCL...')
    ccl, ccl_fuente = get_ccl()
    print(f'  CCL: ${ccl:,.2f} ARS/USD  (fuente: {ccl_fuente})')

    # 2. Rotacion sectorial
    print('\nCalculando rotacion sectorial...')
    rotacion, spy_ret = get_rotacion()

    # 3. Scan CEDEARs
    print(f'\nEscaneando {len(tickers_cedears)} CEDEARs...')
    resultados_cedears = []
    for i, t in enumerate(tickers_cedears):
        r = analyze(t, es_byma=False)
        if r:
            ratio = get_ratio(t)
            r['ratio']         = ratio
            r['nombre_cedear'] = get_nombre_cedear(t)
            r['precio_ars']    = round((r['precio'] / ratio) * ccl, 0)
            r['stop_ars']      = round((r['stop']   / ratio) * ccl, 0)
            r['target_ars']    = round((r['target'] / ratio) * ccl, 0)
            resultados_cedears.append(r)
            print(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ratio:{ratio} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    # 4. Scan ADRs Argentinos
    print(f'\nEscaneando {len(tickers_adrs)} ADRs Argentinos...')
    resultados_adrs = []
    for i, t in enumerate(tickers_adrs):
        r = analyze(t, es_byma=False)
        if r:
            ratio = get_ratio(t)
            r['ratio']         = ratio
            r['nombre_cedear'] = get_nombre_cedear(t)
            r['byma_local']    = get_byma(t)
            r['precio_ars']    = round((r['precio'] / ratio) * ccl, 0)
            r['stop_ars']      = round((r['stop']   / ratio) * ccl, 0)
            r['target_ars']    = round((r['target'] / ratio) * ccl, 0)
            resultados_adrs.append(r)
            print(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ratio:{ratio} | ARS:${r["precio_ars"]:,.0f} | BYMA:{r["byma_local"]}')
        if (i+1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    # 5. Scan Panel BYMA
    print(f'\nEscaneando {len(PANEL_LIDER_BYMA)} acciones BYMA...')
    resultados_byma = []
    for i, t in enumerate(PANEL_LIDER_BYMA.keys()):
        r = analyze(t, es_byma=True)
        if r:
            r['ratio']         = 1
            r['nombre_cedear'] = PANEL_LIDER_BYMA.get(t, t)
            r['precio_ars']    = r['precio']
            r['stop_ars']      = r['stop']
            r['target_ars']    = r['target']
            resultados_byma.append(r)
            print(f'  HIT {t:10} | Score:{r["score"]}/7 | {r["zona"]} | BYMA')
        if (i+1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    # Ordenar
    for lst in [resultados_cedears, resultados_adrs, resultados_byma]:
        lst.sort(key=lambda x: (-x['score'], x['ob_enc_bool'], x['pct_rango']))

    # 6. Generar y guardar reporte
    print('\nGenerando reporte...')
    reporte = generar_reporte(
        ccl, ccl_fuente,
        resultados_cedears, resultados_adrs, resultados_byma,
        rotacion, spy_ret
    )

    os.makedirs('results', exist_ok=True)
    date_str     = inicio.strftime('%Y%m%d_%H%M')
    fname_dated  = f'results/reporte_{date_str}.txt'
    fname_latest = 'results/reporte_latest.txt'

    with open(fname_dated,  'w', encoding='utf-8') as f:
        f.write(reporte)
    with open(fname_latest, 'w', encoding='utf-8') as f:
        f.write(reporte)

    todos = resultados_cedears + resultados_adrs + resultados_byma
    if todos:
        cols = ['ticker','nombre_cedear','score','zona','precio','ratio',
                'precio_ars','stop_ars','target_ars','rsi','estructura']
        df = pd.DataFrame(todos)
        df[[c for c in cols if c in df.columns]].to_csv(
            f'results/confluence_{date_str}.csv', index=False)

    print(reporte)
    elapsed = (datetime.now() - inicio).seconds
    print(f'\nTiempo total: {elapsed}s')
    print(f'Guardado en: {fname_latest}')
