"""
SMC Confluence Screener v1
===========================
Combina SMC (LuxAlgo) + Momentum Institucional + Estructura Alcista
Parametros calibrados con Swing Length 50 (igual que TradingView/LuxAlgo)
Timeframes: Diario (estructura) + 1H (confirmacion de entrada)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import warnings
import time
import os
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================
# === PARAMETROS AJUSTABLES ===
# ============================================================

# --- SMC (calibrado igual que LuxAlgo en TradingView) ---
SWING_LENGTH_D      = 50       # Swing length diario  (LuxAlgo default)
SWING_LENGTH_H      = 50       # Swing length 1H      (LuxAlgo default)
DISCOUNT_PCT        = 0.25     # Zona discount = 25% inferior del rango
NEAR_DISCOUNT_PCT   = 0.40     # Near discount = hasta 40% del rango
EQUILIBRIUM_BAND    = 0.05     # Equilibrium = 50% +/- 5%

# --- Estructura ---
ESTRUCTURA_ALCISTA  = True     # Solo acciones con estructura alcista (HH/HL)
MIN_SWING_HIGHS     = 2        # Minimo de swing highs para confirmar estructura

# --- Momentum Institucional ---
RS_DIAS             = 5        # Dias para calcular Relative Strength
RS_RATIO_MIN        = 1.02     # Accion debe superar X veces a su ETF sectorial
RS_ETF_VS_SPY_MIN   = 0.99     # ETF sectorial vs SPY (0.99 = casi neutral o mejor)
ABSORCION_VOL_RATIO = 2.5      # Volumen minimo vs promedio 20d
ABSORCION_CLOSE_PCT = 0.70     # Cierre en top 30% de la vela
SQUEEZE_RATIO       = 0.65     # Rango actual vs promedio historico

# --- Opciones ---
OPCIONES_PC_MAX     = 0.6      # Put/Call ratio maximo

# --- Filtros finales ---
SCORE_MINIMO        = 3        # Minimo de señales confluentes (max 6)
TARGET_PCT          = 1.0      # Target %
STOP_PCT            = 0.5      # Stop %
DATA_PERIOD_D       = '1y'     # Periodo datos diarios
DATA_PERIOD_H       = '1mo'    # Periodo datos 1H
BATCH_SIZE          = 10
SLEEP_BETWEEN       = 1.5

# --- Sectores ---
SECTORES_EXCLUIR    = ['Utilities', 'Real Estate']

# ============================================================
# MAPEO SECTOR → ETF
# ============================================================
SECTOR_ETF = {
    'Technology':             'XLK',
    'Financial Services':     'XLF',
    'Healthcare':             'XLV',
    'Energy':                 'XLE',
    'Consumer Cyclical':      'XLY',
    'Consumer Defensive':     'XLP',
    'Industrials':            'XLI',
    'Basic Materials':        'XLB',
    'Real Estate':            'XLRE',
    'Utilities':              'XLU',
    'Communication Services': 'XLC',
}

# ============================================================
# TICKERS
# ============================================================
SP500 = [
    'MMM','ABT','ABBV','ACN','ADBE','AMD','AFL','A','APD','ABNB','ALB',
    'ALGN','ALL','GOOGL','GOOG','MO','AMZN','AEE','AAL','AEP','AXP',
    'AIG','AMT','AWK','AMP','AME','AMGN','APH','ADI','AON','APA','AAPL','AMAT',
    'APTV','ACGL','ADM','ANET','AJG','T','ATO','ADSK','ADP','AZO','AVB','AVY',
    'AXON','BKR','BALL','BAC','BK','BBWI','BAX','BDX','BBY','BIIB','BLK','BX',
    'BA','BSX','BMY','AVGO','BR','BRO','BG','CDNS','CPT','CPB','COF','CAH','KMX','CCL',
    'CARR','CAT','CBOE','CBRE','CDW','CE','COR','CNC','CF','CRL','SCHW','CHTR',
    'CVX','CMG','CB','CHD','CI','CINF','CTAS','CSCO','C','CFG','CLX','CME','CMS','KO',
    'CTSH','CL','CMCSA','CAG','COP','ED','STZ','CEG','COO','CPRT','GLW','CTVA','CSGP',
    'COST','CTRA','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','DVN',
    'DXCM','FANG','DLR','DG','DLTR','D','DPZ','DOV','DOW','DTE','DUK','DD',
    'EMN','ETN','EBAY','ECL','EIX','EW','EA','ELV','LLY','EMR','ENPH','ETR','EOG',
    'EQT','EFX','EQIX','EQR','ESS','EL','ETSY','EG','ES','EXC','EXPE','EXPD',
    'EXR','XOM','FFIV','FDS','FICO','FAST','FRT','FDX','FIS','FITB','FSLR','FE',
    'FMC','F','FTNT','FTV','BEN','FCX','GRMN','IT','GE','GEHC','GEN','GNRC','GD','GIS',
    'GM','GPC','GILD','GPN','GL','GDDY','GS','HAL','HIG','HAS','HCA','HSIC','HSY',
    'HPE','HLT','HOLX','HD','HON','HRL','HST','HWM','HPQ','HUBB','HUM','HBAN','HII',
    'IBM','IEX','IDXX','ITW','INCY','IR','INTC','ICE','IFF','IP','IPG','INTU','ISRG',
    'IVZ','INVH','IQV','IRM','JBHT','JBL','J','JNJ','JCI','JPM','JNPR','K',
    'KDP','KEY','KEYS','KMB','KIM','KMI','KLAC','KHC','KR','LHX','LH','LRCX','LW',
    'LVS','LDOS','LEN','LIN','LYV','LKQ','LMT','L','LOW','LULU','LYB','MTB','MRO',
    'MPC','MAR','MMC','MLM','MAS','MA','MTCH','MKC','MCD','MCK','MDT','MRK',
    'META','MET','MTD','MGM','MCHP','MU','MSFT','MAA','MRNA','MHK','MOH','TAP','MDLZ',
    'MPWR','MNST','MCO','MS','MOS','MSI','MSCI','NDAQ','NTAP','NEE','NKE','NEM','NI',
    'NFLX','NWL','NUE','NVDA','NVR','NXPI','ORLY','OXY','ODFL','OMC','ON','OKE','ORCL',
    'OTIS','PCAR','PKG','PANW','PARA','PH','PAYX','PAYC','PYPL','PNR','PEP','PFE','PCG',
    'PM','PSX','PNW','PNC','POOL','PPG','PPL','PFG','PG','PGR','PLD','PRU','PEG','PTC',
    'PSA','PHM','PWR','QCOM','DGX','RL','RJF','RTX','O','REG','REGN','RF','RSG','RMD',
    'ROK','ROL','ROP','ROST','RCL','SPGI','CRM','SBAC','SLB','STX','SRE','NOW','SHW',
    'SPG','SWKS','SJM','SNA','SO','LUV','SWK','SBUX','STT','STLD','STE','SYK','SYF',
    'SNPS','SYY','TMUS','TROW','TTWO','TPR','TRGP','TGT','TEL','TDY','TFX','TER','TSLA',
    'TXN','TXT','TMO','TJX','TSCO','TT','TDG','TRV','TRMB','TFC','TYL','TSN','USB',
    'UBER','UDR','ULTA','UNP','UAL','UPS','URI','UNH','UHS','VLO','VTR','VRSN','VRSK',
    'VZ','VRTX','VTRS','VICI','V','VST','VMC','WM','WAT','WEC','WFC','WELL','WST',
    'WDC','WHR','WMB','WTW','GWW','WYNN','XEL','XYL','YUM','ZBRA','ZBH','ZTS'
]
NASDAQ100 = [
    'ADBE','AMD','ABNB','GOOGL','GOOG','AMZN','AMGN','ADI','AAPL','AMAT',
    'ASML','TEAM','ADSK','ADP','AXON','BIIB','BKNG','AVGO','CDNS','CDW','CHTR','CTAS',
    'CSCO','CTSH','CMCSA','CEG','CPRT','CSGP','COST','CRWD','CSX','DDOG','DXCM','FANG',
    'DLTR','EBAY','EA','EXC','FAST','FTNT','GILD','HON','IDXX','INTC','INTU','ISRG',
    'KDP','KLAC','KHC','LRCX','LULU','MAR','MRVL','MELI','META','MCHP','MU','MSFT',
    'MRNA','MDLZ','MDB','MNST','NFLX','NVDA','NXPI','ORLY','ON','PCAR','PANW','PAYX',
    'PYPL','PEP','QCOM','REGN','ROP','ROST','SBUX','SNPS','TTWO','TMUS','TSLA',
    'TXN','TTD','VRSK','VRTX','WDAY','XEL','ZS'
]
ALL_TICKERS = sorted(list(set(SP500 + NASDAQ100)))

# ============================================================
# FUNCIONES BASE SMC
# ============================================================

def detect_fvg(df):
    # Un FVG alcista ocurre si el Low de la vela 3 es mayor al High de la vela 1
    # Un FVG bajista ocurre si el High de la vela 3 es menor al Low de la vela 1
    
    last_3 = df.tail(3)
    c1_high = last_3.iloc[0]['High']
    c1_low = last_3.iloc[0]['Low']
    c3_high = last_3.iloc[2]['High']
    c3_low = last_3.iloc[2]['Low']
    
    fvg_bullish = c3_low > c1_high
    fvg_bearish = c3_high < c1_low
    
    if fvg_bullish:
        return "BULLISH_FVG"
    elif fvg_bearish:
        return "BEARISH_FVG"
    return None

def check_liquidity(ticker_data):
    # Calculamos el volumen promedio de los últimos 10 días en USD
    avg_volume = ticker_data['Volume'].tail(10).mean()
    avg_price = ticker_data['Close'].tail(10).mean()
    daily_turnover = avg_volume * avg_price
    
    # Solo nos interesan activos que muevan más de $250,000
    return daily_turnover > 250000
    
def to_arr(series):
    """Convierte cualquier serie/dataframe a numpy array plano"""
    return np.array(series).flatten()

def calculate_rsi(arr, period=14):
    s = pd.Series(arr)
    delta = s.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, min_periods=period).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, min_periods=period).mean()
    rs    = gain / loss
    return to_arr(100 - (100 / (1 + rs)))

def find_swings(high, low, length):
    """
    Replica leg() de LuxAlgo con swing length configurable.
    Retorna listas de (indice, precio) para highs y lows.
    """
    highs, lows = [], []
    n = len(high)
    for i in range(length, n):
        w_h = high[i-length:i]
        w_l = low[i-length:i]
        if high[i-length] == max(w_h):
            highs.append((i-length, float(high[i-length])))
        if low[i-length] == min(w_l):
            lows.append((i-length, float(low[i-length])))
    return highs, lows

def get_trailing_extremes(high, low, length):
    sh, sl = find_swings(high, low, length)
    if not sh or not sl:
        return None, None
    top    = max(v for _, v in sh[-5:])
    bottom = min(v for _, v in sl[-5:])
    return top, bottom

def get_zones(top, bottom):
    r = top - bottom
    return {
        'discount':      (bottom,             bottom + DISCOUNT_PCT * r),
        'near_discount': (bottom,             bottom + NEAR_DISCOUNT_PCT * r),
        'equilibrium':   (bottom + (0.5 - EQUILIBRIUM_BAND) * r,
                          bottom + (0.5 + EQUILIBRIUM_BAND) * r),
        'premium':       (top - DISCOUNT_PCT * r, top),
    }

def get_estructura(swing_highs, swing_lows):
    """
    Detecta estructura de mercado:
    - Alcista: HH (Higher High) + HL (Higher Low) — Smart Money acumulando
    - Bajista: LH (Lower High) + LL (Lower Low) — Smart Money distribuyendo
    - Indefinida: mixta
    """
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return 'Indefinida'

    # Ultimos 3 swing highs y lows
    ult_highs = [v for _, v in swing_highs[-3:]]
    ult_lows  = [v for _, v in swing_lows[-3:]]

    hh = all(ult_highs[i] > ult_highs[i-1] for i in range(1, len(ult_highs)))
    hl = all(ult_lows[i]  > ult_lows[i-1]  for i in range(1, len(ult_lows)))
    lh = all(ult_highs[i] < ult_highs[i-1] for i in range(1, len(ult_highs)))
    ll = all(ult_lows[i]  < ult_lows[i-1]  for i in range(1, len(ult_lows)))

    if hh and hl:
        return 'Alcista'
    elif lh and ll:
        return 'Bajista'
    elif hh or hl:
        return 'Alcista Debil'
    elif lh or ll:
        return 'Bajista Debil'
    return 'Lateral'

def detect_ob_encima(high, low, close, price, swing_highs):
    """
    Detecta si hay un Order Block de OFERTA (bajista) inmediatamente encima del precio.
    Si hay OB encima → precio tiene resistencia fuerte → NO es buen setup.
    Retorna True si hay OB bajista bloqueando el camino.
    """
    if not swing_highs:
        return False, None

    last_high_idx = swing_highs[-1][0]
    search_start  = max(1, last_high_idx - 8)
    search_end    = min(len(close) - 1, last_high_idx + 3)

    for i in range(search_end, search_start, -1):
        if i >= len(close) or i < 1:
            continue
        # Barra alcista antes de una caída = OB bajista
        if float(close[i]) > float(close[i-1]):
            ob_low  = float(low[i])
            ob_high = float(high[i])
            # OB encima del precio actual y dentro del 8% de distancia
            if ob_low > price and ob_low < price * 1.08:
                return True, round(ob_low, 2)

    return False, None

# ============================================================
# CACHE DATOS DE REFERENCIA
# ============================================================
print('Cargando referencias de mercado (SPY + ETFs sectoriales)...')
ref_data = {}
for sym in ['SPY'] + list(SECTOR_ETF.values()):
    try:
        df = yf.download(sym, period=DATA_PERIOD_D, interval='1d',
                         progress=False, auto_adjust=True)
        if df is not None and len(df) > 20:
            ref_data[sym] = df.dropna()
    except Exception:
        pass
print(f'  OK: {list(ref_data.keys())}')

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
# SEÑALES INDIVIDUALES
# ============================================================

def sig_smc_zona(high_d, low_d, close_d, price):
    """
    SMC Diario: precio en Discount o Near Discount.
    Calcula zonas con Swing Length 50 (igual que LuxAlgo).
    """
    top, bottom = get_trailing_extremes(high_d, low_d, SWING_LENGTH_D)
    if top is None or top == bottom:
        return False, {}, None, None, None, None

    zones  = get_zones(top, bottom)
    rango  = top - bottom
    pct_r  = (price - bottom) / rango * 100 if rango > 0 else 50

    in_disc      = zones['discount'][0]      <= price <= zones['discount'][1]
    in_near_disc = zones['near_discount'][0]  <= price <= zones['near_discount'][1]

    zona = 'Discount' if in_disc else ('Near Discount' if in_near_disc else None)

    if zona:
        return True, {
            'zona':          zona,
            'pct_rango':     round(pct_r, 1),
            'swing_high':    round(top, 2),
            'swing_low':     round(bottom, 2),
            'disc_top':      round(zones['discount'][1], 2),
            'equil':         round((top + bottom) / 2, 2),
        }, top, bottom, zones, pct_r

    return False, {}, top, bottom, zones, pct_r

def sig_estructura_alcista(high_d, low_d):
    """
    Estructura SMC: confirma que el activo esta en tendencia alcista
    (Higher Highs + Higher Lows). Elimina Markdown como MDB.
    """
    sh, sl = find_swings(high_d, low_d, SWING_LENGTH_D)
    estructura = get_estructura(sh, sl)
    alcista = estructura in ['Alcista', 'Alcista Debil']
    return alcista, {'estructura_d': estructura}, sh, sl

def sig_confirmacion_1h(ticker, zones):
    """
    Confirmacion en 1H: precio en zona de valor tambien en timeframe menor.
    Replica la logica SMC en 1H con Swing Length 50.
    """
    try:
        df = yf.download(ticker, period=DATA_PERIOD_H, interval='1h',
                         progress=False, auto_adjust=True)
        if df is None or len(df) < SWING_LENGTH_H + 10:
            return False, {}

        df    = df.dropna()
        h     = to_arr(df['High'])
        l     = to_arr(df['Low'])
        c     = to_arr(df['Close'])
        price = float(c[-1])

        top_h, bot_h = get_trailing_extremes(h, l, SWING_LENGTH_H)
        if top_h is None or top_h == bot_h:
            return False, {}

        zones_h     = get_zones(top_h, bot_h)
        in_disc_h   = zones_h['discount'][0]      <= price <= zones_h['discount'][1]
        in_near_h   = zones_h['near_discount'][0]  <= price <= zones_h['near_discount'][1]

        # RSI 1H
        rsi_h = float(calculate_rsi(c)[-1])

        if in_disc_h or in_near_h:
            return True, {
                'zona_1h':    'Discount' if in_disc_h else 'Near Discount',
                'rsi_1h':     round(rsi_h, 1),
                'swing_h_1h': round(top_h, 2),
                'swing_l_1h': round(bot_h, 2),
            }
    except Exception:
        pass
    return False, {}

def sig_rs_sectorial(close_d, sector):
    """
    Relative Strength: accion vs ETF sectorial vs SPY.
    Detecta rotacion de capital institucional.
    """
    etf_sym  = SECTOR_ETF.get(sector)
    etf_data = ref_data.get(etf_sym)
    spy_data = ref_data.get('SPY')

    if etf_data is None or spy_data is None:
        return False, {}

    etf_c = to_arr(etf_data['Close'])
    spy_c = to_arr(spy_data['Close'])

    min_len = min(len(close_d), len(etf_c), len(spy_c), RS_DIAS + 2)
    if min_len < RS_DIAS + 2:
        return False, {}

    ret_t   = float(close_d[-1])   / float(close_d[-RS_DIAS])   - 1
    ret_etf = float(etf_c[-1])     / float(etf_c[-RS_DIAS])     - 1
    ret_spy = float(spy_c[-1])     / float(spy_c[-RS_DIAS])     - 1

    rs_vs_etf  = (1 + ret_t)   / (1 + ret_etf) if (1 + ret_etf) != 0 else 0
    rs_etf_spy = (1 + ret_etf) / (1 + ret_spy) if (1 + ret_spy) != 0 else 0

    if rs_vs_etf >= RS_RATIO_MIN and rs_etf_spy >= RS_ETF_VS_SPY_MIN:
        return True, {
            'rs_vs_etf':    round(rs_vs_etf, 3),
            'rs_etf_spy':   round(rs_etf_spy, 3),
            'ret_5d_pct':   round(ret_t * 100, 2),
        }
    return False, {}

def sig_absorcion(high_d, low_d, close_d, vol_d):
    """
    Absorcion institucional: vela con volumen anomalo, cierre alto, wick inferior.
    En zona de Discount esto es acumulacion, no distribucion.
    """
    if len(vol_d) < 25:
        return False, {}

    avg_vol = float(np.mean(vol_d[-21:-1]))
    if avg_vol == 0:
        return False, {}

    for lookback in range(1, 4):
        idx        = -lookback
        vela_range = float(high_d[idx]) - float(low_d[idx])
        if vela_range == 0:
            continue
        vol_ratio  = float(vol_d[idx]) / avg_vol
        close_pct  = (float(close_d[idx]) - float(low_d[idx])) / vela_range
        wick_inf   = (min(float(close_d[idx-1]), float(close_d[idx])) - float(low_d[idx])) / vela_range

        if (vol_ratio  >= ABSORCION_VOL_RATIO and
            close_pct  >= ABSORCION_CLOSE_PCT and
            wick_inf   >= 0.20):
            return True, {
                'absorcion_vol': round(vol_ratio, 2),
                'absorcion_dias': lookback,
            }
    return False, {}

def sig_squeeze(high_d, low_d):
    """
    Compresion de volatilidad: rango diario en minimos.
    En zona de Discount + squeeze = explosion alcista inminente.
    """
    if len(high_d) < 20:
        return False, {}
    rangos        = high_d - low_d
    rango_actual  = float(np.mean(rangos[-3:]))
    rango_previo  = float(np.mean(rangos[-20:-3]))
    if rango_previo == 0:
        return False, {}
    ratio = rango_actual / rango_previo
    if ratio <= SQUEEZE_RATIO:
        return True, {'squeeze_ratio': round(ratio, 3)}
    return False, {}

def sig_opciones(ticker):
    """
    Flujo de opciones: Put/Call ratio bajo = expectativa alcista institucional.
    """
    try:
        tk   = yf.Ticker(ticker)
        exps = tk.options
        if not exps:
            return False, {}
        chain     = tk.option_chain(exps[0])
        calls_vol = float(chain.calls['volume'].sum())
        puts_vol  = float(chain.puts['volume'].sum())
        if calls_vol == 0:
            return False, {}
        pc_ratio = puts_vol / calls_vol
        if pc_ratio <= OPCIONES_PC_MAX:
            return True, {'pc_ratio': round(pc_ratio, 3)}
    except Exception:
        pass
    return False, {}

# ============================================================
# ANALISIS COMPLETO POR TICKER
# ============================================================

def analyze_ticker(ticker):
    try:
        # Sector
        sector = get_sector(ticker)
        if sector in SECTORES_EXCLUIR:
            return None

        # Datos diarios
        df_d = yf.download(ticker, period=DATA_PERIOD_D, interval='1d',
                           progress=False, auto_adjust=True)
        if df_d is None or len(df_d) < SWING_LENGTH_D + 20:
            return None
        df_d    = df_d.dropna()
        high_d  = to_arr(df_d['High'])
        low_d   = to_arr(df_d['Low'])
        close_d = to_arr(df_d['Close'])
        vol_d   = to_arr(df_d['Volume'])
        open_d  = to_arr(df_d['Open'])
        price   = float(close_d[-1])

        # ── SEÑAL 1: Zona SMC Diario ────────────────────────────
        # Esta es la condicion MAS IMPORTANTE — sin esto, no analizamos
        hit_zona, info_zona, top, bottom, zones, pct_r = sig_smc_zona(
            high_d, low_d, close_d, price)
        if not hit_zona:
            return None  # Precio no esta en zona de valor → descartado

        # ── SEÑAL 2: Estructura Alcista ─────────────────────────
        # Elimina casos como MDB (Markdown agresivo)
        hit_struct, info_struct, sh_d, sl_d = sig_estructura_alcista(high_d, low_d)
        if ESTRUCTURA_ALCISTA and not hit_struct:
            return None  # Estructura bajista → descartado

        # ── VERIFICACION: OB de Oferta encima? ─────────────────
        # Elimina casos como AVGO/MRVL (OB bajista bloqueando)
        ob_encima, ob_nivel = detect_ob_encima(high_d, low_d, close_d, price, sh_d)

        # ── SEÑAL 3: Confirmacion 1H ────────────────────────────
        hit_1h, info_1h = sig_confirmacion_1h(ticker, zones)

        # ── SEÑAL 4: Relative Strength Sectorial ────────────────
        hit_rs, info_rs = sig_rs_sectorial(close_d, sector)

        # ── SEÑAL 5: Absorcion Institucional ───────────────────
        hit_abs, info_abs = sig_absorcion(high_d, low_d, close_d, vol_d)

        # ── SEÑAL 6: Squeeze ───────────────────────────────────
        hit_sq, info_sq = sig_squeeze(high_d, low_d)

        # ── SCORE ──────────────────────────────────────────────
        # Zona y Estructura son obligatorias (ya filtramos arriba)
        # Las demas señales suman al score
        score_base   = 2  # zona + estructura
        score_bonus  = sum([hit_1h, hit_rs, hit_abs, hit_sq])

        # Penalizacion si hay OB de oferta encima
        ob_penalty = -1 if ob_encima else 0

        score_total = score_base + score_bonus + ob_penalty

        if score_total < SCORE_MINIMO:
            return None

        # ── OPCIONES (solo si score >= SCORE_MINIMO) ───────────
        hit_op, info_op = sig_opciones(ticker)
        if hit_op:
            score_total += 1

        # ── RSI Diario ─────────────────────────────────────────
        rsi_d = round(float(calculate_rsi(close_d)[-1]), 1)

        # ── Volumen ratio ──────────────────────────────────────
        avg_vol   = float(np.mean(vol_d[-21:-1]))
        vol_ratio = round(float(vol_d[-1]) / avg_vol, 2) if avg_vol > 0 else 0

        # ── Señales activas como string ────────────────────────
        senales = []
        senales.append(info_zona.get('zona', 'Discount'))
        if hit_struct: senales.append('estructura')
        if hit_1h:     senales.append('1H-conf')
        if hit_rs:     senales.append('RS-sector')
        if hit_abs:    senales.append('absorcion')
        if hit_sq:     senales.append('squeeze')
        if hit_op:     senales.append('opciones')
        if ob_encima:  senales.append('OB-ENCIMA⚠')

        # ── Niveles de trade ───────────────────────────────────
        entry  = round(price, 2)
        target = round(price * (1 + TARGET_PCT / 100), 2)
        stop   = round(price * (1 - STOP_PCT / 100), 2)

        # Distancia al equilibrium (imán de precio)
        equil       = (top + bottom) / 2
        dist_equil  = round((equil - price) / price * 100, 2)

        return {
            'Ticker':        ticker,
            'Sector':        sector,
            'ETF_Ref':       SECTOR_ETF.get(sector, 'N/A'),
            'Score':         score_total,
            'Senales':       ' | '.join(senales),
            # Precio y niveles
            'Precio':        entry,
            'Target_1pct':   target,
            'Stop':          stop,
            'RR':            round(TARGET_PCT / STOP_PCT, 1),
            # SMC
            'Zona_D':        info_zona.get('zona', ''),
            'Pct_Rango_D':   info_zona.get('pct_rango', ''),
            'Swing_High_D':  info_zona.get('swing_high', ''),
            'Swing_Low_D':   info_zona.get('swing_low', ''),
            'Equilibrium':   round(equil, 2),
            'Dist_Equil_Pct': dist_equil,
            'OB_Encima':     f'SI ({ob_nivel})' if ob_encima else 'NO',
            # Estructura
            'Estructura_D':  info_struct.get('estructura_d', ''),
            # 1H
            'Zona_1H':       info_1h.get('zona_1h', '-'),
            'RSI_1H':        info_1h.get('rsi_1h', '-'),
            # Momentum
            'RS_vs_ETF':     info_rs.get('rs_vs_etf', '-'),
            'Ret_5d_pct':    info_rs.get('ret_5d_pct', '-'),
            # Otros
            'RSI_D':         rsi_d,
            'Vol_Ratio_20d': vol_ratio,
            'Absorcion_Vol': info_abs.get('absorcion_vol', '-'),
            'Squeeze_Ratio': info_sq.get('squeeze_ratio', '-'),
            'PC_Ratio':      info_op.get('pc_ratio', '-'),
            'TradingView':   f'https://www.tradingview.com/chart/?symbol={ticker}',
        }

    except Exception as e:
        print(f'  Warning {ticker}: {e}')
        return None

# ============================================================
# ROTACION SECTORIAL
# ============================================================

def print_rotacion_sectorial():
    print('\n=== ROTACION SECTORIAL (ETF vs SPY, ultimos 5 dias) ===')
    spy_data = ref_data.get('SPY')
    if spy_data is None:
        return

    spy_c   = to_arr(spy_data['Close'])
    spy_ret = float(spy_c[-1]) / float(spy_c[-RS_DIAS]) - 1

    rows = []
    for sector, etf in SECTOR_ETF.items():
        if etf in ref_data:
            etf_c   = to_arr(ref_data[etf]['Close'])
            etf_ret = float(etf_c[-1]) / float(etf_c[-RS_DIAS]) - 1
            rs      = (1 + etf_ret) / (1 + spy_ret) if (1 + spy_ret) != 0 else 0
            rows.append({
                'Sector':    sector,
                'ETF':       etf,
                'Ret_5d%':   round(etf_ret * 100, 2),
                'RS_SPY':    round(rs, 3),
                'Capital':   'ENTRANDO' if rs >= 1.0 else 'saliendo'
            })

    df_r = pd.DataFrame(rows).sort_values('RS_SPY', ascending=False)
    print(df_r.to_string(index=False))
    print()

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print('\n' + '=' * 70)
    print('  SMC CONFLUENCE SCREENER v1')
    print('  Discount Zone + Estructura Alcista + Momentum Institucional')
    print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 70)
    print(f'  Universo     : {len(ALL_TICKERS)} tickers')
    print(f'  Swing Length : {SWING_LENGTH_D}D / {SWING_LENGTH_H}H (igual que LuxAlgo)')
    print(f'  Discount     : {int(DISCOUNT_PCT*100)}% | Near: {int(NEAR_DISCOUNT_PCT*100)}%')
    print(f'  Score minimo : {SCORE_MINIMO}/7 señales')
    print(f'  Target       : +{TARGET_PCT}% | Stop: -{STOP_PCT}% | R/R: {TARGET_PCT/STOP_PCT}')
    print('=' * 70)

    print_rotacion_sectorial()

    print(f'Escaneando {len(ALL_TICKERS)} tickers...\n')
    results = []
    total   = len(ALL_TICKERS)

    for i, ticker in enumerate(ALL_TICKERS):
        result = analyze_ticker(ticker)
        if result:
            results.append(result)
            ob_warn = ' ⚠ OB ENCIMA' if 'OB-ENCIMA' in result['Senales'] else ''
            print(f'  HIT [{i+1:3}/{total}] {ticker:6} | Score:{result["Score"]}/7 | '
                  f'{result["Zona_D"]:14} | Rango:{result["Pct_Rango_D"]:5.1f}% | '
                  f'{result["Estructura_D"]:14} | {result["Sector"]}{ob_warn}')
        else:
            print(f'  --- [{i+1:3}/{total}] {ticker}')

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    print('\n' + '=' * 70)
    print(f'  RESULTADO: {len(results)} oportunidades de confluencia de {total}')
    print('=' * 70)

    if results:
        df_out = pd.DataFrame(results)

        # Ordenar: mayor score primero, luego menor % en rango (mas cerca del fondo)
        df_out = df_out.sort_values(
            ['Score', 'Pct_Rango_D'],
            ascending=[False, True]
        )

        # Separar con y sin OB encima
        sin_ob  = df_out[~df_out['Senales'].str.contains('OB-ENCIMA')]
        con_ob  = df_out[df_out['Senales'].str.contains('OB-ENCIMA')]

        os.makedirs('results', exist_ok=True)
        date_str = datetime.now().strftime('%Y%m%d_%H%M')
        df_out.to_csv(f'results/confluence_{date_str}.csv', index=False)
        df_out.to_csv('results/confluence_latest.csv', index=False)
        print(f'\nGuardado: results/confluence_latest.csv')

        print(f'\n TIER 1 — Sin OB encima ({len(sin_ob)} acciones):')
        cols = ['Ticker','Sector','Score','Zona_D','Pct_Rango_D',
                'Estructura_D','RS_vs_ETF','RSI_D','Precio','Target_1pct','Stop']
        if not sin_ob.empty:
            print(sin_ob[cols].head(10).to_string(index=False))

        if not con_ob.empty:
            print(f'\n TIER 2 — Con OB encima, mas riesgo ({len(con_ob)} acciones):')
            print(con_ob[cols].head(5).to_string(index=False))

    else:
        print('\nSin resultados hoy.')
        print('Sugerencia: baja SCORE_MINIMO a 2 o aumenta NEAR_DISCOUNT_PCT a 0.45')
