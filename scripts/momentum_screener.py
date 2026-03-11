"""
Institutional Momentum Screener
================================
Detecta acciones con señales de movimiento institucional inminente.
Señales: Absorción, Relative Strength vs ETF sectorial, Squeeze, Gap, Opciones.
Corre en GitHub Actions cada mañana antes de la apertura del mercado.
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
# Modificá estos valores para calibrar el screener
# ============================================================

# --- Universo ---
SECTORES_INCLUIR    = []        # [] = todos los sectores
                                # Opciones: 'Technology', 'Financial Services',
                                # 'Healthcare', 'Energy', 'Consumer Cyclical',
                                # 'Consumer Defensive', 'Industrials', 'Basic Materials',
                                # 'Real Estate', 'Utilities', 'Communication Services'
SECTORES_EXCLUIR    = ['Utilities', 'Real Estate']  # Sectores de bajo movimiento

# --- Señal 1: Absorción Institucional ---
ABSORCION_VOL_RATIO = 2.5       # Volumen mínimo vs promedio 20d (2.5 = 150% más)
ABSORCION_CLOSE_PCT = 0.70      # Cierre en top X% de la vela (0.70 = top 30%)
ABSORCION_WICK_MIN  = 0.30      # Wick inferior mínimo como % del rango de la vela

# --- Señal 2: Relative Strength vs ETF sectorial ---
RS_RATIO_MIN        = 1.03      # Acción debe ser X veces más fuerte que su ETF
RS_DIAS             = 5         # Días a mirar para el cálculo de RS
RS_ETF_VS_SPY_MIN   = 1.00      # El ETF sectorial también debe superar a SPY (1.0 = neutral)

# --- Señal 3: Compresión de Volatilidad (Squeeze) ---
SQUEEZE_DIAS        = 10        # Días de rango comprimido para confirmar squeeze
SQUEEZE_RATIO       = 0.60      # Rango actual debe ser menor al X% del rango promedio

# --- Señal 4: Gap Institucional ---
GAP_MIN_PCT         = 1.2       # Gap mínimo % para ser considerado institucional
GAP_MAX_PCT         = 8.0       # Gap máximo % (gaps enormes son trampas)

# --- Señal 5: Opciones inusuales ---
OPCIONES_PC_MAX     = 0.5       # Put/Call ratio máximo (bajo = dominio de calls = alcista)
OPCIONES_VOL_RATIO  = 1.5       # Volumen de opciones vs promedio

# --- Score y filtros finales ---
SCORE_MINIMO        = 2         # Mínimo de señales para aparecer en resultados
TARGET_PCT          = 1.0       # Target de ganancia %
STOP_PCT            = 0.5       # Stop loss %
DATA_PERIOD         = '3mo'     # Período de datos históricos
BATCH_SIZE          = 10
SLEEP_BETWEEN       = 1.5

# ============================================================
# MAPEO SECTOR → ETF DE REFERENCIA
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
# TICKERS S&P 500 + NASDAQ 100
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
    'GM','GPC','GILD','GPN','GL','GDDY','GS','HAL','HIG','HAS','HCA','HSIC','HSY','HES',
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
# CACHE DE DATOS DE MERCADO
# Descargamos SPY y ETFs sectoriales una sola vez
# ============================================================
print('Descargando datos de referencia (SPY + ETFs sectoriales)...')

REFERENCE_TICKERS = ['SPY'] + list(SECTOR_ETF.values())
ref_data = {}

for ref in REFERENCE_TICKERS:
    try:
        df = yf.download(ref, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is not None and len(df) > 10:
            ref_data[ref] = df.dropna()
    except Exception:
        pass

spy_data = ref_data.get('SPY')
print(f'  Referencias cargadas: {list(ref_data.keys())}')


# ============================================================
# CACHE DE INFO SECTORIAL
# Descargamos el sector de cada ticker una sola vez
# ============================================================
def get_sector(ticker):
    try:
        info = yf.Ticker(ticker).info
        return info.get('sector', 'Unknown')
    except Exception:
        return 'Unknown'


# ============================================================
# SEÑALES
# ============================================================

def signal_absorcion(high_arr, low_arr, close_arr, vol_arr):
    """
    Señal 1: Absorción Institucional
    Ballena comprando → vela con volumen enorme, cierre alto, wick inferior largo.
    Miramos los últimos 3 días para detectar señal reciente.
    """
    if len(vol_arr) < 25:
        return False, {}

    avg_vol = float(np.mean(vol_arr[-21:-1]))
    if avg_vol == 0:
        return False, {}

    # Buscar señal en los últimos 3 días
    for lookback in range(1, 4):
        idx = -lookback
        vela_range = float(high_arr[idx]) - float(low_arr[idx])
        if vela_range == 0:
            continue

        vol_ratio   = float(vol_arr[idx]) / avg_vol
        close_pct   = (float(close_arr[idx]) - float(low_arr[idx])) / vela_range
        wick_inf    = (min(float(close_arr[idx-1]), float(close_arr[idx])) - float(low_arr[idx])) / vela_range

        if (vol_ratio   >= ABSORCION_VOL_RATIO and
            close_pct   >= ABSORCION_CLOSE_PCT and
            wick_inf    >= ABSORCION_WICK_MIN):
            return True, {
                'absorcion_vol_ratio': round(vol_ratio, 2),
                'absorcion_close_pct': round(close_pct * 100, 1),
                'absorcion_dias_atras': lookback
            }

    return False, {}


def signal_relative_strength(ticker_close, sector_etf, spy_close):
    """
    Señal 2: Relative Strength vs ETF sectorial y vs SPY
    Acción debe ser más fuerte que su sector, y el sector más fuerte que SPY.
    """
    if len(ticker_close) < RS_DIAS + 2:
        return False, {}
    if sector_etf is None or len(sector_etf) < RS_DIAS + 2:
        return False, {}
    if spy_close is None or len(spy_close) < RS_DIAS + 2:
        return False, {}

    # Retorno de los últimos RS_DIAS días
    ret_ticker = float(ticker_close[-1]) / float(ticker_close[-RS_DIAS]) - 1
    ret_etf    = float(sector_etf[-1])   / float(sector_etf[-RS_DIAS])   - 1
    ret_spy    = float(spy_close[-1])    / float(spy_close[-RS_DIAS])    - 1

    # Relative strength ratios
    rs_vs_etf  = (1 + ret_ticker) / (1 + ret_etf)   if (1 + ret_etf) != 0  else 0
    rs_etf_spy = (1 + ret_etf)    / (1 + ret_spy)   if (1 + ret_spy) != 0  else 0

    if rs_vs_etf >= RS_RATIO_MIN and rs_etf_spy >= RS_ETF_VS_SPY_MIN:
        return True, {
            'rs_vs_etf':       round(rs_vs_etf, 3),
            'rs_etf_vs_spy':   round(rs_etf_spy, 3),
            'ret_ticker_5d':   round(ret_ticker * 100, 2),
            'ret_etf_5d':      round(ret_etf * 100, 2),
        }

    return False, {}


def signal_squeeze(high_arr, low_arr):
    """
    Señal 3: Compresión de Volatilidad (Squeeze)
    Rango diario en mínimos históricos → expansión inminente.
    """
    if len(high_arr) < SQUEEZE_DIAS + 5:
        return False, {}

    rangos = high_arr - low_arr
    rango_actual  = float(np.mean(rangos[-3:]))          # promedio últimos 3 días
    rango_previo  = float(np.mean(rangos[-(SQUEEZE_DIAS+3):-3]))  # promedio previo

    if rango_previo == 0:
        return False, {}

    ratio = rango_actual / rango_previo

    if ratio <= SQUEEZE_RATIO:
        return True, {
            'squeeze_ratio':      round(ratio, 3),
            'squeeze_dias':       SQUEEZE_DIAS,
            'rango_actual_pct':   round(rango_actual / float(high_arr[-1]) * 100, 2),
        }

    return False, {}


def signal_gap(open_arr, close_arr, high_arr, low_arr, vol_arr):
    """
    Señal 4: Gap Institucional
    Gap alcista significativo que NO fue rellenado el mismo día.
    """
    if len(open_arr) < 3:
        return False, {}

    # Gap entre cierre de ayer y apertura de hoy
    prev_close  = float(close_arr[-2])
    today_open  = float(open_arr[-1])
    today_close = float(close_arr[-1])
    today_low   = float(low_arr[-1])

    if prev_close == 0:
        return False, {}

    gap_pct = (today_open - prev_close) / prev_close * 100

    # Gap alcista dentro del rango esperado
    if GAP_MIN_PCT <= gap_pct <= GAP_MAX_PCT:
        # Verificar que el gap NO fue rellenado (precio no bajó al cierre anterior)
        gap_rellenado = today_low <= prev_close

        if not gap_rellenado:
            avg_vol   = float(np.mean(vol_arr[-21:-1]))
            vol_ratio = float(vol_arr[-1]) / avg_vol if avg_vol > 0 else 0

            return True, {
                'gap_pct':      round(gap_pct, 2),
                'gap_rellenado': gap_rellenado,
                'gap_vol_ratio': round(vol_ratio, 2),
            }

    return False, {}


def signal_opciones(ticker):
    """
    Señal 5: Flujo de Opciones Inusual
    Put/Call ratio bajo + volumen de calls inusual = expectativa alcista institucional.
    """
    try:
        tk        = yf.Ticker(ticker)
        expirations = tk.options
        if not expirations:
            return False, {}

        # Usamos la expiración más cercana (próximas 2 semanas)
        exp = expirations[0]
        chain = tk.option_chain(exp)

        calls_vol = float(chain.calls['volume'].sum())
        puts_vol  = float(chain.puts['volume'].sum())

        if calls_vol + puts_vol == 0:
            return False, {}

        pc_ratio = puts_vol / calls_vol if calls_vol > 0 else 99

        # Calls OTM con volumen inusual (precio strike > precio actual)
        price_now   = float(tk.info.get('regularMarketPrice', 0))
        calls_otm   = chain.calls[chain.calls['strike'] > price_now * 1.02]
        otm_vol     = float(calls_otm['volume'].sum())
        otm_oi      = float(calls_otm['openInterest'].sum())
        vol_oi_ratio = otm_vol / otm_oi if otm_oi > 0 else 0

        if pc_ratio <= OPCIONES_PC_MAX or vol_oi_ratio >= OPCIONES_VOL_RATIO:
            return True, {
                'pc_ratio':      round(pc_ratio, 3),
                'calls_vol':     int(calls_vol),
                'otm_vol_ratio': round(vol_oi_ratio, 2),
            }

    except Exception:
        pass

    return False, {}


# ============================================================
# ANÁLISIS COMPLETO POR TICKER
# ============================================================

def analyze_ticker(ticker, sector_cache):
    try:
        # Obtener sector
        sector = sector_cache.get(ticker, 'Unknown')
        if sector == 'Unknown':
            sector = get_sector(ticker)
            sector_cache[ticker] = sector

        # Filtros de sector
        if SECTORES_INCLUIR and sector not in SECTORES_INCLUIR:
            return None
        if sector in SECTORES_EXCLUIR:
            return None

        # Descargar datos diarios
        df = yf.download(ticker, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is None or len(df) < 30:
            return None

        df        = df.dropna()
        high_arr  = df['High'].values.flatten()
        low_arr   = df['Low'].values.flatten()
        close_arr = df['Close'].values.flatten()
        open_arr  = df['Open'].values.flatten()
        vol_arr   = df['Volume'].values.flatten()
        price     = float(close_arr[-1])

        # ETF sectorial correspondiente
        etf_symbol  = SECTOR_ETF.get(sector)
        etf_data    = ref_data.get(etf_symbol) if etf_symbol else None
        etf_close   = etf_data['Close'].values.flatten() if etf_data is not None else None
        spy_close   = spy_data['Close'].values.flatten() if spy_data is not None else None

        # ── Evaluar cada señal ──────────────────────────────────────
        score   = 0
        signals = {}
        details = {}

        # Señal 1: Absorción
        hit, info = signal_absorcion(high_arr, low_arr, close_arr, vol_arr)
        signals['absorcion'] = hit
        if hit:
            score += 1
            details.update(info)

        # Señal 2: Relative Strength
        hit, info = signal_relative_strength(close_arr, etf_close, spy_close)
        signals['rs'] = hit
        if hit:
            score += 1
            details.update(info)

        # Señal 3: Squeeze
        hit, info = signal_squeeze(high_arr, low_arr)
        signals['squeeze'] = hit
        if hit:
            score += 1
            details.update(info)

        # Señal 4: Gap institucional
        hit, info = signal_gap(open_arr, close_arr, high_arr, low_arr, vol_arr)
        signals['gap'] = hit
        if hit:
            score += 1
            details.update(info)

        # Señal 5: Opciones (solo si ya tiene score >= 1 para no gastar tiempo)
        if score >= 1:
            hit, info = signal_opciones(ticker)
            signals['opciones'] = hit
            if hit:
                score += 1
                details.update(info)

        if score < SCORE_MINIMO:
            return None

        # ── Calcular entrada, target y stop ────────────────────────
        entry       = round(price, 2)
        target      = round(price * (1 + TARGET_PCT / 100), 2)
        stop        = round(price * (1 - STOP_PCT / 100), 2)
        risk_reward = round(TARGET_PCT / STOP_PCT, 1)

        # Señales activas como string
        activas = [k for k, v in signals.items() if v]
        senales_str = ' + '.join(activas)

        return {
            'Ticker':           ticker,
            'Sector':           sector,
            'ETF_Ref':          etf_symbol or 'N/A',
            'Score':            score,
            'Senales':          senales_str,
            'Precio':           entry,
            'Target_1pct':      target,
            'Stop':             stop,
            'RR':               risk_reward,
            # Detalles de señales
            'Absorcion_Vol':    details.get('absorcion_vol_ratio', ''),
            'RS_vs_ETF':        details.get('rs_vs_etf', ''),
            'RS_ETF_vs_SPY':    details.get('rs_etf_vs_spy', ''),
            'Ret_5d_pct':       details.get('ret_ticker_5d', ''),
            'Squeeze_Ratio':    details.get('squeeze_ratio', ''),
            'Gap_pct':          details.get('gap_pct', ''),
            'PC_Ratio':         details.get('pc_ratio', ''),
            'TradingView':      f'https://www.tradingview.com/chart/?symbol={ticker}',
        }

    except Exception as e:
        print(f'  Warning {ticker}: {e}')
        return None


# ============================================================
# RESUMEN DE SECTORES
# ============================================================

def print_sector_summary(results_df, spy_data):
    print('\n=== ROTACION SECTORIAL (ETF vs SPY, ultimos 5 dias) ===')
    if spy_data is None:
        return

    spy_ret = float(spy_data['Close'].iloc[-1]) / float(spy_data['Close'].iloc[-RS_DIAS]) - 1

    rows = []
    for sector, etf in SECTOR_ETF.items():
        if etf in ref_data:
            etf_close = ref_data[etf]['Close']
            if len(etf_close) >= RS_DIAS:
                etf_ret  = float(etf_close.iloc[-1]) / float(etf_close.iloc[-RS_DIAS]) - 1
                rs_ratio = (1 + etf_ret) / (1 + spy_ret) if (1 + spy_ret) != 0 else 0
                rows.append({
                    'Sector': sector,
                    'ETF':    etf,
                    'Ret_5d': round(etf_ret * 100, 2),
                    'RS_vs_SPY': round(rs_ratio, 3),
                    'Lider': 'SI' if rs_ratio >= 1.0 else '  '
                })

    if rows:
        df_sec = pd.DataFrame(rows).sort_values('RS_vs_SPY', ascending=False)
        print(df_sec.to_string(index=False))


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print('\n' + '=' * 65)
    print('  INSTITUTIONAL MOMENTUM SCREENER')
    print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 65)
    print(f'  Universo   : {len(ALL_TICKERS)} tickers')
    print(f'  Score min  : {SCORE_MINIMO}/5 señales')
    print(f'  Target     : +{TARGET_PCT}%  |  Stop: -{STOP_PCT}%  |  R/R: {TARGET_PCT/STOP_PCT}')
    print(f'  RS dias    : {RS_DIAS}  |  Squeeze dias: {SQUEEZE_DIAS}')
    print('=' * 65)

    # Mostrar rotación sectorial
    print_sector_summary(None, spy_data)

    # Scan principal
    print(f'\nEscaneando {len(ALL_TICKERS)} tickers...\n')
    results      = []
    sector_cache = {}
    total        = len(ALL_TICKERS)

    for i, ticker in enumerate(ALL_TICKERS):
        result = analyze_ticker(ticker, sector_cache)
        if result:
            results.append(result)
            print(f'  HIT [{i+1:3}/{total}] {ticker:6} | Score: {result["Score"]}/5 | '
                  f'{result["Senales"]:35} | {result["Sector"]}')
        else:
            print(f'  --- [{i+1:3}/{total}] {ticker}')

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    # Resultados finales
    print('\n' + '=' * 65)
    print(f'  RESULTADO: {len(results)} oportunidades encontradas de {total}')
    print('=' * 65)

    if results:
        df_out = pd.DataFrame(results).sort_values('Score', ascending=False)

        os.makedirs('results', exist_ok=True)
        date_str = datetime.now().strftime('%Y%m%d_%H%M')
        df_out.to_csv(f'results/momentum_{date_str}.csv', index=False)
        df_out.to_csv('results/momentum_latest.csv', index=False)

        print(f'\nGuardado: results/momentum_latest.csv')
        print('\nTOP 15 oportunidades del dia:')
        cols = ['Ticker','Sector','Score','Senales','Precio','Target_1pct','Stop','RR','RS_vs_ETF','Ret_5d_pct']
        print(df_out[cols].head(15).to_string(index=False))
    else:
        print('\nSin resultados. Baja SCORE_MINIMO o ajusta los parametros.')
        print('Sugerencia: SCORE_MINIMO = 1 para ver todas las señales individuales.')
