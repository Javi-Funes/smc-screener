"""
SMC Discount Zone Screener v2
Replica logica LuxAlgo Smart Money Concepts con zonas ampliadas.
Calibrado usando BAX como referencia.
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
# PARAMETROS
# ============================================================
SWING_LENGTH      = 20     # Ventana swings (20=ciclos cortos, 50=largos)
DATA_PERIOD       = '1y'   # Periodo datos: '6mo', '1y', '2y'
BATCH_SIZE        = 10
SLEEP_BETWEEN     = 1.0
DISCOUNT_PCT      = 0.25   # Discount: 25% inferior del rango total
NEAR_DISCOUNT_PCT = 0.38   # Near discount: hasta 38% (captura rebotes)
PREMIUM_PCT       = 0.75   # Premium: por encima del 75% del rango

# ============================================================
# TICKERS S&P 500 + NASDAQ 100
# ============================================================
SP500 = [
    'MMM','AOS','ABT','ABBV','ACN','ADBE','AMD','AES','AFL','A','APD','ABNB','AKAM','ALB',
    'ALGN','ALLE','LNT','ALL','GOOGL','GOOG','MO','AMZN','AMCR','AEE','AAL','AEP','AXP',
    'AIG','AMT','AWK','AMP','AME','AMGN','APH','ADI','ANSS','AON','APA','AAPL','AMAT',
    'APTV','ACGL','ADM','ANET','AJG','AIZ','T','ATO','ADSK','ADP','AZO','AVB','AVY',
    'AXON','BKR','BALL','BAC','BK','BBWI','BAX','BDX','BBY','BIO','BIIB','BLK','BX',
    'BA','BSX','BMY','AVGO','BR','BRO','BG','CDNS','CPT','CPB','COF','CAH','KMX','CCL',
    'CARR','CAT','CBOE','CBRE','CDW','CE','COR','CNC','CF','CRL','SCHW','CHTR',
    'CVX','CMG','CB','CHD','CI','CINF','CTAS','CSCO','C','CFG','CLX','CME','CMS','KO',
    'CTSH','CL','CMCSA','CAG','COP','ED','STZ','CEG','COO','CPRT','GLW','CTVA','CSGP',
    'COST','CTRA','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','DVN',
    'DXCM','FANG','DLR','DG','DLTR','D','DPZ','DOV','DOW','DTE','DUK','DD',
    'EMN','ETN','EBAY','ECL','EIX','EW','EA','ELV','LLY','EMR','ENPH','ETR','EOG',
    'EQT','EFX','EQIX','EQR','ESS','EL','ETSY','EG','EVRG','ES','EXC','EXPE','EXPD',
    'EXR','XOM','FFIV','FDS','FICO','FAST','FRT','FDX','FIS','FITB','FSLR','FE',
    'FMC','F','FTNT','FTV','BEN','FCX','GRMN','IT','GE','GEHC','GEN','GNRC','GD','GIS',
    'GM','GPC','GILD','GPN','GL','GDDY','GS','HAL','HIG','HAS','HCA','HSIC','HSY','HES',
    'HPE','HLT','HOLX','HD','HON','HRL','HST','HWM','HPQ','HUBB','HUM','HBAN','HII',
    'IBM','IEX','IDXX','ITW','INCY','IR','INTC','ICE','IFF','IP','IPG','INTU','ISRG',
    'IVZ','INVH','IQV','IRM','JBHT','JBL','JKHY','J','JNJ','JCI','JPM','JNPR','K',
    'KDP','KEY','KEYS','KMB','KIM','KMI','KLAC','KHC','KR','LHX','LH','LRCX','LW',
    'LVS','LDOS','LEN','LIN','LYV','LKQ','LMT','L','LOW','LULU','LYB','MTB','MRO',
    'MPC','MKTX','MAR','MMC','MLM','MAS','MA','MTCH','MKC','MCD','MCK','MDT','MRK',
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
    'ADBE','AMD','ABNB','GOOGL','GOOG','AMZN','AEP','AMGN','ADI','ANSS','AAPL','AMAT',
    'ASML','TEAM','ADSK','ADP','AXON','BIIB','BKNG','AVGO','CDNS','CDW','CHTR','CTAS',
    'CSCO','CTSH','CMCSA','CEG','CPRT','CSGP','COST','CRWD','CSX','DDOG','DXCM','FANG',
    'DLTR','EBAY','EA','EXC','FAST','FTNT','GILD','HON','IDXX','INTC','INTU','ISRG',
    'KDP','KLAC','KHC','LRCX','LULU','MAR','MRVL','MELI','META','MCHP','MU','MSFT',
    'MRNA','MDLZ','MDB','MNST','NFLX','NVDA','NXPI','ORLY','ON','PCAR','PANW','PAYX',
    'PYPL','PDD','PEP','QCOM','REGN','ROP','ROST','SBUX','SNPS','TTWO','TMUS','TSLA',
    'TXN','TTD','VRSK','VRTX','WDAY','XEL','ZS'
]

ALL_TICKERS = sorted(list(set(SP500 + NASDAQ100)))

# ============================================================
# FUNCIONES SMC
# ============================================================

def calculate_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def find_swing_points(high, low, length):
    swing_highs, swing_lows = [], []
    n = len(high)
    for i in range(length, n):
        if high[i-length] == max(high[i-length:i]):
            swing_highs.append((i-length, high[i-length]))
        if low[i-length] == min(low[i-length:i]):
            swing_lows.append((i-length, low[i-length]))
    return swing_highs, swing_lows


def get_trailing_extremes(high_arr, low_arr, length):
    swing_highs, swing_lows = find_swing_points(high_arr, low_arr, length)
    if not swing_highs or not swing_lows:
        return None, None
    top    = max(v for _, v in swing_highs[-5:])
    bottom = min(v for _, v in swing_lows[-5:])
    return top, bottom


def calculate_zones(top, bottom):
    rango = top - bottom
    return {
        'discount':      (bottom,                        bottom + DISCOUNT_PCT * rango),
        'near_discount': (bottom,                        bottom + NEAR_DISCOUNT_PCT * rango),
        'equilibrium':   (bottom + 0.45 * rango,         bottom + 0.55 * rango),
        'premium':       (bottom + PREMIUM_PCT * rango,  top),
    }


def detect_bullish_ob(high_arr, low_arr, close_arr, swing_lows):
    if not swing_lows:
        return 'N/A'
    last_low_idx = swing_lows[-1][0]
    search_start = max(1, last_low_idx - 10)
    search_end   = min(len(close_arr) - 1, last_low_idx + 3)
    for i in range(search_end, search_start, -1):
        if close_arr[i] < close_arr[i-1]:
            return f'{low_arr[i]:.2f}-{high_arr[i]:.2f}'
    return 'N/A'


def analyze_ticker(ticker):
    try:
        df = yf.download(ticker, period=DATA_PERIOD, interval='1d',
                         progress=False, auto_adjust=True)
        if df is None or len(df) < SWING_LENGTH + 20:
            return None

        df        = df.dropna()
        high_arr  = df['High'].values.flatten()
        low_arr   = df['Low'].values.flatten()
        close_arr = df['Close'].values.flatten()
        vol_arr   = df['Volume'].values.flatten()

        top, bottom = get_trailing_extremes(high_arr, low_arr, SWING_LENGTH)
        if top is None or top == bottom:
            return None

        zones = calculate_zones(top, bottom)
        price = float(close_arr[-1])
        rango = top - bottom

        pct_en_rango = (price - bottom) / rango * 100

        in_discount      = zones['discount'][0]      <= price <= zones['discount'][1]
        in_near_discount = zones['near_discount'][0]  <= price <= zones['near_discount'][1]

        if not in_near_discount:
            return None

        zona_label = 'Discount' if in_discount else 'Near Discount'

        nd_range    = zones['near_discount'][1] - zones['near_discount'][0]
        pct_in_zone = ((price - bottom) / nd_range * 100) if nd_range > 0 else 0

        rsi       = round(float(calculate_rsi(pd.Series(close_arr)).iloc[-1]), 1)
        avg_vol   = float(np.mean(vol_arr[-21:-1]))
        vol_ratio = round(float(vol_arr[-1]) / avg_vol, 2) if avg_vol > 0 else 0

        swing_highs, swing_lows = find_swing_points(high_arr, low_arr, SWING_LENGTH)
        if len(swing_highs) >= 2:
            trend = 'Bullish' if swing_highs[-1][1] > swing_highs[-2][1] else 'Bearish'
        else:
            trend = 'N/A'

        ob_str = detect_bullish_ob(high_arr, low_arr, close_arr, swing_lows)

        return {
            'Ticker':        ticker,
            'Zona':          zona_label,
            'Precio':        round(price, 2),
            'Swing_High':    round(float(top), 2),
            'Swing_Low':     round(float(bottom), 2),
            'Pct_Rango':     round(pct_en_rango, 1),
            'Pct_Zona':      round(pct_in_zone, 1),
            'Dist_Low_Pct':  round((price - bottom) / bottom * 100, 2),
            'OB_Bullish':    ob_str,
            'RSI':           rsi,
            'Vol_Ratio_20d': vol_ratio,
            'Tendencia':     trend,
            'TradingView':   f'https://www.tradingview.com/chart/?symbol={ticker}'
        }

    except Exception as e:
        print(f'  Warning {ticker}: {e}')
        return None


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print('SMC Discount Zone Screener v2')
    print(f'Universo   : {len(ALL_TICKERS)} tickers')
    print(f'Swing Len  : {SWING_LENGTH} | Discount: {int(DISCOUNT_PCT*100)}% | Near: {int(NEAR_DISCOUNT_PCT*100)}%')
    print(f'Inicio     : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('-' * 65)

    results = []
    total   = len(ALL_TICKERS)

    for i, ticker in enumerate(ALL_TICKERS):
        result = analyze_ticker(ticker)
        if result:
            results.append(result)
            print(f'  HIT [{i+1:3}/{total}] {ticker:6} | {result["Zona"]:14} | '
                  f'Rango: {result["Pct_Rango"]:5.1f}% | RSI: {result["RSI"]:5.1f} | {result["Tendencia"]}')
        else:
            print(f'  --- [{i+1:3}/{total}] {ticker}')

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    print('-' * 65)
    print(f'Resultado: {len(results)} acciones en zona de interes de {total}')

    if results:
        df_out = pd.DataFrame(results)
        zona_order = {'Discount': 0, 'Near Discount': 1}
        df_out['_ord'] = df_out['Zona'].map(zona_order).fillna(2)
        df_out = df_out.sort_values(['_ord', 'Pct_Rango']).drop(columns=['_ord'])

        os.makedirs('results', exist_ok=True)
        date_str = datetime.now().strftime('%Y%m%d_%H%M')
        df_out.to_csv(f'results/smc_discount_{date_str}.csv', index=False)
        df_out.to_csv('results/smc_discount_latest.csv', index=False)
        print(f'Guardado: results/smc_discount_latest.csv')
        print()
        print('TOP 15:')
        cols = ['Ticker','Zona','Precio','Pct_Rango','RSI','Vol_Ratio_20d','Tendencia','OB_Bullish']
        print(df_out[cols].head(15).to_string(index=False))
    else:
        print('Sin resultados. Aumenta NEAR_DISCOUNT_PCT en el script.')
