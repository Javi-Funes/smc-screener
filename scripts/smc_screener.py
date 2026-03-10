"""
SMC Discount Zone Screener
Replica la lógica de LuxAlgo Smart Money Concepts
Corre en GitHub Actions y guarda resultados como CSV
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
# PARÁMETROS
# ============================================================
SWING_LENGTH  = 50
DATA_PERIOD   = '1y'
BATCH_SIZE    = 10
SLEEP_BETWEEN = 1.0

# ============================================================
# TICKERS — S&P 500 + NASDAQ 100
# ============================================================
SP500 = [
    'MMM','AOS','ABT','ABBV','ACN','ADBE','AMD','AES','AFL','A','APD','ABNB','AKAM','ALB',
    'ALGN','ALLE','LNT','ALL','GOOGL','GOOG','MO','AMZN','AMCR','AEE','AAL','AEP','AXP',
    'AIG','AMT','AWK','AMP','AME','AMGN','APH','ADI','ANSS','AON','APA','AAPL','AMAT',
    'APTV','ACGL','ADM','ANET','AJG','AIZ','T','ATO','ADSK','ADP','AZO','AVB','AVY',
    'AXON','BKR','BALL','BAC','BK','BBWI','BAX','BDX','BBY','BIO','BIIB','BLK','BX',
    'BA','BSX','BMY','AVGO','BR','BRO','BG','CDNS','CPT','CPB','COF','CAH','KMX','CCL',
    'CARR','CAT','CBOE','CBRE','CDW','CE','COR','CNC','CDAY','CF','CRL','SCHW','CHTR',
    'CVX','CMG','CB','CHD','CI','CINF','CTAS','CSCO','C','CFG','CLX','CME','CMS','KO',
    'CTSH','CL','CMCSA','CAG','COP','ED','STZ','CEG','COO','CPRT','GLW','CTVA','CSGP',
    'COST','CTRA','CCI','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','DVN',
    'DXCM','FANG','DLR','DFS','DG','DLTR','D','DPZ','DOV','DOW','DTE','DUK','DD',
    'EMN','ETN','EBAY','ECL','EIX','EW','EA','ELV','LLY','EMR','ENPH','ETR','EOG',
    'EQT','EFX','EQIX','EQR','ESS','EL','ETSY','EG','EVRG','ES','EXC','EXPE','EXPD',
    'EXR','XOM','FFIV','FDS','FICO','FAST','FRT','FDX','FIS','FITB','FSLR','FE','FI',
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
# FUNCIONES SMC — replica lógica LuxAlgo
# ============================================================

def calculate_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def find_swing_points(high, low, length=50):
    """Replica función leg() de LuxAlgo"""
    swing_highs, swing_lows = [], []
    n = len(high)
    for i in range(length, n):
        window_high = high[i-length:i]
        window_low  = low[i-length:i]
        if high[i-length] == max(window_high):
            swing_highs.append((i-length, high[i-length]))
        if low[i-length] == min(window_low):
            swing_lows.append((i-length, low[i-length]))
    return swing_highs, swing_lows


def get_trailing_extremes(high_arr, low_arr, length=50):
    """Replica trailing.top y trailing.bottom de LuxAlgo"""
    swing_highs, swing_lows = find_swing_points(high_arr, low_arr, length)
    if not swing_highs or not swing_lows:
        return None, None
    trailing_top    = max(v for _, v in swing_highs[-5:])
    trailing_bottom = min(v for _, v in swing_lows[-5:])
    return trailing_top, trailing_bottom


def calculate_zones(top, bottom):
    """
    Replica exacta de drawPremiumDiscountZones() de LuxAlgo:
      Premium:     0.95*T + 0.05*B  →  T
      Equilibrium: 0.525*B + 0.475*T → 0.525*T + 0.475*B
      Discount:    B  →  0.95*B + 0.05*T
    """
    T, B = top, bottom
    return {
        'premium':     (0.95*T + 0.05*B,  T),
        'equilibrium': (0.525*B + 0.475*T, 0.525*T + 0.475*B),
        'discount':    (B, 0.95*B + 0.05*T)
    }


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
        if top is None:
            return None

        zones         = calculate_zones(top, bottom)
        price         = float(close_arr[-1])
        disc_low      = float(zones['discount'][0])
        disc_high     = float(zones['discount'][1])

        if not (disc_low <= price <= disc_high):
            return None

        # % dentro de la zona (0% = fondo, 100% = techo)
        zone_range  = disc_high - disc_low
        pct_in_zone = ((price - disc_low) / zone_range * 100) if zone_range > 0 else 0

        # RSI
        rsi = round(float(calculate_rsi(pd.Series(close_arr)).iloc[-1]), 1)

        # Volumen ratio vs 20d
        avg_vol   = float(np.mean(vol_arr[-21:-1]))
        vol_ratio = round(float(vol_arr[-1]) / avg_vol, 2) if avg_vol > 0 else 0

        # Tendencia
        swing_highs, _ = find_swing_points(high_arr, low_arr, SWING_LENGTH)
        if len(swing_highs) >= 2:
            trend = 'Bullish' if swing_highs[-1][1] > swing_highs[-2][1] else 'Bearish'
        else:
            trend = 'N/A'

        return {
            'Ticker':          ticker,
            'Precio':          round(price, 2),
            'Swing_High':      round(float(top), 2),
            'Swing_Low':       round(float(bottom), 2),
            'Discount_Top':    round(disc_high, 2),
            'Discount_Bot':    round(disc_low, 2),
            'Pct_en_Zona':     round(pct_in_zone, 1),
            'Pct_desde_Low':   round((price - bottom) / bottom * 100, 2),
            'RSI':             rsi,
            'Vol_Ratio_20d':   vol_ratio,
            'Tendencia':       trend,
            'TradingView':     f'https://www.tradingview.com/chart/?symbol={ticker}'
        }
    except Exception as e:
        print(f'  ⚠ Error {ticker}: {e}')
        return None


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print(f'🔍 SMC Discount Zone Screener')
    print(f'   Universo  : {len(ALL_TICKERS)} tickers (S&P500 + NASDAQ100)')
    print(f'   Timeframe : Diario | Período: {DATA_PERIOD}')
    print(f'   Inicio    : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('─' * 55)

    results = []
    total   = len(ALL_TICKERS)

    for i, ticker in enumerate(ALL_TICKERS):
        result = analyze_ticker(ticker)
        if result:
            results.append(result)
            print(f'  ✅ [{i+1:3}/{total}] {ticker:6} → En Discount | RSI: {result["RSI"]} | Tendencia: {result["Tendencia"]}')
        else:
            print(f'  ·  [{i+1:3}/{total}] {ticker:6}')

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)

    print('─' * 55)
    print(f'✅ Scan completo: {len(results)} acciones en Discount de {total}')

    if results:
        df_out = pd.DataFrame(results).sort_values('Pct_en_Zona')

        # Guardar CSV
        os.makedirs('results', exist_ok=True)
        date_str  = datetime.now().strftime('%Y%m%d_%H%M')
        filename  = f'results/smc_discount_{date_str}.csv'
        df_out.to_csv(filename, index=False)
        # También guardar como "latest" para fácil acceso
        df_out.to_csv('results/smc_discount_latest.csv', index=False)
        print(f'💾 Guardado: {filename}')
        print(f'💾 Guardado: results/smc_discount_latest.csv')

        print('\n📋 TOP 10 mejores setups:')
        print(df_out[['Ticker','Precio','Pct_en_Zona','RSI','Vol_Ratio_20d','Tendencia']].head(10).to_string(index=False))
    else:
        print('⚠️  No se encontraron acciones en zona Discount.')
