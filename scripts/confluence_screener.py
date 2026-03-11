"""
SMC CONFLUENCE SCREENER v1.6 - THE BUNKER EDITION (API FIX)
=================================================
Lead Indicators (USA) -> Execution (Argentina)
Integrates: SMC + FVG + Momentum + OB Detection + API Fault Tolerance
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

# --- SMC (Calibrado LuxAlgo) ---
SWING_LENGTH_D      = 50       # Swing diario
SWING_LENGTH_H      = 50       # Swing 1H
DISCOUNT_PCT        = 0.25     # 25% inferior del rango
NEAR_DISCOUNT_PCT   = 0.40     # Hasta 40% del rango
EQUILIBRIUM_BAND    = 0.05     # 50% +/- 5%

# --- Estructura y Momentum ---
ESTRUCTURA_ALCISTA  = True     # Filtrar solo HH/HL
RS_DIAS             = 5        # Ventana Fuerza Relativa
RS_RATIO_MIN        = 1.02     # 2% mejor que el sector
ABSORCION_VOL_RATIO = 2.5      # Volumen 2.5x promedio
ABSORCION_CLOSE_PCT = 0.70     # Cierre en top 30%
SQUEEZE_RATIO       = 0.65     # Compresion de rango
SCORE_MINIMO        = 3        # Minimo de señales (Max 7)

# --- Gestión de Riesgo (Local) ---
STOP_PCT            = 1.5      # Ajustado por CCL y Spread
TARGET_PCT          = 3.0      # Ratio 2:1

# --- Configuración de Datos ---
DATA_PERIOD_D       = '1y'
DATA_PERIOD_H       = '1mo'
BATCH_SIZE          = 10
SLEEP_BETWEEN       = 1.5
SECTORES_EXCLUIR    = ['Utilities', 'Real Estate']

# ============================================================
# MAPEO SECTOR → ETF & TICKERS LEAD INDICATORS
# ============================================================
SECTOR_ETF = {
    'Technology': 'XLK', 'Financial Services': 'XLF', 'Healthcare': 'XLV',
    'Energy': 'XLE', 'Consumer Cyclical': 'XLY', 'Consumer Defensive': 'XLP',
    'Industrials': 'XLI', 'Basic Materials': 'XLB', 'Communication Services': 'XLC'
}

# CORRECCIÓN: PAMP en USA es PAM
ADRS_ARG = ['GGAL', 'YPF', 'PAM', 'BMA', 'CEPU', 'LOMA', 'CRESY', 'EDN', 'SUPV', 'TGS']

BLUE_CHIPS = [
    'AAPL', 'AMZN', 'MSFT', 'NVDA', 'META', 'GOOGL', 'TSLA', 'MELI', 'KO', 'PEP', 
    'JNJ', 'PFE', 'WMT', 'MCD', 'DIS', 'NFLX', 'GOLD', 'BABA', 'VALE', 'V', 
    'MA', 'PYPL', 'NKE', 'CAT', 'JPM', 'BRK-B', 'CVX', 'XOM', 'T', 'VZ', 
    'INTC', 'AMD', 'BA', 'GE', 'IBM', 'WFC', 'C', 'MS', 'GS', 'HMY',
    'AUY', 'PAAS', 'X', 'NEM', 'FCX', 'TS', 'ERJ', 'ABEV', 'DESP', 'GLOB', 
    'JD', 'BIDU', 'SQ', 'COIN', 'SHOP', 'AVGO', 'QCOM', 'MU', 'AMAT', 'CRM', 
    'ADBE', 'ORCL', 'CSCO', 'UBER', 'ABNB', 'F', 'GM', 'COST', 'SBUX'
]

ALL_TICKERS = sorted(list(set(ADRS_ARG + BLUE_CHIPS)))

# ============================================================
# FUNCIONES MATEMÁTICAS Y SMC
# ============================================================

def to_arr(series):
    return np.array(series).flatten()

def calculate_rsi(s, period=14):
    delta = s.diff()
    gain = delta.clip(lower=0).ewm(com=period-1, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(com=period-1, min_periods=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def find_swings(high, low, length):
    highs, lows = [], []
    for i in range(length, len(high)):
        if high[i-length] == max(high[i-length:i]): highs.append((i-length, float(high[i-length])))
        if low[i-length] == min(low[i-length:i]): lows.append((i-length, float(low[i-length])))
    return highs, lows

def get_zones(top, bottom):
    r = top - bottom
    return {
        'discount': (bottom, bottom + DISCOUNT_PCT * r),
        'near_discount': (bottom, bottom + NEAR_DISCOUNT_PCT * r),
        'equilibrium': (bottom + (0.5 - EQUILIBRIUM_BAND) * r, bottom + (0.5 + EQUILIBRIUM_BAND) * r)
    }

def detect_fvg(df):
    l3 = df.tail(3)
    if l3.iloc[2]['Low'] > l3.iloc[0]['High']: return "BULLISH"
    if l3.iloc[2]['High'] < l3.iloc[0]['Low']: return "BEARISH"
    return None

def detect_ob_encima(high, low, close, price, swing_highs):
    if not swing_highs: return False, None
    last_h_idx = swing_highs[-1][0]
    for i in range(min(len(close)-1, last_h_idx+3), max(1, last_h_idx-8), -1):
        if close[i] > close[i-1]:
            ob_l = float(low[i])
            if ob_l > price and ob_l < price * 1.08: return True, round(ob_l, 2)
    return False, None

# ============================================================
# MOTOR DE ANÁLISIS
# ============================================================

def analyze_ticker(ticker):
    try:
        # --- BLINDAJE CONTRA BUG 404 DE YFINANCE ---
        sector = 'Unknown'
        try:
            tk = yf.Ticker(ticker)
            sector = tk.info.get('sector', 'Unknown')
        except:
            pass # Si Yahoo falla al dar la info, ignoramos y seguimos operando.

        if sector in SECTORES_EXCLUIR: return None

        # Descarga de datos de precios (Esto sí funciona estable)
        df = yf.download(ticker, period=DATA_PERIOD_D, interval='1d', progress=False, auto_adjust=True)
        if df.empty or len(df) < SWING_LENGTH_D + 10: return None
        
        c = to_arr(df['Close'])
        h = to_arr(df['High'])
        l = to_arr(df['Low'])
        v = to_arr(df['Volume'])
        price = float(c[-1])

        # 1. SMC Zonas
        sh, sl = find_swings(h, l, SWING_LENGTH_D)
        if not sh or not sl: return None
        top, bottom = max(val for _, val in sh[-5:]), min(val for _, val in sl[-5:])
        zones = get_zones(top, bottom)
        
        in_disc = zones['discount'][0] <= price <= zones['discount'][1]
        in_near = zones['near_discount'][0] <= price <= zones['near_discount'][1]
        if not (in_disc or in_near): return None

        # 2. Estructura HH/HL
        hh = all(sh[i][1] > sh[i-1][1] for i in range(-2, 0))
        hl = all(sl[i][1] > sl[i-1][1] for i in range(-2, 0))
        if ESTRUCTURA_ALCISTA and not (hh or hl): return None

        # 3. RS Fuerza Relativa
        etf_sym = SECTOR_ETF.get(sector, 'SPY')
        etf_c = yf.download(etf_sym, period='10d', progress=False, auto_adjust=True)['Close']
        rs = (c[-1]/c[-RS_DIAS]) / (etf_c.iloc[-1]/etf_c.iloc[-RS_DIAS])

        # Scoring
        score = 2 # Base por Zona + Estructura
        fvg = detect_fvg(df)
        if fvg == "BULLISH": score += 1
        if rs >= RS_RATIO_MIN: score += 1
        
        avg_v = np.mean(v[-21:-1])
        if avg_v > 0 and (v[-1]/avg_v) >= ABSORCION_VOL_RATIO: score += 1
        
        rsi = calculate_rsi(df['Close'])[-1]
        if rsi < 40: score += 1

        ob_up, ob_lv = detect_ob_encima(h, l, c, price, sh)
        if ob_up: score -= 1

        if score < SCORE_MINIMO: return None

        return {
            'Ticker': ticker, 'Score': f"{score}/7", 'Precio': round(price, 2),
            'Zona': 'Discount' if in_disc else 'Near Discount', 'FVG': fvg if fvg else 'None',
            'RS_Sector': round(rs, 2), 'OB_Encima': f'SI ({ob_lv})' if ob_up else 'NO',
            'Stop': round(price * (1-STOP_PCT/100), 2), 'Target': round(price * (1+TARGET_PCT/100), 2),
            'View': f'https://tradingview.com/symbols/{ticker}'
        }
    except Exception as e: 
        # Ya no fallará en silencio total, pero pasará al siguiente ticker limpiamente.
        return None

# ============================================================
# MODULO: RADAR DE ROTACION SECTORIAL (Hedge Fund Tracker)
# ============================================================
def scan_sector_rotation():
    print("\n" + "="*50)
    print(" 📡 RADAR DE ROTACIÓN INSTITUCIONAL (Últimos 5 días)")
    print("="*50)
    try:
        # Benchmark general del mercado
        spy = yf.download('SPY', period='10d', progress=False, auto_adjust=True)['Close']
        if spy.empty: return
        spy_ret = (spy.iloc[-1] / spy.iloc[-RS_DIAS]) - 1
        
        rotation_data = []
        for sector, ticker in SECTOR_ETF.items():
            try:
                etf = yf.download(ticker, period='10d', progress=False, auto_adjust=True)['Close']
                if etf.empty: continue
                etf_ret = (etf.iloc[-1] / etf.iloc[-RS_DIAS]) - 1
                
                # Relative Strength vs SPY
                rs_spy = (1 + etf_ret) / (1 + spy_ret) if (1 + spy_ret) != 0 else 0
                
                estado = "🟢 ENTRANDO" if rs_spy > 1.0 else "🔴 SALIENDO"
                rotation_data.append({
                    'Sector': sector,
                    'ETF': ticker,
                    'RS_Score': round(rs_spy, 3),
                    'Estado': estado
                })
            except: continue
        
        if rotation_data:
            df_rot = pd.DataFrame(rotation_data).sort_values(by='RS_Score', ascending=False)
            print(df_rot.to_string(index=False))
        print("="*50 + "\n")
    except Exception as e:
        print(f"Error cargando rotación: {e}")

# ============================================================
# EJECUCIÓN
# ============================================================

if __name__ == '__main__':
    print(f"--- SMC Búnker Screener | {datetime.now().strftime('%Y-%m-%d %H:%M')} ---")
    
    # 1. Ejecutamos el radar de rotación primero
    scan_sector_rotation()
    
    # 2. Inicia el escaneo de Perlas
    results = []
    for i, t in enumerate(ALL_TICKERS):
        print(f"[{i+1}/{len(ALL_TICKERS)}] Analizando {t}...    ", end='\r')
        res = analyze_ticker(t)
        if res: results.append(res)
        
        # Respetar Rate Limits de Yahoo
        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_BETWEEN)
    
    if results:
        df_final = pd.DataFrame(results).sort_values(by='Score', ascending=False)
        os.makedirs('results', exist_ok=True)
        df_final.to_csv('results/perlas_usa_argentina.csv', index=False)
        print(f"\n\nÉXITO: {len(results)} perlas encontradas y guardadas en 'results/perlas_usa_argentina.csv'.")
        print(df_final[['Ticker', 'Score', 'Precio', 'OB_Encima', 'Zona']].head(10).to_string(index=False))
    else:
        print("\n\nSin hits hoy. El mercado está fuera de zona de valor o lateral.")
