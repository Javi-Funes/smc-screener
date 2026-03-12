"""
SMC REPORTE DIARIO — Argentina Edition
=======================================
Universo: 52 CEDEARs + 11 ADRs argentinos + 11 Panel Lider BYMA
Salida: HTML monoespacio (mismo formato texto) + Telegram con link
Horario: 7AM ARG via GitHub Actions
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
import html as html_lib
import re
from datetime import datetime
from zoneinfo import ZoneInfo

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('smc')
TZ_ARG = ZoneInfo('America/Argentina/Buenos_Aires')
def now_arg(): return datetime.now(TZ_ARG)

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
# UNIVERSO
# ============================================================
CEDEARS_NYSE = [
    'MELI','VIST','NU','MSFT','ORCL','HMY','SPY','USO','HUT','IBIT',
    'MU','NIO','NVDA','PBR','GOOGL','MSTR','AMZN','SQ','MO','KO',
    'AAPL','SATL','EWZ','SH','ASTS','TSLA','VXX','BRK-B','UNH','META',
    'ETHA','IBM','CRM','GLD','GLOB','COIN','QQQ','SLV','AMD','BAC',
    'V','URA','STNE','CSCO','ANF','XLE','PLTR','GOLD','MRNA','BITF',
    'PAGS','GPRK',
]
ADRS_ARG_NYSE = {
    'GGAL':{'byma':'GGAL.BA','nombre':'Grupo Galicia','ratio':10},
    'YPF': {'byma':'YPFD.BA','nombre':'YPF','ratio':1},
    'PAM': {'byma':'PAMP.BA','nombre':'Pampa Energia','ratio':25},
    'BMA': {'byma':'BMA.BA', 'nombre':'Banco Macro','ratio':10},
    'CEPU':{'byma':'CEPU.BA','nombre':'Central Puerto','ratio':10},
    'LOMA':{'byma':'LOMA.BA','nombre':'Loma Negra','ratio':5},
    'SUPV':{'byma':'SUPV.BA','nombre':'Supervielle','ratio':5},
    'TGS': {'byma':'TGSU2.BA','nombre':'TGS','ratio':5},
    'MELI':{'byma':'MELI.BA','nombre':'MercadoLibre','ratio':1},
    'GLOB':{'byma':'GLOB.BA','nombre':'Globant','ratio':1},
    'DESP':{'byma':'DESP.BA','nombre':'Despegar','ratio':1},
}
PANEL_LIDER_BYMA = {
    'TXAR.BA':'Ternium Argentina','ALUA.BA':'Aluar','BBAR.BA':'BBVA Argentina',
    'TECO2.BA':'Telecom Argentina','COME.BA':'Soc. Comercial del Plata',
    'CVH.BA':'Cablevision Holding','MIRG.BA':'Mirgor','BYMA.BA':'BYMA',
    'VALO.BA':'Grupo Valores','EDN.BA':'Edenor','CRES.BA':'Cresud',
}
SECTOR_ETF = {
    'Technology':'XLK','Financial Services':'XLF','Healthcare':'XLV',
    'Energy':'XLE','Consumer Cyclical':'XLY','Consumer Defensive':'XLP',
    'Industrials':'XLI','Basic Materials':'XLB','Communication Services':'XLC',
}

# ============================================================
# SMC HELPERS
# ============================================================
def to_arr(s): return np.array(s).flatten()

def calculate_rsi(arr, period=14):
    s=pd.Series(arr); delta=s.diff()
    gain=delta.clip(lower=0).ewm(com=period-1,min_periods=period).mean()
    loss=(-delta.clip(upper=0)).ewm(com=period-1,min_periods=period).mean()
    rs=gain/loss.replace(0,np.nan)
    return to_arr(100-(100/(1+rs)))

def find_swings(high,low,length):
    sh,sl=[],[]
    for i in range(length,len(high)):
        wh=high[i-length:i]; wl=low[i-length:i]
        if len(wh)==0: continue
        if high[i-length]==np.max(wh): sh.append((i-length,float(high[i-length])))
        if low[i-length]==np.min(wl):  sl.append((i-length,float(low[i-length])))
    return sh,sl

def get_zones(top,bottom):
    r=top-bottom
    return {
        'discount':(bottom,bottom+DISCOUNT_PCT*r),
        'near_discount':(bottom,bottom+NEAR_DISCOUNT_PCT*r),
    }

def get_estructura(sh,sl):
    if len(sh)<2 or len(sl)<2: return 'Indefinida'
    uh=[v for _,v in sh[-3:]]; ul=[v for _,v in sl[-3:]]
    hh=all(uh[i]>uh[i-1] for i in range(1,len(uh)))
    hl=all(ul[i]>ul[i-1] for i in range(1,len(ul)))
    lh=all(uh[i]<uh[i-1] for i in range(1,len(uh)))
    ll=all(ul[i]<ul[i-1] for i in range(1,len(ul)))
    if hh and hl: return 'Alcista'
    if lh and ll: return 'Bajista'
    if hh or hl:  return 'Alcista Debil'
    if lh or ll:  return 'Bajista Debil'
    return 'Lateral'

def detect_ob_encima(high,low,close,price,sh):
    if not sh: return False,None
    idx=sh[-1][0]; start=min(len(close)-1,idx+3); end=max(1,idx-8)
    if start<=end: return False,None
    for i in range(start,end,-1):
        if i>=len(close) or i<1: continue
        if close[i]>close[i-1]:
            ob_l=float(low[i])
            if ob_l>price and ob_l<price*1.08: return True,round(ob_l,2)
    return False,None

def detect_fvg_all(high,low,close,price,lookback=30):
    fvgs=[]; n=min(lookback,len(close)-3)
    for i in range(2,n+2):
        idx=-i
        try:
            h0=float(high[idx-1]); l0=float(low[idx-1])
            h2=float(high[idx+1]); l2=float(low[idx+1])
        except IndexError: continue
        if l2>h0 and price>float(low[idx-1]):
            fvgs.append({'tipo':'BULLISH','low':round(h0,2),'high':round(l2,2),'mid':round((h0+l2)/2,2),'dist_pct':round((h0-price)/price*100,2),'relacion':'DEBAJO' if h0<price else 'ENCIMA'})
        if h2<l0 and price<float(high[idx-1]):
            fvgs.append({'tipo':'BEARISH','low':round(h2,2),'high':round(l0,2),'mid':round((h2+l0)/2,2),'dist_pct':round((l0-price)/price*100,2),'relacion':'ENCIMA' if l0>price else 'DEBAJO'})
    unique=[]
    for fvg in fvgs:
        if not any(abs(f['mid']-fvg['mid'])/max(fvg['mid'],0.01)<0.005 for f in unique): unique.append(fvg)
    unique.sort(key=lambda x:abs(x['dist_pct']))
    return unique[:6]

def calc_fibonacci_pois(sh,sl,price):
    if not sh or not sl: return [],[]
    last_sh_idx,last_sh_val=sh[-1]
    sl_antes=[(i,v) for i,v in sl if i<last_sh_idx]
    if not sl_antes: return [],[]
    _,imp_low=sl_antes[-1]; imp_high=last_sh_val; rango_imp=imp_high-imp_low
    if rango_imp<=0: return [],[]
    niveles=[(0.236,'23.6% Retroceso menor',False),(0.382,'38.2% POI moderado',False),
             (0.500,'50.0% Equilibrium',False),(0.618,'61.8% Golden Pocket',True),
             (0.650,'65.0% Golden Pocket ext.',True),(0.786,'78.6% Ultimo soporte',False)]
    retrocesos=[]
    for nivel,nombre,es_golden in niveles:
        precio_fib=round(imp_high-(rango_imp*nivel),2)
        retrocesos.append({'nivel':nivel,'nombre':nombre,'precio':precio_fib,
                           'dist_pct':round((precio_fib-price)/price*100,2),
                           'zona':'SOPORTE' if precio_fib<price else 'RESISTENCIA','es_golden':es_golden})
    extensiones=[]
    for nivel,nombre in [(1.272,'127.2% Extension 1'),(1.414,'141.4% Extension 2'),(1.618,'161.8% Extension dorada')]:
        precio_ext=round(imp_low+(rango_imp*nivel),2)
        extensiones.append({'nivel':nivel,'nombre':nombre,'precio':precio_ext,'dist_pct':round((precio_ext-price)/price*100,2)})
    return retrocesos,extensiones

def detect_absorcion(vol,close,high,low):
    if len(vol)<25: return False,0
    avg_vol=float(np.mean(vol[-21:-1]))
    if avg_vol==0: return False,0
    for lb in range(1,4):
        vr=float(vol[-lb])/avg_vol; rang=float(high[-lb])-float(low[-lb])
        if rang==0: continue
        cp=(float(close[-lb])-float(low[-lb]))/rang
        if vr>=ABSORCION_VOL_RATIO and cp>=0.70: return True,round(vr,2)
    return False,0

def detect_squeeze(high,low):
    if len(high)<20: return False
    rangos=high-low
    r_act=float(np.mean(rangos[-3:])); r_prev=float(np.mean(rangos[-20:-3]))
    if r_prev==0: return False
    return (r_act/r_prev)<=SQUEEZE_RATIO

# ============================================================
# RATIOS / CCL
# ============================================================
_RATIOS_CEDEARS={}; _ADRS_ARG_JSON={}

def _cargar_ratios():
    global _RATIOS_CEDEARS,_ADRS_ARG_JSON
    if not os.path.exists(_RATIOS_JSON):
        log.warning(f"{_RATIOS_JSON} no encontrado"); return
    try:
        with open(_RATIOS_JSON,'r',encoding='utf-8') as f: data=json.load(f)
        data.pop('_meta',None)
        for ticker,info in data.items():
            ratio=info.get('ratio',1)
            if not ratio or float(ratio)<=0: info['ratio']=1
            if info.get('tipo')=='ADR-Argentina': _ADRS_ARG_JSON[ticker]=info
            else: _RATIOS_CEDEARS[ticker]=info
        log.info(f"Ratios: {len(_RATIOS_CEDEARS)} CEDEARs + {len(_ADRS_ARG_JSON)} ADRs")
    except Exception as e: log.error(f"Error cargando ratios: {e}")

def get_ratio(ticker):
    info=_RATIOS_CEDEARS.get(ticker) or _ADRS_ARG_JSON.get(ticker)
    if info and info.get('ratio') and float(info['ratio'])>0: return float(info['ratio'])
    return float(ADRS_ARG_NYSE.get(ticker,{}).get('ratio',1))

def get_ccl():
    try:
        r=requests.get('https://criptoya.com/api/dolar',timeout=5)
        if r.status_code==200:
            data=r.json(); ccl=data.get('ccl',{})
            venta=ccl.get('ask') or ccl.get('venta') or ccl.get('price')
            if venta and float(venta)>100: return float(venta),'CriptoYa'
    except: pass
    try:
        r=requests.get('https://dolarapi.com/v1/dolares/contadoconliqui',timeout=5)
        if r.status_code==200:
            data=r.json(); venta=data.get('venta')
            if venta and float(venta)>100: return float(venta),'DolarAPI'
    except: pass
    return CCL_FALLBACK,'FALLBACK'

# ============================================================
# CACHE REFERENCIAS
# ============================================================
log.info('Cargando referencias (SPY + ETFs)...')
ref_data={}; sector_cache={}
for sym in ['SPY']+list(SECTOR_ETF.values()):
    try:
        df=yf.download(sym,period=DATA_PERIOD,interval='1d',progress=False,auto_adjust=True)
        if df is not None and len(df)>20: ref_data[sym]=df.dropna()
    except Exception as e: log.warning(f"No se pudo cargar {sym}: {e}")
log.info(f'Referencias cargadas: {len(ref_data)}')

def get_sector(ticker):
    if ticker in sector_cache: return sector_cache[ticker]
    try:
        s=yf.Ticker(ticker).info.get('sector','Unknown')
        sector_cache[ticker]=s; return s
    except: sector_cache[ticker]='Unknown'; return 'Unknown'

# ============================================================
# ANALISIS
# ============================================================
def analyze(ticker, es_byma=False):
    try:
        df=yf.download(ticker,period=DATA_PERIOD,interval='1d',progress=False,auto_adjust=True)
        if df is None or df.empty or len(df)<SWING_LENGTH+20: return None
        df=df.dropna()
        h=to_arr(df['High']); l=to_arr(df['Low']); c=to_arr(df['Close']); v=to_arr(df['Volume'])
        price=float(c[-1])
        sh,sl=find_swings(h,l,SWING_LENGTH)
        if not sh or not sl: return None
        top=max(val for _,val in sh[-5:]); bottom=min(val for _,val in sl[-5:])
        if top<=bottom: return None
        zones=get_zones(top,bottom); rango=top-bottom; pct_r=(price-bottom)/rango*100
        in_disc=zones['discount'][0]<=price<=zones['discount'][1]
        in_near=zones['near_discount'][0]<=price<=zones['near_discount'][1]
        if not (in_disc or in_near): return None
        zona='Discount' if in_disc else 'Near Discount'
        estructura=get_estructura(sh,sl)
        if ESTRUCTURA_ALCISTA and estructura not in ['Alcista','Alcista Debil']: return None
        ob_enc,ob_lvl=detect_ob_encima(h,l,c,price,sh)
        fvgs_all=detect_fvg_all(h,l,c,price)
        fvg_bull=any(f['tipo']=='BULLISH' for f in fvgs_all)
        fib_r,fib_e=calc_fibonacci_pois(sh,sl,price)
        abs_hit,abs_vol=detect_absorcion(v,c,h,l)
        sq_hit=detect_squeeze(h,l)
        rs_hit,rs_ratio=False,None
        if not es_byma:
            sector=get_sector(ticker); etf_sym=SECTOR_ETF.get(sector)
            etf_d=ref_data.get(etf_sym); spy_d=ref_data.get('SPY')
            if etf_d is not None and spy_d is not None:
                etf_c=to_arr(etf_d['Close']); spy_c=to_arr(spy_d['Close'])
                min_len=min(len(c),len(etf_c),len(spy_c))
                if min_len>RS_DIAS+1:
                    ret_t=float(c[-1])/float(c[-RS_DIAS])-1
                    ret_etf=float(etf_c[-1])/float(etf_c[-RS_DIAS])-1
                    ret_spy=float(spy_c[-1])/float(spy_c[-RS_DIAS])-1
                    rs_vs_etf=(1+ret_t)/(1+ret_etf) if (1+ret_etf)!=0 else 0
                    rs_etf_spy=(1+ret_etf)/(1+ret_spy) if (1+ret_spy)!=0 else 0
                    rs_hit=rs_vs_etf>=RS_RATIO_MIN and rs_etf_spy>=0.99
                    rs_ratio=round(rs_vs_etf,3)
        else: sector='Argentina'
        rsi=round(float(calculate_rsi(c)[-1]),1)
        avg_vol=float(np.mean(v[-21:-1]))
        vol_ratio=round(float(v[-1])/avg_vol,2) if avg_vol>0 else 0
        score=2
        if fvg_bull: score+=1
        if rs_hit:   score+=1
        if abs_hit:  score+=1
        if sq_hit:   score+=1
        if ob_enc:   score-=1
        if score<SCORE_MINIMO: return None
        equil=(top+bottom)/2
        tv_ticker=ticker.replace('.BA','')
        tv_exchange='BYMA' if es_byma else 'NASDAQ'
        return {
            'ticker':ticker,'sector':sector,'score':score,'zona':zona,'pct_rango':round(pct_r,1),
            'estructura':estructura,'precio':round(price,2),'swing_high':round(top,2),'swing_low':round(bottom,2),
            'equilibrium':round(equil,2),'dist_equil':round((equil-price)/price*100,2),
            'ob_encima':f'SI ({ob_lvl})' if ob_enc else 'NO','ob_enc_bool':ob_enc,
            'fvg':'BULLISH' if fvg_bull else ('BEARISH' if fvgs_all else 'None'),
            'fvgs_all':fvgs_all,'fib_retrocesos':fib_r,'fib_extensiones':fib_e,
            'rs_ratio':rs_ratio or '-','rsi':rsi,'vol_ratio':vol_ratio,
            'abs_vol':abs_vol or '-','squeeze':'SI' if sq_hit else 'NO',
            'target':round(price*(1+TARGET_PCT/100),2),'stop':round(price*(1-STOP_PCT/100),2),
            'tv_link':f'https://www.tradingview.com/chart/?symbol={tv_exchange}%3A{tv_ticker}',
        }
    except Exception as e:
        log.error(f"{ticker}: error — {e}"); return None

def get_rotacion():
    spy_d=ref_data.get('SPY')
    if spy_d is None: return [],0
    spy_c=to_arr(spy_d['Close']); spy_ret=float(spy_c[-1])/float(spy_c[-RS_DIAS])-1
    rows=[]
    for sector,etf in SECTOR_ETF.items():
        if etf not in ref_data: continue
        etf_c=to_arr(ref_data[etf]['Close']); etf_ret=float(etf_c[-1])/float(etf_c[-RS_DIAS])-1
        rs=(1+etf_ret)/(1+spy_ret) if (1+spy_ret)!=0 else 0
        rows.append({'sector':sector,'etf':etf,'ret_5d':round(etf_ret*100,2),'rs_spy':round(rs,3),'estado':'ENTRANDO' if rs>=1.0 else 'saliendo'})
    rows.sort(key=lambda x:x['rs_spy'],reverse=True)
    return rows,round(spy_ret*100,2)

# ============================================================
# REPORTE TEXTO
# ============================================================
def generar_reporte_texto(ccl, ccl_fuente, resultados_cedears, resultados_adrs,
                          resultados_byma, rotacion, spy_ret):
    now=now_arg().strftime('%d/%m/%Y %H:%M')
    lines=[]; L=lines.append

    L('='*65)
    L('   SMC CONFLUENCE SCREENER — REPORTE DIARIO')
    L(f'   {now} (ARG) — Swing Length: {SWING_LENGTH} (LuxAlgo)')
    L('='*65)
    L(f'  CCL: ${ccl:,.2f} pesos/USD  (fuente: {ccl_fuente})')
    L('')
    L('SECCION 1: CONTEXTO DE MERCADO')
    L('-'*65)
    spy_txt='sube' if spy_ret>=0 else 'baja'
    L(f'  SPY (ultimos {RS_DIAS} dias): {spy_ret:+.2f}% ({spy_txt})')
    L('')
    L('  ROTACION SECTORIAL:')
    L(f'  {"Sector":<25} {"ETF":<5} {"Ret5d":>7}  {"RS/SPY":>7}  Capital')
    L('  '+'-'*55)
    sectores_ok=[]
    for r in rotacion:
        estado_str='ENTRANDO [OK]' if r['estado']=='ENTRANDO' else 'saliendo [--]'
        L(f'  {r["sector"]:<25} {r["etf"]:<5} {r["ret_5d"]:>+6.2f}%  {r["rs_spy"]:>7.3f}  {estado_str}')
        if r['estado']=='ENTRANDO': sectores_ok.append(r['etf'])
    L('')
    if sectores_ok:
        L(f'  CONCLUSION: Capital entrando en {", ".join(sectores_ok[:3])}')
        L(f'  Operar preferentemente acciones de esos sectores hoy.')
    else:
        L('  CONCLUSION: Mercado defensivo — ser muy selectivo hoy.')

    def fmt_resultado(r, ccl_val, es_adr=False):
        out=[]; O=out.append
        medal='[*]' if r['zona']=='Discount' else '   '
        ob_warn=' [!]' if r['ob_enc_bool'] else ''
        dc=' [DOBLE CONFLUENCIA]' if es_adr and r['ticker'] in ADRS_ARG_NYSE else ''
        O(f'  TICKER: {r["ticker"]}  |  Score: {r["score"]}/7  |  {r["sector"]}{dc}')
        O('  '+'─'*60)
        O(f'  Precio:         ${r["precio"]:,.2f}')
        O(f'  Zona SMC:       {medal} {r["zona"]}  ({r["pct_rango"]:.1f}% del rango)')
        O(f'  Swing High:     ${r["swing_high"]:,.2f}   Swing Low: ${r["swing_low"]:,.2f}')
        O(f'  Equilibrium:    ${r["equilibrium"]:,.2f}  (iman +{r["dist_equil"]:.1f}%)')
        O(f'  Estructura:     {r["estructura"]}')
        O(f'  OB Encima:      {r["ob_encima"]}{ob_warn}')
        O(f'  RS vs Sector:   {r["rs_ratio"]}')
        O(f'  RSI Diario:     {r["rsi"]}')
        O(f'  Vol Ratio 20d:  {r["vol_ratio"]}x')
        fvgs=r.get('fvgs_all',[])
        fvgs_enc=[f for f in fvgs if f['relacion']=='ENCIMA']
        fvgs_deb=[f for f in fvgs if f['relacion']=='DEBAJO']
        if fvgs:
            O('  -- FVGs ACTIVOS --')
            if fvgs_enc:
                O('  Encima (resistencia):')
                for f in fvgs_enc[:3]: O(f'    [{f["tipo"]}] ${f["low"]:,.2f}-${f["high"]:,.2f} mid:${f["mid"]:,.2f} [{f["dist_pct"]:+.1f}%]')
            if fvgs_deb:
                O('  Debajo (soporte):')
                for f in fvgs_deb[:3]: O(f'    [{f["tipo"]}] ${f["low"]:,.2f}-${f["high"]:,.2f} mid:${f["mid"]:,.2f} [{f["dist_pct"]:+.1f}%]')
        fibs_r=r.get('fib_retrocesos',[]); fibs_e=r.get('fib_extensiones',[])
        if fibs_r:
            O('  -- FIBONACCI --')
            for f in fibs_r:
                star=' [GP]' if f['es_golden'] else ''
                here=' <- AQUI' if abs(f['dist_pct'])<1.0 else ''
                ico='v' if f['zona']=='SOPORTE' else '^'
                O(f'  [{ico}] {f["nombre"]:<28} ${f["precio"]:>10,.2f} [{f["dist_pct"]:+.1f}%]{star}{here}')
            for f in fibs_e:
                O(f'  [T] {f["nombre"]:<28} ${f["precio"]:>10,.2f} [{f["dist_pct"]:+.1f}%]')
        O('  -- TRADE --')
        O(f'  Entrada: ${r["precio"]:,.2f}  |  Stop: ${r["stop"]:,.2f} (-{STOP_PCT}%)  |  Target: ${r["target"]:,.2f} (+{TARGET_PCT}%)')
        O(f'  R/R: {TARGET_PCT/STOP_PCT:.1f}  |  TV: {r["tv_link"]}')
        ratio=r.get('ratio',1) or 1
        if not r['ticker'].endswith('.BA'):
            p_ars=(r['precio']/ratio)*ccl_val
            s_ars=(r['stop']  /ratio)*ccl_val
            t_ars=(r['target']/ratio)*ccl_val
            O(f'  -- EN PESOS (CCL ${ccl_val:,.0f} | ratio {ratio}:1) --')
            O(f'  Entrada: ${p_ars:>12,.0f} ARS  |  Stop: ${s_ars:>12,.0f}  |  Target: ${t_ars:>12,.0f}')
        if es_adr:
            adr_info=ADRS_ARG_NYSE.get(r['ticker'],{})
            if adr_info:
                O(f'  -- BYMA LOCAL --')
                O(f'  Ticker: {adr_info["byma"]}  |  1 ADR = {adr_info["ratio"]} acciones  |  {adr_info["nombre"]}')
        O('  '+'─'*60)
        O('')
        return out

    # CEDEARs
    L(''); L('='*65)
    L('SECCION 2: CEDEARs — SENALES EN NYSE')
    L(f'  (Operas el CEDEAR en pesos. CCL ${ccl:,.0f})')
    L('-'*65)
    cedears_sin=[r for r in resultados_cedears if not r['ob_enc_bool']]
    cedears_con=[r for r in resultados_cedears if r['ob_enc_bool']]
    if cedears_sin:
        L(f'\n  TIER 1 — Camino libre ({len(cedears_sin)}):')
        for i,r in enumerate(cedears_sin[:5],1):
            L(f'\n  #{i}')
            for line in fmt_resultado(r,ccl): L(line)
    else: L('\n  Sin senales Tier 1 hoy.')
    if cedears_con:
        L(f'\n  TIER 2 — Con OB encima ({len(cedears_con)}):')
        for i,r in enumerate(cedears_con[:3],1):
            L(f'\n  #{i}')
            for line in fmt_resultado(r,ccl): L(line)

    # ADRs
    L(''); L('='*65); L('SECCION 3: ADRs ARGENTINOS'); L('-'*65)
    adrs_sin=[r for r in resultados_adrs if not r['ob_enc_bool']]
    adrs_con=[r for r in resultados_adrs if r['ob_enc_bool']]
    if adrs_sin:
        for i,r in enumerate(adrs_sin,1):
            L(f'\n  #{i}')
            for line in fmt_resultado(r,ccl,es_adr=True): L(line)
    else: L('\n  Sin senales en ADRs hoy.')
    if adrs_con:
        L(f'\n  ADRs con OB encima:')
        for r in adrs_con: L(f'  {r["ticker"]:6} | Score:{r["score"]}/7 | {r["zona"]} | OB: {r["ob_encima"]}')

    # BYMA
    L(''); L('='*65); L('SECCION 4: PANEL LIDER BYMA — EN PESOS'); L('-'*65)
    byma_sin=[r for r in resultados_byma if not r['ob_enc_bool']]
    if byma_sin:
        for i,r in enumerate(byma_sin,1):
            L(f'\n  #{i}  {r["ticker"]}  ({PANEL_LIDER_BYMA.get(r["ticker"],"")})')
            L('  '+'─'*60)
            L(f'  Precio: ${r["precio"]:,.2f} ARS  |  Zona: {r["zona"]}  |  RSI: {r["rsi"]}')
            L(f'  Estructura: {r["estructura"]}  |  Score: {r["score"]}/7')
            L(f'  Equilibrium: ${r["equilibrium"]:,.2f} ARS (+{r["dist_equil"]:.1f}%)')
            L(f'  Entrada: ${r["precio"]:,.2f}  Stop: ${r["stop"]:,.2f}  Target: ${r["target"]:,.2f}')
            L(f'  TV: {r["tv_link"]}')
            L('  '+'─'*60)
    else: L('\n  Sin senales en Panel Lider BYMA hoy.')

    # Plan
    L(''); L('='*65); L('SECCION 5: PLAN DEL DIA'); L('-'*65)
    todos=sorted(resultados_cedears+resultados_adrs+resultados_byma,key=lambda x:(x['ob_enc_bool'],-x['score'],x['pct_rango']))
    top3=[r for r in todos if not r['ob_enc_bool']][:3]
    L(''); L('  HORARIOS (hora Argentina):')
    L('  10:00       Apertura BYMA')
    L('  11:30       Apertura NYSE')
    L('  11:30-12:30 EVITAR — solapamiento Londres/NY')
    L('  13:00-15:00 MEJOR VENTANA')
    L('  17:00-18:00 EVITAR — cierre NY')
    L('')
    if top3:
        L('  PRIORIDADES HOY:')
        for i,r in enumerate(top3,1):
            L(f'  {i}. {r["ticker"]:6} | Score:{r["score"]}/7 | {r["zona"]} | RSI:{r["rsi"]} | ${r["precio"]:,.2f}')
    L('')
    L(f'  MAX/TRADE: 25% capital  |  STOP: -{STOP_PCT}%  |  TARGET: +{TARGET_PCT}%  |  R/R: {TARGET_PCT/STOP_PCT:.1f}')
    if not any([resultados_cedears,resultados_adrs,resultados_byma]):
        L(''); L('  SIN SENALES HOY. El capital que no se pierde, no necesita recuperarse.')
    L(''); L('='*65)
    L(f'  Generado: {now_arg().strftime("%d/%m/%Y %H:%M")} ARG  |  Swing:{SWING_LENGTH}  |  Score min:{SCORE_MINIMO}')
    L('='*65)
    return '\n'.join(lines)

# ============================================================
# HTML — mismo texto con colores
# ============================================================
def texto_a_html(texto, fecha_str, html_url_latest=''):
    def colorizar(texto):
        lines=texto.split('\n'); out=[]
        for line in lines:
            e=html_lib.escape(line)
            if e.startswith('==='): e=f'<span class="sep">{e}</span>'
            elif e.startswith('---'): e=f'<span class="sep2">{e}</span>'
            elif 'SMC CONFLUENCE SCREENER' in e or 'REPORTE DIARIO' in e: e=f'<span class="title">{e}</span>'
            elif re.match(r'\s*SECCION\s+\d+', e): e=f'<span class="section">{e}</span>'
            elif 'TIER 1' in e or 'TIER 2' in e: e=f'<span class="tier">{e}</span>'
            elif 'TICKER:' in e:
                e=re.sub(r'TICKER:\s*([\w\-\.]+)',r'TICKER: <span class="hl-ticker">\1</span>',e)
                e=re.sub(r'Score:\s*(\d+/7)',r'Score: <span class="hl-score">\1</span>',e)
                e=f'<span class="ticker-line">{e}</span>'
            elif '────' in e or '----' in e: e=f'<span class="divider">{e}</span>'
            elif re.match(r'\s*-- TRADE --', e): e=f'<span class="hdr-trade">{e}</span>'
            elif re.match(r'\s*-- EN PESOS', e): e=f'<span class="hdr-pesos">{e}</span>'
            elif re.match(r'\s*-- (FVG|FIBONACCI)', e): e=f'<span class="hdr-sub">{e}</span>'
            elif re.match(r'\s*-- BYMA LOCAL', e): e=f'<span class="hdr-byma">{e}</span>'
            elif 'ARS' in e and 'Entrada:' in e: e=f'<span class="pesos-line">{e}</span>'
            elif 'Entrada:' in e and 'Stop:' in e and 'Target:' in e: e=f'<span class="trade-line">{e}</span>'
            elif 'TV:' in e:
                e=re.sub(r'TV:\s*(https?://\S+)',r'TV: <a href="\1" target="_blank" class="tv-link">&#128200; Ver en TradingView &#8599;</a>',e)
            elif '[*] Discount' in e: e=e.replace('[*] Discount','<span class="hl-disc">&#9679; Discount</span>')
            elif 'Near Discount' in e: e=e.replace('Near Discount','<span class="hl-near">&#9679; Near Discount</span>')
            elif 'ENTRANDO [OK]' in e: e=e.replace('ENTRANDO [OK]','<span class="hl-in">ENTRANDO &#10003;</span>')
            elif 'saliendo [--]' in e: e=e.replace('saliendo [--]','<span class="hl-out">saliendo</span>')
            elif '[BULLISH]' in e: e=e.replace('[BULLISH]','<span class="hl-bull">[BULLISH]</span>')
            elif '[BEARISH]' in e: e=e.replace('[BEARISH]','<span class="hl-bear">[BEARISH]</span>')
            elif '[GP]' in e: e=e.replace('[GP]','<span class="hl-gp">[GP]</span>')
            elif '<- AQUI' in e: e=e.replace('&lt;- AQUI','<span class="hl-aqui">&lt;- AQUI</span>')
            elif 'CONCLUSION:' in e: e=f'<span class="hl-concl">{e}</span>'
            elif 'PRIORIDADES HOY:' in e: e=f'<span class="hl-prio">{e}</span>'
            elif re.match(r'\s+\d+\.\s+\w', e) and 'Score:' in e: e=f'<span class="prio-line">{e}</span>'
            out.append(e)
        return '\n'.join(out)

    nav=''
    if html_url_latest:
        nav=f'<div class="nav-bar">&#128279; <a href="{html_url_latest}">Ver reporte mas reciente</a></div>'

    return f'''<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SMC Reporte — {html_lib.escape(fecha_str)}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');
  body{{background:#0d0f14;color:#c8d3e0;font-family:'JetBrains Mono','Courier New',monospace;font-size:13.5px;line-height:1.7;padding:0;margin:0}}
  .nav-bar{{background:#151820;border-bottom:1px solid #1e2a3a;padding:10px 28px;font-size:12px}}
  .nav-bar a{{color:#4fa3ff;text-decoration:none}}
  .wrap{{padding:24px 28px;max-width:960px;margin:0 auto}}
  pre{{white-space:pre-wrap;word-break:break-word;margin:0}}
  .sep    {{color:#1e3050}}
  .sep2   {{color:#1a2535}}
  .title  {{color:#4fa3ff;font-weight:700}}
  .section{{color:#7eb8ff;font-weight:700}}
  .tier   {{color:#a0b4cc;font-weight:700}}
  .divider{{color:#1a2535}}
  .hl-ticker{{color:#00d4aa;font-weight:700;font-size:15px}}
  .ticker-line{{color:#dde4ef}}
  .hl-score{{color:#ffc947;font-weight:700}}
  .hl-disc{{color:#00d4aa;font-weight:700}}
  .hl-near{{color:#ffc947;font-weight:700}}
  .hl-in  {{color:#00d4aa;font-weight:700}}
  .hl-out {{color:#3a4a5c}}
  .hl-bull{{color:#00d4aa}}
  .hl-bear{{color:#ff5470}}
  .hl-gp  {{color:#a78bfa;font-weight:700}}
  .hl-aqui{{color:#ffc947;font-weight:700}}
  .hl-concl{{color:#ffc947}}
  .hl-prio {{color:#ffc947;font-weight:700}}
  .prio-line{{color:#e0eaf8}}
  .hdr-trade{{color:#ffc947;font-weight:700}}
  .trade-line{{color:#e0eaf8;font-weight:700}}
  .hdr-pesos{{color:#4fa3ff}}
  .pesos-line{{color:#4fa3ff;font-weight:700}}
  .hdr-sub{{color:#5a6a7a}}
  .hdr-byma{{color:#4fa3ff}}
  .tv-link{{color:#4fa3ff;text-decoration:none;background:rgba(79,163,255,.1);padding:1px 9px;border-radius:3px;border:1px solid rgba(79,163,255,.25)}}
  .tv-link:hover{{background:rgba(79,163,255,.22)}}
  @media(max-width:600px){{.wrap{{padding:12px 10px}}body{{font-size:12px}}}}
</style>
</head>
<body>
{nav}
<div class="wrap"><pre>{colorizar(texto)}</pre></div>
</body>
</html>'''

# ============================================================
# TELEGRAM
# ============================================================
def enviar_telegram(mensaje, token, chat_id):
    if not token or not chat_id: return False
    url=f'https://api.telegram.org/bot{token}/sendMessage'
    chunks=[mensaje[i:i+4000] for i in range(0,len(mensaje),4000)]; ok=0
    for chunk in chunks:
        try:
            r=requests.post(url,json={'chat_id':chat_id,'text':chunk,'parse_mode':'HTML'},timeout=15)
            if r.status_code==200: ok+=1
            else: log.error(f"Telegram {r.status_code}: {r.text[:200]}")
        except Exception as e: log.error(f"Telegram: {e}")
        if len(chunks)>1: time.sleep(0.5)
    return ok==len(chunks)

def mensaje_telegram(ccl, ccl_fuente, resultados_cedears, resultados_adrs,
                     resultados_byma, rotacion, spy_ret, fecha_str, html_url=''):
    sin_ob=[r for r in resultados_cedears+resultados_adrs if not r.get('ob_enc_bool')]
    con_ob=[r for r in resultados_cedears+resultados_adrs if r.get('ob_enc_bool')]
    total=len(sin_ob)+len(con_ob)+len(resultados_byma)
    spy_ico='📈' if spy_ret>=0 else '📉'
    sect_in =' '.join(r['etf'] for r in rotacion if r['estado']=='ENTRANDO')[:40] or '—'
    sect_out=' '.join(r['etf'] for r in rotacion if r['estado']!='ENTRANDO')[:40] or '—'

    L=[]; A=L.append
    A(f'<b>📊 SMC REPORTE — {fecha_str}</b>')
    A('')
    A(f'<b>💵 CCL:</b> ${ccl:,.0f} ARS/USD  ({ccl_fuente})')
    A(f'<b>{spy_ico} SPY 5d:</b> {spy_ret:+.2f}%')
    A('')
    A('<b>🔄 ROTACION SECTORIAL</b>')
    A(f'  ✅ Entrando: <b>{sect_in}</b>')
    A(f'  ❌ Saliendo: <b>{sect_out}</b>')
    A('')
    A(f'<b>🎯 SEÑALES: {total}</b>  ({len(sin_ob)} libres · {len(con_ob)} con OB · {len(resultados_byma)} BYMA)')

    if sin_ob:
        A(''); A('─── <b>PRIORIDAD ALTA</b> ───')
        for r in sin_ob[:5]:
            ratio=r.get('ratio',1) or 1
            p_ars=(r['precio']/ratio)*ccl; t_ars=(r['target']/ratio)*ccl; s_ars=(r['stop']/ratio)*ccl
            zona_ico='🟢' if r['zona']=='Discount' else '🟡'
            extras=[]
            if r.get('fvg')=='BULLISH':         extras.append('FVG')
            if r.get('squeeze')=='SI':           extras.append('SQZ')
            if str(r.get('rs_ratio','-'))!='-':  extras.append(f'RS {r["rs_ratio"]}')
            ext_str=' · '.join(extras)
            stars='⭐'*r['score']+'·'*(7-r['score'])
            A('')
            A(f'<b>{r["ticker"]}</b>  {zona_ico}  {stars}')
            A(f'  💰 ${p_ars:,.0f}  →  🎯 ${t_ars:,.0f}  🛑 ${s_ars:,.0f}')
            A(f'  RSI {r["rsi"]}  {r["estructura"]}' + (f'  {ext_str}' if ext_str else ''))
            A(f'  <a href="{r["tv_link"]}">📈 Ver en TradingView</a>')

    if con_ob:
        A(''); A('─── <b>CON OB ⚠️</b> ───')
        for r in con_ob[:3]:
            ratio=r.get('ratio',1) or 1; p_ars=(r['precio']/ratio)*ccl
            stars='⭐'*r['score']+'·'*(7-r['score'])
            A(f'<b>{r["ticker"]}</b>  🟡  {stars}  RSI {r["rsi"]}  ${p_ars:,.0f}')
            A(f'  <a href="{r["tv_link"]}">📈 TradingView</a>')

    if resultados_byma:
        A(''); A('─── <b>BYMA 🇦🇷</b> ───')
        for r in resultados_byma[:5]:
            stars='⭐'*r['score']+'·'*(7-r['score'])
            zona_ico='🟢' if r['zona']=='Discount' else '🟡'
            A(f'<b>{r["ticker"]}</b>  {zona_ico}  {stars}')
            A(f'  💰 ${r["precio"]:,.0f}  →  🎯 ${r["target"]:,.0f}  RSI {r["rsi"]}')
            A(f'  <a href="{r["tv_link"]}">📈 TradingView</a>')

    if total==0:
        A(''); A('⚪ Sin señales hoy.')
        A('El capital que no se pierde no necesita recuperarse.')

    A(''); A('─────────────────')
    if html_url:
        A(f'<b>📄 Reporte completo:</b>')
        A(html_url)
    return '\n'.join(L)

# ============================================================
# MAIN
# ============================================================
if __name__=='__main__':
    inicio=now_arg()
    log.info('='*60)
    log.info(f'SMC REPORTE DIARIO — {inicio.strftime("%d/%m/%Y %H:%M")} ARG')
    log.info('='*60)

    TELEGRAM_TOKEN  =os.environ.get('TELEGRAM_TOKEN','')
    TELEGRAM_CHAT_ID=os.environ.get('TELEGRAM_CHAT_ID','')
    GITHUB_PAGES_URL=os.environ.get('GITHUB_PAGES_URL','https://javi-funes.github.io/smc-screener').rstrip('/')

    _cargar_ratios()
    log.info(f'Universo: {len(CEDEARS_NYSE)} CEDEARs + {len(ADRS_ARG_NYSE)} ADRs + {len(PANEL_LIDER_BYMA)} BYMA')

    ccl,ccl_fuente=get_ccl()
    log.info(f'CCL: ${ccl:,.2f} ({ccl_fuente})')

    rotacion,spy_ret=get_rotacion()

    log.info(f'Escaneando CEDEARs...')
    resultados_cedears=[]
    for i,t in enumerate(CEDEARS_NYSE):
        r=analyze(t)
        if r:
            ratio=get_ratio(t); r['ratio']=ratio
            r['precio_ars']=round((r['precio']/ratio)*ccl,0)
            resultados_cedears.append(r)
            log.info(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1)%BATCH_SIZE==0: time.sleep(SLEEP_BETWEEN)

    log.info('Escaneando ADRs...')
    resultados_adrs=[]
    for i,t in enumerate(ADRS_ARG_NYSE.keys()):
        r=analyze(t)
        if r:
            ratio=get_ratio(t); r['ratio']=ratio
            r['precio_ars']=round((r['precio']/ratio)*ccl,0)
            resultados_adrs.append(r)
            log.info(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1)%BATCH_SIZE==0: time.sleep(SLEEP_BETWEEN)

    log.info('Escaneando BYMA...')
    resultados_byma=[]
    for i,t in enumerate(PANEL_LIDER_BYMA.keys()):
        r=analyze(t,es_byma=True)
        if r:
            r['ratio']=1; r['precio_ars']=r['precio']
            resultados_byma.append(r)
            log.info(f'  HIT {t:12} | Score:{r["score"]}/7 | {r["zona"]}')
        if (i+1)%BATCH_SIZE==0: time.sleep(SLEEP_BETWEEN)

    for lst in [resultados_cedears,resultados_adrs,resultados_byma]:
        lst.sort(key=lambda x:(-x['score'],x['ob_enc_bool'],x['pct_rango']))

    fecha_str=inicio.strftime('%d/%m/%Y %H:%M')
    date_fn  =inicio.strftime('%Y%m%d_%H%M')
    os.makedirs('results',exist_ok=True)

    # Generar texto
    reporte_txt=generar_reporte_texto(ccl,ccl_fuente,resultados_cedears,resultados_adrs,resultados_byma,rotacion,spy_ret)

    # URLs
    html_url_dated =f'{GITHUB_PAGES_URL}/results/reporte_{date_fn}.html'
    html_url_latest=f'{GITHUB_PAGES_URL}/results/reporte_latest.html'

    # Generar HTML
    html_dated =texto_a_html(reporte_txt, fecha_str, html_url_latest)
    html_latest=texto_a_html(reporte_txt, fecha_str)  # sin nav en el latest

    with open(f'results/reporte_{date_fn}.html','w',encoding='utf-8') as f: f.write(html_dated)
    with open('results/reporte_latest.html',  'w',encoding='utf-8') as f: f.write(html_latest)
    with open(f'results/reporte_{date_fn}.txt','w',encoding='utf-8') as f: f.write(reporte_txt)
    with open('results/reporte_latest.txt',   'w',encoding='utf-8') as f: f.write(reporte_txt)
    log.info(f'Archivos guardados: reporte_{date_fn}.html + reporte_latest.html')

    # CSV
    todos=resultados_cedears+resultados_adrs+resultados_byma
    if todos:
        cols=['ticker','score','zona','precio','ratio','precio_ars','rsi','estructura']
        df=pd.DataFrame(todos)
        df[[c for c in cols if c in df.columns]].to_csv(f'results/confluence_{date_fn}.csv',index=False)

    # Telegram
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        total=len(todos)
        log.info(f'Enviando Telegram ({total} senales)...')
        msg=mensaje_telegram(ccl,ccl_fuente,resultados_cedears,resultados_adrs,
                             resultados_byma,rotacion,spy_ret,fecha_str,html_url=html_url_dated)
        ok=enviar_telegram(msg,TELEGRAM_TOKEN,TELEGRAM_CHAT_ID)
        log.info(f'Telegram: {"OK" if ok else "FALLO"}')
    else:
        log.warning('Telegram no configurado')

    print(reporte_txt)
    log.info(f'Tiempo total: {(now_arg()-inicio).seconds}s')
    log.info(f'HTML: {html_url_dated}')
