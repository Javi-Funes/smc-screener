"""
SMC REPORTE DIARIO — Argentina Edition
=======================================
Universo: 52 CEDEARs + 11 ADRs argentinos + 11 Panel Lider BYMA
Salida: HTML con diseño + Telegram (resumen + link)
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
# HELPERS
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
        'equilibrium':(bottom+(0.5-EQUILIBRIUM_BAND)*r,bottom+(0.5+EQUILIBRIUM_BAND)*r),
        'premium':(top-DISCOUNT_PCT*r,top),
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
    imp_low_idx,imp_low=sl_antes[-1]
    imp_high=last_sh_val; rango_imp=imp_high-imp_low
    if rango_imp<=0: return [],[]
    niveles=[(0.236,'23.6%',False),(0.382,'38.2%',False),(0.500,'50.0%',False),(0.618,'61.8% GP',True),(0.650,'65.0% GP',True),(0.786,'78.6%',False)]
    retrocesos=[]
    for nivel,nombre,es_golden in niveles:
        precio_fib=round(imp_high-(rango_imp*nivel),2)
        retrocesos.append({'nivel':nivel,'nombre':nombre,'precio':precio_fib,'dist_pct':round((precio_fib-price)/price*100,2),'zona':'SOPORTE' if precio_fib<price else 'RESISTENCIA','es_golden':es_golden})
    extensiones=[]
    for nivel,nombre in [(1.272,'127.2%'),(1.414,'141.4%'),(1.618,'161.8%')]:
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
    adr_info=ADRS_ARG_NYSE.get(ticker)
    if adr_info: return float(adr_info['ratio'])
    return 1.0

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
        fib_retrocesos,fib_ext=calc_fibonacci_pois(sh,sl,price)
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
            'fvgs_all':fvgs_all,'fib_retrocesos':fib_retrocesos,'fib_extensiones':fib_ext,
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
# REPORTE HTML
# ============================================================
def generar_html(ccl, ccl_fuente, resultados_cedears, resultados_adrs,
                 resultados_byma, rotacion, spy_ret, fecha_str, repo_url=''):

    def score_stars(score):
        return '★'*score + '☆'*(7-score)

    def zona_badge(zona):
        cls = 'discount' if zona=='Discount' else 'near'
        return f'<span class="badge badge-{cls}">{zona}</span>'

    def ob_badge(ob):
        if ob: return '<span class="badge badge-ob">OB ⚠</span>'
        return ''

    def card_html(r, ccl_val, es_adr=False, es_byma=False):
        ratio  = r.get('ratio',1) or 1
        if es_byma:
            p_ars = r['precio']; t_ars = r['target']; s_ars = r['stop']
        else:
            p_ars = (r['precio']/ratio)*ccl_val
            t_ars = (r['target']/ratio)*ccl_val
            s_ars = (r['stop']  /ratio)*ccl_val

        extras=[]
        if r.get('fvg')=='BULLISH':             extras.append('<span class="tag tag-fvg">FVG</span>')
        if r.get('squeeze')=='SI':              extras.append('<span class="tag tag-sqz">SQZ</span>')
        if str(r.get('rs_ratio','-'))!='-':     extras.append(f'<span class="tag tag-rs">RS {r["rs_ratio"]}</span>')
        if r.get('ob_enc_bool'):                extras.append('<span class="tag tag-ob">OB encima</span>')
        extras_html=''.join(extras)

        fibs_html=''
        for f in r.get('fib_retrocesos',[]):
            gp=' <b>GP</b>' if f['es_golden'] else ''
            here=' ← AQUI' if abs(f['dist_pct'])<1.0 else ''
            ico='▼' if f['zona']=='SOPORTE' else '▲'
            fibs_html+=f'<div class="fib-row"><span class="fib-ico">{ico}</span><span class="fib-name">{f["nombre"]}</span><span class="fib-price">${f["precio"]:,.2f}</span><span class="fib-dist {("fib-pos" if f["dist_pct"]>0 else "fib-neg")}">{f["dist_pct"]:+.1f}%{gp}{here}</span></div>'
        for f in r.get('fib_extensiones',[]):
            fibs_html+=f'<div class="fib-row"><span class="fib-ico">▲</span><span class="fib-name">{f["nombre"]}</span><span class="fib-price">${f["precio"]:,.2f}</span><span class="fib-dist fib-pos">{f["dist_pct"]:+.1f}%</span></div>'

        fvgs_html=''
        for f in r.get('fvgs_all',[])[:4]:
            rel_cls='fvg-enc' if f['relacion']=='ENCIMA' else 'fvg-deb'
            fvgs_html+=f'<div class="fvg-row {rel_cls}"><span class="fvg-tipo">{f["tipo"]}</span><span>${f["low"]:,.2f}–${f["high"]:,.2f}</span><span class="fvg-dist">{f["dist_pct"]:+.1f}%</span></div>'

        byma_str=''
        if es_adr:
            adr_info=ADRS_ARG_NYSE.get(r['ticker'],{})
            if adr_info:
                byma_str=f'<div class="byma-local">🇦🇷 BYMA: <b>{adr_info["byma"]}</b> · {adr_info["nombre"]} · ratio {adr_info["ratio"]}:1</div>'

        rr = TARGET_PCT/STOP_PCT

        return f'''
<div class="card {'card-ob' if r['ob_enc_bool'] else ''}">
  <div class="card-header">
    <div class="card-title">
      <span class="ticker">{r['ticker']}</span>
      {zona_badge(r['zona'])}
      {ob_badge(r['ob_enc_bool'])}
      <span class="stars">{score_stars(r['score'])}</span>
    </div>
    <a href="{r['tv_link']}" target="_blank" class="tv-btn">Ver en TradingView ↗</a>
  </div>
  <div class="card-body">
    <div class="metrics-grid">
      <div class="metric"><div class="metric-label">Precio USD</div><div class="metric-value">${r['precio']:,.2f}</div></div>
      <div class="metric highlight"><div class="metric-label">Precio ARS</div><div class="metric-value">${p_ars:,.0f}</div></div>
      <div class="metric green"><div class="metric-label">Target</div><div class="metric-value">${t_ars:,.0f}</div></div>
      <div class="metric red"><div class="metric-label">Stop</div><div class="metric-value">${s_ars:,.0f}</div></div>
      <div class="metric"><div class="metric-label">RSI</div><div class="metric-value {'rsi-low' if r['rsi']<35 else ''}">{r['rsi']}</div></div>
      <div class="metric"><div class="metric-label">R/R</div><div class="metric-value">{rr:.1f}</div></div>
      <div class="metric"><div class="metric-label">Estructura</div><div class="metric-value">{r['estructura']}</div></div>
      <div class="metric"><div class="metric-label">Equilibrium</div><div class="metric-value">+{r['dist_equil']:.1f}%</div></div>
    </div>
    <div class="tags-row">{extras_html}</div>
    {byma_str}
    {'<div class="section-label">FIBONACCI</div>' + fibs_html if fibs_html else ''}
    {'<div class="section-label">FVGs</div>' + fvgs_html if fvgs_html else ''}
  </div>
</div>'''

    # Rotacion
    rot_rows=''
    for r in rotacion:
        cls='rot-in' if r['estado']=='ENTRANDO' else 'rot-out'
        arrow='↑' if r['estado']=='ENTRANDO' else '↓'
        rot_rows+=f'<tr class="{cls}"><td>{r["sector"]}</td><td><b>{r["etf"]}</b></td><td>{r["ret_5d"]:+.2f}%</td><td>{r["rs_spy"]:.3f}</td><td>{arrow} {r["estado"].upper()}</td></tr>'

    # Cards CEDEARs
    sin_ob=[r for r in resultados_cedears+resultados_adrs if not r['ob_enc_bool']]
    con_ob=[r for r in resultados_cedears+resultados_adrs if r['ob_enc_bool']]
    cards_sinob=''.join(card_html(r,ccl,es_adr=(r['ticker'] in ADRS_ARG_NYSE)) for r in sin_ob)
    cards_conob=''.join(card_html(r,ccl,es_adr=(r['ticker'] in ADRS_ARG_NYSE)) for r in con_ob)
    cards_byma =''.join(card_html(r,ccl,es_byma=True) for r in resultados_byma)

    spy_arrow='▲' if spy_ret>=0 else '▼'
    spy_cls='green' if spy_ret>=0 else 'red'
    total=len(sin_ob)+len(con_ob)+len(resultados_byma)
    sectores_ok=[r['etf'] for r in rotacion if r['estado']=='ENTRANDO']

    html=f'''<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SMC Reporte — {fecha_str}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
  :root{{
    --bg:#0d0f14;--surface:#151820;--surface2:#1c2030;
    --border:#252a38;--border2:#2e3448;
    --text:#e2e8f0;--text2:#8892a4;--text3:#4a5568;
    --green:#00d4aa;--red:#ff5470;--yellow:#ffc947;
    --blue:#4fa3ff;--purple:#a78bfa;
    --accent:#00d4aa;
  }}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans',sans-serif;font-size:14px;line-height:1.5}}
  a{{color:var(--blue);text-decoration:none}}
  a:hover{{text-decoration:underline}}

  .header{{background:var(--surface);border-bottom:1px solid var(--border);padding:20px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
  .header-title{{font-family:'IBM Plex Mono',monospace;font-size:18px;font-weight:600;color:var(--accent)}}
  .header-meta{{font-size:12px;color:var(--text2);font-family:'IBM Plex Mono',monospace}}
  .header-date{{font-size:11px;color:var(--text3);margin-top:4px}}

  .summary-bar{{background:var(--surface2);border-bottom:1px solid var(--border);padding:12px 32px;display:flex;gap:32px;flex-wrap:wrap}}
  .sum-item{{display:flex;flex-direction:column}}
  .sum-label{{font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.08em}}
  .sum-value{{font-size:20px;font-weight:600;font-family:'IBM Plex Mono',monospace}}
  .sum-value.green{{color:var(--green)}}
  .sum-value.red{{color:var(--red)}}
  .sum-value.yellow{{color:var(--yellow)}}

  .container{{max-width:1200px;margin:0 auto;padding:24px 32px}}
  .section-title{{font-family:'IBM Plex Mono',monospace;font-size:11px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.12em;margin:32px 0 12px;padding-bottom:8px;border-bottom:1px solid var(--border)}}

  /* Rotacion */
  .rot-table{{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:8px}}
  .rot-table th{{text-align:left;color:var(--text3);font-weight:400;font-size:11px;text-transform:uppercase;padding:6px 12px;border-bottom:1px solid var(--border)}}
  .rot-table td{{padding:7px 12px;border-bottom:1px solid var(--border)}}
  .rot-in td:last-child{{color:var(--green);font-weight:600}}
  .rot-out td:last-child{{color:var(--text3)}}
  .rot-in{{background:rgba(0,212,170,.04)}}

  /* Cards */
  .cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:16px}}
  .card{{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;transition:border-color .2s}}
  .card:hover{{border-color:var(--border2)}}
  .card-ob{{border-color:rgba(255,84,112,.25)}}
  .card-header{{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;border-bottom:1px solid var(--border);background:var(--surface2)}}
  .card-title{{display:flex;align-items:center;gap:8px;flex-wrap:wrap}}
  .ticker{{font-family:'IBM Plex Mono',monospace;font-size:17px;font-weight:600;color:var(--text)}}
  .stars{{font-size:13px;color:var(--yellow);letter-spacing:1px}}
  .tv-btn{{background:rgba(79,163,255,.12);color:var(--blue);border:1px solid rgba(79,163,255,.3);padding:5px 12px;border-radius:4px;font-size:11px;font-weight:600;white-space:nowrap}}
  .tv-btn:hover{{background:rgba(79,163,255,.25);text-decoration:none}}

  .badge{{font-size:10px;font-weight:600;padding:2px 8px;border-radius:3px;text-transform:uppercase;letter-spacing:.06em}}
  .badge-discount{{background:rgba(0,212,170,.15);color:var(--green);border:1px solid rgba(0,212,170,.3)}}
  .badge-near{{background:rgba(255,201,71,.12);color:var(--yellow);border:1px solid rgba(255,201,71,.3)}}
  .badge-ob{{background:rgba(255,84,112,.12);color:var(--red);border:1px solid rgba(255,84,112,.3)}}

  .card-body{{padding:16px}}
  .metrics-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}}
  .metric{{background:var(--surface2);border-radius:6px;padding:10px 12px}}
  .metric.highlight{{border:1px solid rgba(0,212,170,.2);background:rgba(0,212,170,.05)}}
  .metric.green{{border:1px solid rgba(0,212,170,.15)}}
  .metric.red{{border:1px solid rgba(255,84,112,.15)}}
  .metric-label{{font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px}}
  .metric-value{{font-family:'IBM Plex Mono',monospace;font-size:14px;font-weight:600}}
  .rsi-low{{color:var(--green)}}

  .tags-row{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}}
  .tag{{font-size:10px;padding:2px 8px;border-radius:3px;font-weight:600}}
  .tag-fvg{{background:rgba(0,212,170,.1);color:var(--green);border:1px solid rgba(0,212,170,.2)}}
  .tag-sqz{{background:rgba(167,139,250,.1);color:var(--purple);border:1px solid rgba(167,139,250,.2)}}
  .tag-rs{{background:rgba(79,163,255,.1);color:var(--blue);border:1px solid rgba(79,163,255,.2)}}
  .tag-ob{{background:rgba(255,84,112,.1);color:var(--red);border:1px solid rgba(255,84,112,.2)}}

  .section-label{{font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.1em;margin:10px 0 6px;font-weight:600}}
  .fib-row{{display:flex;gap:12px;font-size:12px;font-family:'IBM Plex Mono',monospace;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.03)}}
  .fib-ico{{color:var(--text3);width:12px}}
  .fib-name{{color:var(--text2);flex:1}}
  .fib-price{{color:var(--text);width:90px;text-align:right}}
  .fib-dist{{width:80px;text-align:right}}
  .fib-pos{{color:var(--green)}}
  .fib-neg{{color:var(--red)}}

  .fvg-row{{display:flex;gap:12px;font-size:12px;font-family:'IBM Plex Mono',monospace;padding:3px 0}}
  .fvg-tipo{{width:60px;font-weight:600}}
  .fvg-enc .fvg-tipo{{color:var(--red)}}
  .fvg-deb .fvg-tipo{{color:var(--green)}}
  .fvg-dist{{margin-left:auto}}

  .byma-local{{font-size:12px;color:var(--blue);margin:8px 0;padding:6px 10px;background:rgba(79,163,255,.05);border-radius:4px;border-left:2px solid var(--blue)}}
  .no-signal{{color:var(--text3);font-style:italic;padding:20px 0}}

  .footer{{text-align:center;padding:32px;color:var(--text3);font-size:11px;font-family:'IBM Plex Mono',monospace;border-top:1px solid var(--border);margin-top:40px}}

  @media(max-width:600px){{
    .container{{padding:16px}}
    .cards-grid{{grid-template-columns:1fr}}
    .metrics-grid{{grid-template-columns:repeat(2,1fr)}}
    .summary-bar{{gap:16px;padding:12px 16px}}
    .header{{padding:16px}}
  }}
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="header-title">📊 SMC SCREENER — Argentina</div>
    <div class="header-meta">Swing {SWING_LENGTH} · Score min {SCORE_MINIMO} · Stop {STOP_PCT}% · Target {TARGET_PCT}%</div>
  </div>
  <div style="text-align:right">
    <div class="header-meta">Generado: <b>{fecha_str}</b></div>
    <div class="header-date">Fuente CCL: {ccl_fuente}</div>
  </div>
</div>

<div class="summary-bar">
  <div class="sum-item">
    <span class="sum-label">CCL</span>
    <span class="sum-value yellow">${ccl:,.0f}</span>
  </div>
  <div class="sum-item">
    <span class="sum-label">SPY 5d</span>
    <span class="sum-value {spy_cls}">{spy_arrow} {spy_ret:+.2f}%</span>
  </div>
  <div class="sum-item">
    <span class="sum-label">Señales</span>
    <span class="sum-value {'green' if total>0 else ''}">{total}</span>
  </div>
  <div class="sum-item">
    <span class="sum-label">Libres</span>
    <span class="sum-value green">{len(sin_ob)}</span>
  </div>
  <div class="sum-item">
    <span class="sum-label">Con OB</span>
    <span class="sum-value {'yellow' if con_ob else ''}">{len(con_ob)}</span>
  </div>
  <div class="sum-item">
    <span class="sum-label">BYMA</span>
    <span class="sum-value">{len(resultados_byma)}</span>
  </div>
  <div class="sum-item">
    <span class="sum-label">Sectores</span>
    <span class="sum-value" style="font-size:14px;color:var(--green)">{' '.join(sectores_ok[:3]) or '—'}</span>
  </div>
</div>

<div class="container">

  <div class="section-title">Rotación Sectorial — últimos {RS_DIAS} días</div>
  <table class="rot-table">
    <tr><th>Sector</th><th>ETF</th><th>Ret 5d</th><th>RS/SPY</th><th>Estado</th></tr>
    {rot_rows}
  </table>

  <div class="section-title">Prioridad Alta — Sin OB encima ({len(sin_ob)})</div>
  {'<div class="cards-grid">'+cards_sinob+'</div>' if sin_ob else '<div class="no-signal">Sin señales de prioridad alta hoy.</div>'}

  {'<div class="section-title">Con OB Encima — Precaución ('+str(len(con_ob))+')</div><div class="cards-grid">'+cards_conob+'</div>' if con_ob else ''}

  <div class="section-title">Panel Líder BYMA 🇦🇷 ({len(resultados_byma)})</div>
  {'<div class="cards-grid">'+cards_byma+'</div>' if resultados_byma else '<div class="no-signal">Sin señales en Panel Lider BYMA hoy.</div>'}

</div>

<div class="footer">
  SMC Screener Argentina · {fecha_str} · Swing {SWING_LENGTH} · Generado con GitHub Actions
</div>

</body>
</html>'''
    return html

# ============================================================
# TELEGRAM
# ============================================================
def enviar_telegram(mensaje, token, chat_id):
    if not token or not chat_id: return False
    url=f'https://api.telegram.org/bot{token}/sendMessage'
    max_len=4000; chunks=[mensaje[i:i+max_len] for i in range(0,len(mensaje),max_len)]
    ok=0
    for chunk in chunks:
        try:
            r=requests.post(url,json={'chat_id':chat_id,'text':chunk,'parse_mode':'HTML'},timeout=15)
            if r.status_code==200: ok+=1
            else: log.error(f"Telegram {r.status_code}: {r.text[:200]}")
        except Exception as e: log.error(f"Telegram excepcion: {e}")
        if len(chunks)>1: time.sleep(0.5)
    return ok==len(chunks)

def mensaje_telegram(ccl, ccl_fuente, resultados_cedears, resultados_adrs,
                     resultados_byma, rotacion, spy_ret, fecha_str, html_url=''):
    sin_ob=[r for r in resultados_cedears+resultados_adrs if not r.get('ob_enc_bool')]
    con_ob=[r for r in resultados_cedears+resultados_adrs if r.get('ob_enc_bool')]
    total=len(sin_ob)+len(con_ob)+len(resultados_byma)
    spy_ico='📈' if spy_ret>=0 else '📉'
    sectores_ok=[r['etf'] for r in rotacion if r['estado']=='ENTRANDO']
    sectores_out=[r['etf'] for r in rotacion if r['estado']!='ENTRANDO']
    sect_in=' '.join(sectores_ok[:4]) or '—'
    sect_out=' '.join(sectores_out[:4]) or '—'

    lines=[]
    lines.append(f'<b>📊 SMC REPORTE — {fecha_str}</b>')
    lines.append('')
    lines.append(f'<b>💵 CCL:</b> ${ccl:,.0f} ARS/USD  ({ccl_fuente})')
    lines.append(f'<b>{spy_ico} SPY 5d:</b> {spy_ret:+.2f}%')
    lines.append('')
    lines.append(f'<b>🔄 ROTACION SECTORIAL</b>')
    lines.append(f'  ✅ Entrando: <b>{sect_in}</b>')
    lines.append(f'  ❌ Saliendo: <b>{sect_out}</b>')
    lines.append('')
    lines.append(f'<b>🎯 SEÑALES: {total}</b>  ({len(sin_ob)} libres · {len(con_ob)} con OB · {len(resultados_byma)} BYMA)')

    if sin_ob:
        lines.append('')
        lines.append('─── <b>PRIORIDAD ALTA</b> ───')
        for r in sin_ob[:5]:
            ratio=r.get('ratio',1) or 1
            p_ars=(r['precio']/ratio)*(ccl)
            t_ars=(r['target']/ratio)*(ccl)
            s_ars=(r['stop']  /ratio)*(ccl)
            zona_ico='🟢' if r['zona']=='Discount' else '🟡'
            extras=[]
            if r.get('fvg')=='BULLISH':          extras.append('FVG')
            if r.get('squeeze')=='SI':            extras.append('SQZ')
            if str(r.get('rs_ratio','-'))!='-':  extras.append(f'RS {r["rs_ratio"]}')
            ext_str=' · '.join(extras)
            stars='⭐'*r['score']+'·'*(7-r['score'])
            lines.append('')
            lines.append(f'<b>{r["ticker"]}</b>  {zona_ico}  {stars}')
            lines.append(f'  💰 ${p_ars:,.0f}  →  🎯 ${t_ars:,.0f}  🛑 ${s_ars:,.0f}')
            lines.append(f'  RSI {r["rsi"]}  {r["estructura"]}' + (f'  {ext_str}' if ext_str else ''))
            lines.append(f'  <a href="{r["tv_link"]}">📈 Ver en TradingView</a>')

    if con_ob:
        lines.append('')
        lines.append('─── <b>CON OB ⚠️</b> ───')
        for r in con_ob[:3]:
            ratio=r.get('ratio',1) or 1
            p_ars=(r['precio']/ratio)*ccl
            stars='⭐'*r['score']+'·'*(7-r['score'])
            lines.append(f'<b>{r["ticker"]}</b>  🟡  {stars}  RSI {r["rsi"]}  ${p_ars:,.0f}')
            lines.append(f'  <a href="{r["tv_link"]}">📈 TradingView</a>')

    if resultados_byma:
        lines.append('')
        lines.append('─── <b>BYMA 🇦🇷</b> ───')
        for r in resultados_byma[:5]:
            stars='⭐'*r['score']+'·'*(7-r['score'])
            zona_ico='🟢' if r['zona']=='Discount' else '🟡'
            lines.append(f'<b>{r["ticker"]}</b>  {zona_ico}  {stars}')
            lines.append(f'  💰 ${r["precio"]:,.0f}  →  🎯 ${r["target"]:,.0f}  RSI {r["rsi"]}')
            lines.append(f'  <a href="{r["tv_link"]}">📈 TradingView</a>')

    if total==0:
        lines.append('')
        lines.append('⚪ Sin señales hoy.')
        lines.append('El capital que no se pierde no necesita recuperarse.')

    lines.append('')
    lines.append('─────────────────')
    if html_url:
        lines.append(f'<b>📄 Reporte completo:</b>')
        lines.append(html_url)

    return '\n'.join(lines)

# ============================================================
# MAIN
# ============================================================
if __name__=='__main__':
    inicio=now_arg()
    log.info('='*60)
    log.info('SMC REPORTE DIARIO — Argentina Edition')
    log.info(f'{inicio.strftime("%d/%m/%Y %H:%M")} ARG')
    log.info('='*60)

    TELEGRAM_TOKEN  =os.environ.get('TELEGRAM_TOKEN','')
    TELEGRAM_CHAT_ID=os.environ.get('TELEGRAM_CHAT_ID','')
    GITHUB_PAGES_URL=os.environ.get('GITHUB_PAGES_URL','https://javi-funes.github.io/smc-screener')

    log.info('Cargando ratios...')
    _cargar_ratios()

    log.info(f'Universo: {len(CEDEARS_NYSE)} CEDEARs + {len(ADRS_ARG_NYSE)} ADRs + {len(PANEL_LIDER_BYMA)} BYMA')

    log.info('Obteniendo CCL...')
    ccl,ccl_fuente=get_ccl()
    log.info(f'CCL: ${ccl:,.2f} ({ccl_fuente})')

    log.info('Rotacion sectorial...')
    rotacion,spy_ret=get_rotacion()

    log.info(f'Escaneando {len(CEDEARS_NYSE)} CEDEARs...')
    resultados_cedears=[]
    for i,t in enumerate(CEDEARS_NYSE):
        r=analyze(t,es_byma=False)
        if r:
            ratio=get_ratio(t); r['ratio']=ratio
            r['precio_ars']=round((r['precio']/ratio)*ccl,0)
            r['stop_ars']  =round((r['stop']  /ratio)*ccl,0)
            r['target_ars']=round((r['target']/ratio)*ccl,0)
            resultados_cedears.append(r)
            log.info(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1)%BATCH_SIZE==0: time.sleep(SLEEP_BETWEEN)

    log.info(f'Escaneando {len(ADRS_ARG_NYSE)} ADRs...')
    resultados_adrs=[]
    for i,t in enumerate(ADRS_ARG_NYSE.keys()):
        r=analyze(t,es_byma=False)
        if r:
            ratio=get_ratio(t); r['ratio']=ratio
            r['byma_local']=ADRS_ARG_NYSE[t]['byma']
            r['precio_ars']=round((r['precio']/ratio)*ccl,0)
            r['stop_ars']  =round((r['stop']  /ratio)*ccl,0)
            r['target_ars']=round((r['target']/ratio)*ccl,0)
            resultados_adrs.append(r)
            log.info(f'  HIT {t:6} | Score:{r["score"]}/7 | {r["zona"]} | ARS:${r["precio_ars"]:,.0f}')
        if (i+1)%BATCH_SIZE==0: time.sleep(SLEEP_BETWEEN)

    log.info(f'Escaneando {len(PANEL_LIDER_BYMA)} BYMA...')
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

    # Generar HTML
    log.info('Generando HTML...')
    html=generar_html(ccl,ccl_fuente,resultados_cedears,resultados_adrs,
                      resultados_byma,rotacion,spy_ret,fecha_str)
    fname_dated =f'results/reporte_{date_fn}.html'
    fname_latest='results/reporte_latest.html'
    with open(fname_dated, 'w',encoding='utf-8') as f: f.write(html)
    with open(fname_latest,'w',encoding='utf-8') as f: f.write(html)
    log.info(f'HTML guardado: {fname_dated}')

    # URL publica del reporte
    html_url=f'{GITHUB_PAGES_URL}/results/reporte_{date_fn}.html'
    latest_url=f'{GITHUB_PAGES_URL}/results/reporte_latest.html'

    # CSV
    todos=resultados_cedears+resultados_adrs+resultados_byma
    if todos:
        cols=['ticker','score','zona','precio','ratio','precio_ars','stop_ars','target_ars','rsi','estructura']
        df=pd.DataFrame(todos)
        df[[c for c in cols if c in df.columns]].to_csv(f'results/confluence_{date_fn}.csv',index=False)

    # Telegram
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        total=len(resultados_cedears)+len(resultados_adrs)+len(resultados_byma)
        log.info(f'Enviando Telegram ({total} senales)...')
        msg=mensaje_telegram(ccl,ccl_fuente,resultados_cedears,resultados_adrs,
                             resultados_byma,rotacion,spy_ret,fecha_str,html_url=html_url)
        ok=enviar_telegram(msg,TELEGRAM_TOKEN,TELEGRAM_CHAT_ID)
        log.info(f'Telegram: {"OK" if ok else "FALLO"}')
    else:
        log.warning('Telegram no configurado')

    elapsed=(now_arg()-inicio).seconds
    log.info(f'Tiempo total: {elapsed}s')
    log.info(f'Reporte: {html_url}')
