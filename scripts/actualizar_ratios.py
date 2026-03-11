"""
ACTUALIZAR RATIOS CEDEARs
=========================
Fuente 1: Comafi     → Acciones USA + algunos globales (~200 tickers)
Fuente 2: Caja de Valores → ETFs + Acciones Brasil + otras (~50 tickers)

Corre: Domingos a las 8AM ARG (o manualmente)
Output: results/ratios_cedears.json

Formato del JSON:
{
  "BABA":  {"ratio": 9,  "nombre": "ALIBABA GROUP",    "tipo": "Accion", "fuente": "Comafi"},
  "SPY":   {"ratio": 20, "nombre": "SPDR S&P 500",     "tipo": "ETF",    "fuente": "CajaValores"},
  "VALE3": {"ratio": 1,  "nombre": "VALE S.A.",        "tipo": "Brasil", "fuente": "CajaValores"},
  ...
}

Formula precio CEDEAR en pesos:
  precio_ars = (precio_usd / ratio) * ccl
  
  Ejemplo BABA: ($136.29 / 9) * $1,455 = $22,044 por CEDEAR
"""

import requests
import pandas as pd
import json
import os
import re
from datetime import datetime
from io import BytesIO

# ============================================================
# URLs de descarga
# ============================================================
URL_COMAFI = 'https://www.comafi.com.ar/Multimedios/otros/7279.xlsx'

# Nota: la URL de Caja de Valores puede cambiar cuando actualizan el archivo.
# Si falla, buscar en: https://cajadevalores.com.ar/Servicios/Cedears
URL_CAJVAL = 'https://cajadevalores.com.ar/uploads/Tablas - Caja de Valores (24-06) - actualizaciones 12-02.xlsx'

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

OUTPUT_JSON = 'results/ratios_cedears.json'
OUTPUT_CSV  = 'results/ratios_cedears.csv'

# ============================================================
# PARSER COMAFI
# Columnas relevantes del Excel de Comafi:
#   - "Ticker en mercado de origen"  → ticker NYSE/NASDAQ
#   - "Ratio Cedear / valor sub-yacente" → ej: "9:1"
#   - "Programa de CEDEAR"           → nombre completo
#   - "Id de mercado"                → ticker BYMA local
# ============================================================

def parse_comafi(content: bytes) -> dict:
    """
    Parsea el Excel de Comafi y retorna dict {ticker_nyse: {...}}
    El ratio en Comafi viene como "9:1" — tomamos el numerador.
    """
    ratios = {}
    try:
        # Comafi puede tener el header en distintas filas — probamos las primeras 5
        for skip in range(0, 6):
            try:
                df = pd.read_excel(BytesIO(content), skiprows=skip, engine='openpyxl')
                df.columns = df.columns.str.strip()

                # Buscar columnas por palabras clave (tolerante a variaciones)
                col_ticker = next((c for c in df.columns if 'ticker' in c.lower() and 'origen' in c.lower()), None)
                col_ratio  = next((c for c in df.columns if 'ratio' in c.lower()), None)
                col_nombre = next((c for c in df.columns if 'programa' in c.lower() or 'cedear' in c.lower()), None)
                col_byma   = next((c for c in df.columns if 'mercado' in c.lower() and 'id' in c.lower()), None)

                if col_ticker and col_ratio:
                    print(f"  Comafi: header encontrado en fila {skip}")
                    print(f"  Columnas: ticker='{col_ticker}' | ratio='{col_ratio}'")
                    break
            except Exception:
                continue
        else:
            print("  Comafi: No se encontró header válido")
            return {}

        # Procesar filas
        for _, row in df.iterrows():
            ticker = str(row.get(col_ticker, '')).strip().upper()
            ratio_raw = str(row.get(col_ratio, '')).strip()
            nombre = str(row.get(col_nombre, '')).strip() if col_nombre else ''
            byma   = str(row.get(col_byma,   '')).strip() if col_byma   else ticker

            # Saltar filas vacías o headers repetidos
            if not ticker or ticker in ('NAN', 'TICKER', ''):
                continue

            # Parsear ratio — formatos posibles: "9:1", "9", "9/1", "9 : 1"
            ratio = parse_ratio(ratio_raw)
            if ratio is None:
                continue

            ratios[ticker] = {
                'ratio':   ratio,
                'nombre':  nombre,
                'byma':    byma if byma and byma != 'NAN' else ticker,
                'tipo':    'Accion',
                'fuente':  'Comafi',
            }

        print(f"  Comafi: {len(ratios)} tickers parseados")

    except Exception as e:
        print(f"  Comafi ERROR: {e}")

    return ratios


# ============================================================
# PARSER CAJA DE VALORES
# El Excel de CajaValores tiene DOS hojas (o secciones):
#   1. CEDEARs de ETF
#   2. CEDEARs de Acciones (incluye Brasil)
# Columnas: "Símbolo BYMA" | "Ticker en Mercado de Origen" | "Ratio CEDEARs / valor subyacente"
# ============================================================

def parse_cajavaloroes(content: bytes) -> dict:
    """
    Parsea el Excel de Caja de Valores.
    Puede tener múltiples hojas — procesamos todas.
    """
    ratios = {}
    try:
        xls = pd.ExcelFile(BytesIO(content), engine='openpyxl')
        print(f"  CajaValores: hojas encontradas → {xls.sheet_names}")

        for sheet in xls.sheet_names:
            try:
                # Intentar con y sin skiprows
                for skip in range(0, 5):
                    try:
                        df = pd.read_excel(BytesIO(content), sheet_name=sheet,
                                           skiprows=skip, engine='openpyxl')
                        df.columns = df.columns.str.strip()

                        col_ticker = next((c for c in df.columns
                                          if 'ticker' in c.lower() and 'origen' in c.lower()), None)
                        col_byma   = next((c for c in df.columns
                                          if 'byma' in c.lower() or 'símbolo' in c.lower()
                                          or 'simbolo' in c.lower()), None)
                        col_ratio  = next((c for c in df.columns if 'ratio' in c.lower()), None)
                        col_nombre = next((c for c in df.columns
                                          if 'cedear' in c.lower() and 'etf' in c.lower()
                                          or 'cedear' in c.lower() and 'accion' in c.lower()
                                          or 'nombre' in c.lower()), None)

                        if col_ratio and (col_ticker or col_byma):
                            print(f"  CajaValores hoja '{sheet}': header en fila {skip}")
                            break
                    except Exception:
                        continue
                else:
                    continue

                # Determinar tipo por nombre de hoja
                tipo = 'ETF' if 'etf' in sheet.lower() else \
                       'Brasil' if 'brasil' in sheet.lower() or 'b3' in sheet.lower() else \
                       'Accion'

                for _, row in df.iterrows():
                    # Ticker origen (para cruzar con yfinance)
                    ticker_origen = str(row.get(col_ticker, '')).strip().upper() if col_ticker else ''
                    ticker_byma   = str(row.get(col_byma,   '')).strip().upper() if col_byma   else ''
                    ratio_raw     = str(row.get(col_ratio,  '')).strip()
                    nombre        = str(row.get(col_nombre, '')).strip() if col_nombre else ''

                    # Preferir ticker de origen; si no hay, usar BYMA
                    ticker = ticker_origen or ticker_byma
                    if not ticker or ticker in ('NAN', ''):
                        continue

                    ratio = parse_ratio(ratio_raw)
                    if ratio is None:
                        continue

                    # Detectar Brasil por mercado B3
                    if ticker.endswith('3') or ticker.endswith('4') or ticker.endswith('11'):
                        tipo_final = 'Brasil'
                    else:
                        tipo_final = tipo

                    ratios[ticker] = {
                        'ratio':  ratio,
                        'nombre': nombre,
                        'byma':   ticker_byma or ticker,
                        'tipo':   tipo_final,
                        'fuente': 'CajaValores',
                    }

            except Exception as e:
                print(f"  CajaValores hoja '{sheet}' ERROR: {e}")
                continue

        print(f"  CajaValores: {len(ratios)} tickers parseados")

    except Exception as e:
        print(f"  CajaValores ERROR: {e}")

    return ratios


# ============================================================
# HELPER: parsear ratio desde string
# Formatos posibles: "9:1", "9", "9/1", "9 : 1", "1:5" (inverso!)
# ============================================================

def parse_ratio(raw: str):
    """
    Retorna el ratio como float.
    "9:1"  → 9.0   (9 CEDEARs = 1 acción)
    "1:5"  → 0.2   (1 CEDEAR = 5 acciones) — casos como BITF
    "9"    → 9.0
    """
    raw = str(raw).strip()
    if not raw or raw.upper() in ('NAN', 'N/A', '-', ''):
        return None
    try:
        # Formato X:Y
        m = re.match(r'(\d+\.?\d*)\s*[:/]\s*(\d+\.?\d*)', raw)
        if m:
            num, den = float(m.group(1)), float(m.group(2))
            if den == 0:
                return None
            return round(num / den, 4)
        # Solo número
        val = float(re.sub(r'[^\d.]', '', raw))
        return val if val > 0 else None
    except Exception:
        return None


# ============================================================
# MAIN
# ============================================================

def descargar(url: str, nombre: str) -> bytes | None:
    try:
        print(f"  Descargando {nombre}...")
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code == 200 and len(r.content) > 1000:
            print(f"  OK: {len(r.content):,} bytes")
            return r.content
        else:
            print(f"  Error HTTP {r.status_code}")
            return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


if __name__ == '__main__':
    inicio = datetime.now()
    print(f'\n{"="*60}')
    print(f'  ACTUALIZADOR DE RATIOS CEDEARs')
    print(f'  {inicio.strftime("%d/%m/%Y %H:%M")}')
    print(f'{"="*60}\n')

    os.makedirs('results', exist_ok=True)

    # ── 1. Descargar ambos Excel ──────────────────────────────
    print('[ 1/4 ] Descargando Excel Comafi...')
    content_comafi = descargar(URL_COMAFI, 'Comafi')

    print('\n[ 2/4 ] Descargando Excel Caja de Valores...')
    content_cajval = descargar(URL_CAJVAL, 'CajaValores')

    # ── 2. Parsear ────────────────────────────────────────────
    print('\n[ 3/4 ] Parseando...')

    ratios_comafi = parse_comafi(content_comafi)   if content_comafi else {}
    ratios_cajval = parse_cajavaloroes(content_cajval) if content_cajval else {}

    # ── 3. Merge — CajaValores primero, Comafi sobreescribe ──
    # Lógica: Comafi tiene más acciones USA con ratios verificados.
    # CajaValores aporta ETFs y Brasil que Comafi no tiene.
    # En caso de conflicto: Comafi gana para acciones, CajaValores para ETFs/Brasil.
    print('\n[ 4/4 ] Mergeando fuentes...')

    ratios_final = {}

    # Primero CajaValores (ETFs + Brasil)
    for ticker, data in ratios_cajval.items():
        ratios_final[ticker] = data

    # Luego Comafi sobreescribe acciones (más completo para acciones USA)
    for ticker, data in ratios_comafi.items():
        if ticker not in ratios_final or ratios_final[ticker]['tipo'] == 'Accion':
            ratios_final[ticker] = data

    # ── 4. Estadísticas ───────────────────────────────────────
    tipos = {}
    for d in ratios_final.values():
        t = d['tipo']
        tipos[t] = tipos.get(t, 0) + 1

    print(f'\n  RESULTADO FINAL:')
    print(f'  Total tickers:    {len(ratios_final)}')
    for tipo, count in sorted(tipos.items()):
        print(f'  {tipo:<12}    {count}')
    print(f'  Solo Comafi:      {len(ratios_comafi)}')
    print(f'  Solo CajaValores: {len(ratios_cajval)}')

    # ── 5. Guardar JSON ───────────────────────────────────────
    meta = {
        '_meta': {
            'actualizado':    inicio.strftime('%Y-%m-%d %H:%M'),
            'total_tickers':  len(ratios_final),
            'fuentes': {
                'comafi':      len(ratios_comafi),
                'cajavaloroes': len(ratios_cajval),
            }
        }
    }
    output = {**meta, **ratios_final}

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f'\n  Guardado: {OUTPUT_JSON}')

    # ── 6. Guardar CSV (más fácil de inspeccionar) ────────────
    rows = []
    for ticker, d in ratios_final.items():
        rows.append({
            'ticker':  ticker,
            'byma':    d.get('byma', ticker),
            'ratio':   d['ratio'],
            'nombre':  d.get('nombre', ''),
            'tipo':    d['tipo'],
            'fuente':  d['fuente'],
        })
    df_out = pd.DataFrame(rows).sort_values(['tipo', 'ticker'])
    df_out.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    print(f'  Guardado: {OUTPUT_CSV}')

    # ── 7. Preview ────────────────────────────────────────────
    print(f'\n  MUESTRA (primeros 15):')
    print(f'  {"Ticker":<8} {"Ratio":>7}  {"Tipo":<10}  {"Fuente":<12}  Nombre')
    print(f'  {"─"*65}')
    for r in rows[:15]:
        print(f'  {r["ticker"]:<8} {r["ratio"]:>7.1f}  {r["tipo"]:<10}  {r["fuente"]:<12}  {r["nombre"][:30]}')

    elapsed = (datetime.now() - inicio).seconds
    print(f'\n  Tiempo: {elapsed}s')
    print(f'{"="*60}')
