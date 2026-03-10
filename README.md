# 📊 SMC Discount Zone Screener

Screener automático basado en la lógica del indicador **Smart Money Concepts de LuxAlgo**.  
Detecta acciones del **S&P 500 + NASDAQ 100** en zona de **Discount** usando timeframe diario.

---

## 🚀 Cómo usar

### Correr manualmente
1. Ir a la pestaña **Actions** en GitHub
2. Click en **SMC Discount Zone Screener**
3. Click en **Run workflow** → **Run workflow**
4. Esperar ~15 minutos
5. Descargar el CSV desde **Artifacts**

### Correr automáticamente
El workflow corre solo **de lunes a viernes a las 19:00 (hora Argentina)**.  
El CSV más reciente siempre está en `results/smc_discount_latest.csv`.

---

## 📋 Columnas del CSV

| Columna | Descripción |
|---|---|
| `Ticker` | Símbolo de la acción |
| `Precio` | Precio de cierre actual |
| `Swing_High` | Trailing swing high (LuxAlgo) |
| `Swing_Low` | Trailing swing low (LuxAlgo) |
| `Discount_Top` | Techo de la zona Discount |
| `Discount_Bot` | Piso de la zona Discount |
| `Pct_en_Zona` | % dentro de la zona (0% = fondo, 100% = techo) |
| `Pct_desde_Low` | % de subida desde el swing low |
| `RSI` | RSI de 14 períodos |
| `Vol_Ratio_20d` | Volumen actual vs promedio 20 días |
| `Tendencia` | Bullish / Bearish basado en swing highs |
| `TradingView` | Link directo al chart |

---

## 🧠 Lógica de zonas (replica LuxAlgo exacto)

```
Premium:      0.95 * SwingHigh + 0.05 * SwingLow   →  SwingHigh
Equilibrium:  zona media del rango
Discount:     SwingLow  →  0.95 * SwingLow + 0.05 * SwingHigh  ✅
```

---

## 🔍 Flujo recomendado

1. Descargá el CSV de Artifacts después de cada run
2. Filtrá: **Tendencia = Bullish + RSI < 40 + Pct_en_Zona < 40%**
3. Abrí los top 5-10 en TradingView con el link de la columna `TradingView`
4. Confirmá con el indicador LuxAlgo SMC visualmente
5. Buscá confluence con FVGs u Order Blocks
