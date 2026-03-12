"""
SMC Telegram Bot — Polling via GitHub Actions
==============================================
Corre cada 5 minutos via workflow.
Chequea mensajes nuevos y responde comandos.
Comandos:
  /reporte  — reporte completo SMC
  /ccl      — CCL actual
  /hits     — solo tickers con senal
  /ayuda    — lista de comandos
"""

import os
import json
import time
import requests
import logging
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('bot')

TZ_ARG   = ZoneInfo('America/Argentina/Buenos_Aires')
OFFSET_F = 'results/telegram_offset.txt'   # persiste el ultimo update_id procesado


# ============================================================
# TELEGRAM HELPERS
# ============================================================

def get_updates(token: str, offset: int = 0) -> list:
    """Obtiene mensajes nuevos desde Telegram."""
    try:
        r = requests.get(
            f'https://api.telegram.org/bot{token}/getUpdates',
            params={'offset': offset, 'timeout': 10, 'limit': 10},
            timeout=15
        )
        if r.status_code == 200:
            return r.json().get('result', [])
    except Exception as e:
        log.error(f"getUpdates error: {e}")
    return []


def send_message(token: str, chat_id: str, texto: str, parse_mode: str = 'HTML') -> bool:
    """Envia mensaje dividiendo en chunks si es necesario."""
    url      = f'https://api.telegram.org/bot{token}/sendMessage'
    max_len  = 4000
    chunks   = [texto[i:i+max_len] for i in range(0, len(texto), max_len)]
    ok_count = 0
    for chunk in chunks:
        try:
            r = requests.post(url, json={
                'chat_id':    chat_id,
                'text':       chunk,
                'parse_mode': parse_mode,
            }, timeout=15)
            if r.status_code == 200:
                ok_count += 1
            else:
                log.error(f"sendMessage error {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.error(f"sendMessage excepcion: {e}")
        if len(chunks) > 1:
            time.sleep(0.5)
    return ok_count == len(chunks)


def send_typing(token: str, chat_id: str):
    """Muestra 'escribiendo...' mientras procesa."""
    try:
        requests.post(
            f'https://api.telegram.org/bot{token}/sendChatAction',
            json={'chat_id': chat_id, 'action': 'typing'},
            timeout=5
        )
    except Exception:
        pass


# ============================================================
# OFFSET — para no reprocesar mensajes viejos
# ============================================================

def leer_offset() -> int:
    """Lee el ultimo offset procesado desde archivo."""
    try:
        if os.path.exists(OFFSET_F):
            with open(OFFSET_F) as f:
                return int(f.read().strip())
    except Exception:
        pass
    return 0


def guardar_offset(offset: int):
    """Guarda el ultimo offset procesado."""
    try:
        os.makedirs('results', exist_ok=True)
        with open(OFFSET_F, 'w') as f:
            f.write(str(offset))
    except Exception as e:
        log.error(f"Error guardando offset: {e}")


# ============================================================
# COMANDOS
# ============================================================

def cmd_ccl() -> str:
    """Obtiene el CCL en tiempo real."""
    # Intentar CriptoYa
    try:
        r = requests.get('https://criptoya.com/api/dolar', timeout=5)
        if r.status_code == 200:
            data  = r.json()
            ccl   = data.get('ccl', {})
            venta = ccl.get('ask') or ccl.get('venta') or ccl.get('price')
            if venta and float(venta) > 100:
                return (
                    f"<b>💵 CCL ACTUAL</b>\n"
                    f"${float(venta):,.2f} ARS/USD\n"
                    f"Fuente: CriptoYa\n"
                    f"Hora: {datetime.now(TZ_ARG).strftime('%H:%M')} ARG"
                )
    except Exception:
        pass
    # Intentar DolarAPI
    try:
        r = requests.get('https://dolarapi.com/v1/dolares/contadoconliqui', timeout=5)
        if r.status_code == 200:
            data  = r.json()
            venta = data.get('venta')
            if venta and float(venta) > 100:
                return (
                    f"<b>💵 CCL ACTUAL</b>\n"
                    f"${float(venta):,.2f} ARS/USD\n"
                    f"Fuente: DolarAPI\n"
                    f"Hora: {datetime.now(TZ_ARG).strftime('%H:%M')} ARG"
                )
    except Exception:
        pass
    return "⚠️ No se pudo obtener el CCL ahora. Intentá de nuevo en unos minutos."


def cmd_hits() -> str:
    """Lee el ultimo reporte y extrae solo los HITs."""
    fname = 'results/reporte_latest.txt'
    if not os.path.exists(fname):
        return "⚠️ No hay reporte generado todavia. Usa /reporte para generarlo."

    try:
        with open(fname, encoding='utf-8') as f:
            contenido = f.read()

        # Extraer fecha del reporte
        fecha_linea = ''
        for linea in contenido.split('\n'):
            if 'Generado:' in linea:
                fecha_linea = linea.strip()
                break

        # Buscar tickers con señal (TICKER: XXX | Score:)
        hits = []
        for linea in contenido.split('\n'):
            if 'TICKER:' in linea and 'Score:' in linea:
                hits.append(linea.strip())

        if not hits:
            return (
                f"<b>📊 RESUMEN — Sin señales hoy</b>\n"
                f"{fecha_linea}\n\n"
                f"El mercado está fuera de zona de valor.\n"
                f"Capital que no se pierde, no necesita recuperarse."
            )

        lineas = [f"<b>📊 SEÑALES ACTIVAS ({len(hits)})</b>", fecha_linea, ""]
        for h in hits:
            # Limpiar y formatear
            h_clean = h.replace('TICKER:', '').strip()
            lineas.append(f"• {h_clean}")

        lineas.append("")
        lineas.append("Usa /reporte para el análisis completo.")
        return '\n'.join(lineas)

    except Exception as e:
        log.error(f"cmd_hits error: {e}")
        return "⚠️ Error leyendo el reporte. Intenta /reporte para regenerarlo."


def cmd_ayuda() -> str:
    return (
        "<b>🤖 SMC Screener — Comandos disponibles</b>\n\n"
        "/reporte — Reporte completo (~2 min)\n"
        "/hits    — Solo tickers con señal hoy\n"
        "/ccl     — CCL en tiempo real\n"
        "/ayuda   — Esta ayuda\n\n"
        "<i>El reporte automático llega todos los días a las 7AM ARG.</i>"
    )


def cmd_reporte(token: str, chat_id: str):
    """
    Genera el reporte completo corriendo reporte_diario.py como subprocess.
    Manda mensajes de estado mientras procesa.
    """
    send_message(token, chat_id,
        "⏳ <b>Generando reporte completo...</b>\n"
        "Esto tarda ~2 minutos. Te aviso cuando esté listo."
    )
    send_typing(token, chat_id)

    try:
        log.info("Corriendo reporte_diario.py...")
        result = subprocess.run(
            ['python', 'scripts/reporte_diario.py'],
            capture_output=True, text=True, timeout=300
        )

        if result.returncode == 0:
            # El propio reporte_diario.py ya envía a Telegram
            # Solo confirmamos que corrió bien
            log.info("reporte_diario.py terminó OK")
            # Si por alguna razón no envió, mandamos el archivo
            fname = 'results/reporte_latest.txt'
            if os.path.exists(fname):
                with open(fname, encoding='utf-8') as f:
                    reporte = f.read()
                # Verificar que no se haya enviado ya (el script lo hace)
                # Para evitar duplicado, solo mandamos si hay error en el log
                if 'Telegram: enviado OK' not in result.stdout:
                    send_message(token, chat_id,
                        f"<pre>{reporte[:3800]}</pre>"
                    )
        else:
            log.error(f"reporte_diario.py error: {result.stderr[:500]}")
            send_message(token, chat_id,
                f"⚠️ <b>Error generando el reporte:</b>\n"
                f"<pre>{result.stderr[:500]}</pre>"
            )
    except subprocess.TimeoutExpired:
        send_message(token, chat_id,
            "⚠️ El reporte tardó demasiado. "
            "Intenta de nuevo en unos minutos."
        )
    except Exception as e:
        log.error(f"cmd_reporte excepcion: {e}")
        send_message(token, chat_id, f"⚠️ Error inesperado: {e}")


# ============================================================
# PROCESADOR DE MENSAJES
# ============================================================

def procesar_update(update: dict, token: str, chat_id_autorizado: str):
    """Procesa un update de Telegram y ejecuta el comando correspondiente."""
    mensaje = update.get('message', {})
    if not mensaje:
        return

    chat_id_msg = str(mensaje.get('chat', {}).get('id', ''))
    texto       = mensaje.get('text', '').strip().lower()
    usuario     = mensaje.get('from', {}).get('username', 'desconocido')

    log.info(f"Mensaje de @{usuario} ({chat_id_msg}): {texto}")

    # Seguridad: solo responder al chat autorizado
    if chat_id_msg != chat_id_autorizado:
        log.warning(f"Mensaje de chat no autorizado: {chat_id_msg}")
        return

    # Rutear comandos
    if texto in ['/reporte', '/reporte@smc_arg_bot']:
        cmd_reporte(token, chat_id_msg)

    elif texto in ['/ccl', '/ccl@smc_arg_bot']:
        respuesta = cmd_ccl()
        send_message(token, chat_id_msg, respuesta)

    elif texto in ['/hits', '/hits@smc_arg_bot']:
        respuesta = cmd_hits()
        send_message(token, chat_id_msg, respuesta)

    elif texto in ['/ayuda', '/ayuda@smc_arg_bot', '/start', '/help']:
        respuesta = cmd_ayuda()
        send_message(token, chat_id_msg, respuesta)

    else:
        send_message(token, chat_id_msg,
            f"No entiendo ese comando. Usa /ayuda para ver los disponibles."
        )


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    TOKEN   = os.environ.get('TELEGRAM_TOKEN', '')
    CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

    if not TOKEN or not CHAT_ID:
        log.error("TELEGRAM_TOKEN o TELEGRAM_CHAT_ID no configurados")
        exit(1)

    log.info(f"Bot iniciado — {datetime.now(TZ_ARG).strftime('%d/%m/%Y %H:%M')} ARG")

    # Leer offset para no reprocesar mensajes viejos
    offset = leer_offset()
    log.info(f"Offset actual: {offset}")

    # Obtener updates nuevos
    updates = get_updates(TOKEN, offset)
    log.info(f"Updates nuevos: {len(updates)}")

    if not updates:
        log.info("Sin mensajes nuevos.")
        exit(0)

    # Procesar cada update
    max_offset = offset
    for update in updates:
        update_id = update.get('update_id', 0)
        max_offset = max(max_offset, update_id)
        procesar_update(update, TOKEN, CHAT_ID)

    # Guardar nuevo offset (update_id + 1 para no reprocesar)
    guardar_offset(max_offset + 1)
    log.info(f"Offset guardado: {max_offset + 1}")
