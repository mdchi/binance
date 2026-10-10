#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de trading automatico en Binance.com

Modo Aislado
Apalancamiento 10x 
Monto: 100% de usdt de la cuenta futuros

Estrategia: Vela apertura
1) analizar la vela apertura en temporalidad 1 min
2) hacer una sola entrada a la vez, no hacer varias entradas en simultaneo

2) analizar vela apertura de bolsa de new york 10:30 hs (horario buenos aires por la mañana)

3) entrada en long: si la vela apertura analizada es una vela roja
   TP: 2% de ganancia descontando comisiones
   SL: 6% de perdida incluyendo comisiones

4) entrada en short: si la vela apertura analizada es una vela verde
   TP: 2% de ganancia descontando comisiones
   SL: 6% de perdida incluyendo comisiones

El formato del estado actual para estrategia:
en una linea: nombre de estrategia
en otra linea: precio
en otra linea: horario
en otra linea: posicion

Detalles:
1) Cerrar posiciones abiertas al iniciar bot

2) hacer archivo 2ganadas.txt donde van las operaciones que se ganaron y archivo 2perdidas.txt donde van las opereciones que se perdieron, con columnas alineadas, con los datos:
dia, hora, % ganancia maximo, % perdida maximo, duracion de la operacion

3) Mantener cabecera siempre visible en pantalla.
Mantener visible en pantalla unicamente el estado actual.
No utilizar colores en todo el texto visualizado en pantalla.
Reposicionar el cursor al inicio de la pantalla antes de actualizar la visualizacion de datos en vez de borrar la pantalla por completo.
Hacer operaciones con dinero real.

4) .envprivado con la configuracion de api de binance 
   .envpublico con el resto de parametros
"""

import os
import sys
import time
import math
import logging
from datetime import datetime, date, time as dt_time, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Zona horaria de Buenos Aires (UTC-3) y New York (ET)
TZ_BA = ZoneInfo("America/Argentina/Buenos_Aires")
TZ_NY = ZoneInfo("America/New_York")

# 1. Configuración de consola Windows para soporte ANSI/VT
if os.name == 'nt':
    os.system('')
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# 2. Configuración de Logging a bot.log (para mantener la pantalla limpia de texto no deseado)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)

# 3. Cargar variables de entorno (.envprivado y .envpublico)
for env_priv in [".envprivado", "envprivado", "envprivado.env"]:
    if os.path.exists(env_priv):
        load_dotenv(env_priv)

for env_pub in [".envpublico", "envpublico", ".env", "env"]:
    if os.path.exists(env_pub):
        load_dotenv(env_pub)

# 4. Importar biblioteca python-binance evitando colisión con el nombre de archivo binance.py
import importlib
local_bin_module = sys.modules.pop('binance', None)
sys_path_bak = list(sys.path)
cwd = os.getcwd()
file_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else cwd
sys.path = [p for p in sys.path if p not in ('', cwd, file_dir)]
try:
    binance_pkg = importlib.import_module('binance')
    Client = binance_pkg.client.Client
    BinanceAPIException = binance_pkg.exceptions.BinanceAPIException
finally:
    sys.path = sys_path_bak
    if local_bin_module is not None:
        sys.modules['binance'] = local_bin_module


class BinanceOpeningCandleBot:
    def __init__(self):
        # Claves API desde .envprivado
        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        # Parámetros desde .envpublico
        self.symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
        self.margin_pct = float(os.getenv("MARGIN_PCT", "100.0"))
        self.margin_usdt = float(os.getenv("MARGIN_USDT", "0.0"))  # 0.0 indica usar self.margin_pct (100%)
        self.leverage = int(os.getenv("LEVERAGE", "10"))
        self.margin_type = os.getenv("MARGIN_TYPE", "ISOLATED").upper()
        self.strategy_name = os.getenv("STRATEGY_NAME", "Vela apertura")
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.take_profit_pct = float(os.getenv("TAKE_PROFIT_PCT", "2.0"))
        self.stop_loss_pct = float(os.getenv("STOP_LOSS_PCT", "6.0"))
        self.fee_rate_pct = float(os.getenv("FEE_RATE_PCT", "0.05"))
        self.poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))

        # Modos de ejecución (dinero real por defecto)
        self.dry_run = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Configuración de apertura: Bolsa de New York 10:30 hs (Horario Buenos Aires)
        self.sessions = [
            {"bolsa": "NEW YORK", "hour": 10, "minute": 30}
        ]

        # Cliente Binance y reglas de precisión de mercado
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001
        self.last_execution_error = None
        self.actual_margin_used = 0.0

        # Control de velas analizadas (clave: "YYYY-MM-DD_BOLSA")
        self.processed_sessions = set()

        # Estado de posición activa (una sola entrada a la vez)
        self.current_position = None  # None, 'LONG', 'SHORT'
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.entry_time = None
        self.simulated_balance = 50.0

        # Métricas de la operación en curso
        self.max_gain_pct = 0.0
        self.max_loss_pct = 0.0

        # Métricas históricas de la sesión
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0

        # Inicialización de archivos y conexión
        self._init_trade_log_files()
        self._initialize_client()

    def _init_trade_log_files(self):
        """DETALLES 2: Inicializar 2ganadas.txt y 2perdidas.txt con columnas: dia, hora, % ganancia maximo, % perdida maximo, duracion de la operacion."""
        header = f"{'Dia':<10} | {'Hora':<8} | {'% Ganancia Max':<15} | {'% Perdida Max':<15} | {'Duracion':<10}\n"
        separator = "-" * 70 + "\n"
        for filename in ["2ganadas.txt", "2perdidas.txt"]:
            if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                try:
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(header)
                        f.write(separator)
                except Exception as e:
                    logging.error(f"Error inicializando {filename}: {e}")

    def _initialize_client(self):
        """Inicializa cliente Binance, configura modo AISLADO 10x y cierra posiciones previas."""
        monto_desc = f"{self.margin_usdt:.2f} USDT" if self.margin_usdt > 0 else f"{self.margin_pct:.0f}% de la cuenta USDT"
        logging.info("Iniciando Bot Binance Vela apertura (New York 10:30 hs)...")
        logging.info(f"Símbolo: {self.symbol} | Margen: {self.margin_type} | Apalancamiento: {self.leverage}x | Monto: {monto_desc}")

        try:
            if self.api_key and self.api_secret:
                if self.use_testnet:
                    self.client = Client(self.api_key, self.api_secret, testnet=True)
                else:
                    self.client = Client(self.api_key, self.api_secret)
            else:
                self.client = Client("", "")

            self._update_symbol_precision()

            if not self.dry_run and self.api_key and self.api_secret:
                self._setup_futures_account()
                logging.info("Conexión autenticada exitosamente a Binance Futures API con dinero real.")
            else:
                logging.info("Modo de simulación (DRY-RUN) activo o sin API keys configuradas.")

            # DETALLES 1: Cerrar posiciones abiertas al iniciar bot
            self.close_existing_positions()

        except Exception as e:
            logging.error(f"Error al inicializar cliente Binance: {e}")
            if not self.dry_run:
                logging.info("Cambiando automáticamente a modo DRY-RUN debido a error de autenticación/conexión.")
                self.dry_run = True

    def _update_symbol_precision(self):
        """Obtiene precisión de precio y lot size para el símbolo desde Binance Futures."""
        try:
            info = self.client.futures_exchange_info()
            for s in info.get('symbols', []):
                if s['symbol'] == self.symbol:
                    for f in s.get('filters', []):
                        if f['filterType'] == 'PRICE_FILTER':
                            self.tick_size = float(f['tickSize'])
                            self.price_precision = self._precision_from_step(f['tickSize'])
                        elif f['filterType'] == 'LOT_SIZE':
                            self.step_size = float(f['stepSize'])
                            self.min_qty = float(f['minQty'])
                            self.qty_precision = self._precision_from_step(f['stepSize'])
                    break
        except Exception as e:
            logging.warning(f"No se pudieron consultar filtros dinámicos de {self.symbol} ({e}). Usando valores por defecto.")

    @staticmethod
    def _precision_from_step(step_str):
        step = float(step_str)
        if step >= 1:
            return 0
        return int(round(-math.log10(step)))

    def _format_price(self, price):
        return round(round(price / self.tick_size) * self.tick_size, self.price_precision)

    def _format_quantity(self, qty):
        rounded = round(round(qty / self.step_size) * self.step_size, self.qty_precision)
        return max(rounded, self.min_qty)

    def _setup_futures_account(self):
        """Configura margen AISLADO (ISOLATED) y apalancamiento 10x en Binance Futures."""
        try:
            try:
                self.client.futures_change_margin_type(symbol=self.symbol, marginType='ISOLATED')
                logging.info(f"Margen configurado a ISOLATED para {self.symbol}.")
            except BinanceAPIException as e:
                # Código -4046: No need to change margin type
                if e.code != -4046 and "No need to change" not in str(e):
                    logging.warning(f"Nota de configuración de margen aislado: {e.message}")

            self.client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            logging.info(f"Apalancamiento configurado a {self.leverage}x para {self.symbol}.")
        except Exception as e:
            logging.error(f"Error configurando cuenta de futuros: {e}")

    def close_existing_positions(self):
        """DETALLES 1: Cerrar posiciones abiertas al iniciar bot."""
        logging.info("DETALLES 1: Verificando y cerrando cualquier posición abierta al iniciar el bot...")
        if self.dry_run:
            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_gain_pct = 0.0
            self.max_loss_pct = 0.0
            return

        if not self.client or not self.api_key or not self.api_secret:
            return

        try:
            # Cancelar órdenes pendientes existentes
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            except Exception as e:
                logging.warning(f"Nota cancelando órdenes previas: {e}")

            # Cancelar posibles órdenes condicionales
            try:
                self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": self.symbol})
            except Exception:
                pass

            # Detectar y cerrar posición activa
            positions = self.client.futures_position_information(symbol=self.symbol)
            closed_any = False
            for pos in positions:
                amt = float(pos['positionAmt'])
                if amt != 0:
                    side_to_close = 'SELL' if amt > 0 else 'BUY'
                    qty = self._format_quantity(abs(amt))
                    pos_type = 'LONG' if amt > 0 else 'SHORT'
                    logging.info(f"Cerrando posición previa {pos_type} de {qty} {self.symbol}...")
                    self.client.futures_create_order(
                        symbol=self.symbol,
                        side=side_to_close,
                        type='MARKET',
                        quantity=qty,
                        reduceOnly=True
                    )
                    closed_any = True
                    logging.info(f"Posición previa {pos_type} cerrada a mercado con éxito.")

            if not closed_any:
                logging.info(f"No hay posiciones abiertas previas para {self.symbol}.")

            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_gain_pct = 0.0
            self.max_loss_pct = 0.0

        except Exception as e:
            logging.error(f"Error al cerrar posiciones existentes: {e}")

    def get_active_position(self):
        """Consulta la posición actualmente abierta en Binance Futures."""
        if self.dry_run:
            return self.current_position, self.entry_price, self.position_qty

        try:
            positions = self.client.futures_position_information(symbol=self.symbol)
            for pos in positions:
                amt = float(pos['positionAmt'])
                if amt != 0:
                    side = 'LONG' if amt > 0 else 'SHORT'
                    entry = float(pos['entryPrice'])
                    qty = abs(amt)
                    return side, entry, qty
            return None, 0.0, 0.0
        except Exception as e:
            logging.error(f"Error consultando posiciones activas: {e}")
            return self.current_position, self.entry_price, self.position_qty

    def get_available_balance(self):
        """Consulta el saldo USDT disponible en la cuenta de Binance Futures."""
        if self.dry_run or not self.client or not self.api_key or not self.api_secret:
            return self.simulated_balance

        try:
            acc = self.client.futures_account()
            # Buscar balance específico de USDT en assets o availableBalance
            avail = float(acc.get('availableBalance', 0.0))
            for asset in acc.get('assets', []):
                if asset.get('asset') == 'USDT':
                    avail = float(asset.get('availableBalance', avail))
                    break
            return avail
        except Exception as e:
            logging.error(f"Error consultando balance disponible de USDT: {e}")
            return 0.0

    def get_target_margin_usdt(self):
        """Calcula el margen a utilizar en USDT según configuración (100% de la cuenta o fijo)."""
        if self.margin_usdt > 0.0:
            return self.margin_usdt
        avail = self.get_available_balance()
        target = (avail * (self.margin_pct / 100.0))
        # Asegurar un margen de seguridad mínimo (descontando pequeña fracción para comisiones de margen si aplica)
        margin = max(0.0, target * 0.995)
        return margin

    def get_latest_price(self):
        """Obtiene el precio más reciente de Binance Futures."""
        try:
            ticker = self.client.futures_symbol_ticker(symbol=self.symbol)
            return float(ticker['price'])
        except Exception as e:
            logging.error(f"Error al consultar precio de {self.symbol}: {e}")
            return 0.0

    def fetch_opening_candle(self, target_dt_ba):
        """
        Obtiene la vela de 1 minuto correspondiente al horario de apertura especificado (horario Buenos Aires).
        target_dt_ba: datetime con TZ_BA del minuto exacto de apertura (10:30:00).
        Retorna: dict con {open, high, low, close, is_closed} o None.
        """
        try:
            # Convertir el datetime de BA a timestamp UTC en milisegundos
            start_ts = int(target_dt_ba.timestamp() * 1000)
            end_ts = start_ts + 60000  # 1 minuto después

            klines = self.client.futures_klines(
                symbol=self.symbol,
                interval=self.timeframe,
                startTime=start_ts,
                endTime=end_ts,
                limit=5
            )

            if not klines:
                return None

            k = klines[0]
            candle_open_time = int(k[0])
            open_p = float(k[1])
            high_p = float(k[2])
            low_p = float(k[3])
            close_p = float(k[4])
            candle_close_time = int(k[6])

            # Verificar si la vela ya cerró (tiempo actual >= close_time)
            now_ms = int(time.time() * 1000)
            is_closed = now_ms >= candle_close_time

            return {
                "open_time": candle_open_time,
                "close_time": candle_close_time,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p,
                "is_closed": is_closed
            }
        except Exception as e:
            logging.error(f"Error al obtener vela de apertura para {target_dt_ba}: {e}")
            return None

    @staticmethod
    def _easter_date(year):
        """Calcula el Domingo de Pascua para determinar Viernes Santo (Good Friday)."""
        a = year % 19
        b = year // 100
        c = year % 100
        d = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i = c // 4
        k = c % 4
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1
        return date(year, month, day)

    @classmethod
    def get_nyse_holidays(cls, year):
        """Feriados en los que la Bolsa de New York (NYSE) está cerrada."""
        holidays = set()

        def add_observed(d):
            if d.weekday() == 5:    # Sábado -> se observa Viernes anterior
                holidays.add(d - timedelta(days=1))
            elif d.weekday() == 6:  # Domingo -> se observa Lunes posterior
                holidays.add(d + timedelta(days=1))
            else:
                holidays.add(d)

        def nth_weekday(yr, mn, target_weekday, n):
            count = 0
            cur = date(yr, mn, 1)
            while cur.month == mn:
                if cur.weekday() == target_weekday:
                    count += 1
                    if count == n:
                        return cur
                cur += timedelta(days=1)
            return None

        def last_weekday(yr, mn, target_weekday):
            cur = date(yr, mn + 1, 1) - timedelta(days=1) if mn < 12 else date(yr, 12, 31)
            while cur.month == mn:
                if cur.weekday() == target_weekday:
                    return cur
                cur -= timedelta(days=1)
            return None

        # 1. New Year's Day (1 de enero)
        nyd = date(year, 1, 1)
        if nyd.weekday() == 6:
            holidays.add(date(year, 1, 2))
        elif nyd.weekday() < 5:
            holidays.add(nyd)

        # 2. Martin Luther King Jr. Day (3er lunes de enero)
        holidays.add(nth_weekday(year, 1, 0, 3))
        # 3. Washington's Birthday (3er lunes de febrero)
        holidays.add(nth_weekday(year, 2, 0, 3))
        # 4. Good Friday (Viernes Santo)
        holidays.add(cls._easter_date(year) - timedelta(days=2))
        # 5. Memorial Day (Último lunes de mayo)
        holidays.add(last_weekday(year, 5, 0))
        # 6. Juneteenth (19 de junio)
        if year >= 2022:
            add_observed(date(year, 6, 19))
        # 7. Independence Day (4 de julio)
        add_observed(date(year, 7, 4))
        # 8. Labor Day (1er lunes de septiembre)
        holidays.add(nth_weekday(year, 9, 0, 1))
        # 9. Thanksgiving Day (4to jueves de noviembre)
        holidays.add(nth_weekday(year, 11, 3, 4))
        # 10. Christmas Day (25 de diciembre)
        add_observed(date(year, 12, 25))

        return {h for h in holidays if h is not None}

    def is_nyse_open_day(self, date_obj):
        """Verifica si la Bolsa de New York opera en la fecha indicada (Lunes a Viernes y no feriado NYSE)."""
        if date_obj.weekday() >= 5:
            return False
        return date_obj not in self.get_nyse_holidays(date_obj.year)

    def analyze_strategy(self, current_price, ba_now):
        """
        Evalúa las condiciones de la estrategia: Vela apertura
        - Bolsa de New York 10:30 hs (horario Buenos Aires por la mañana)
        - Únicamente cuando está abierta la Bolsa de New York (días hábiles y no feriados de NYSE)
        - Temporalidad: 1 min
        - Entrada en LONG: si la vela de apertura analizada es una vela roja (close < open)
        - Entrada en SHORT: si la vela de apertura analizada es una vela verde (close > open)
        """
        today_date = ba_now.date()

        next_session = None
        next_session_diff = None

        signal = None
        signal_bolsa = None
        candle_info = None

        for s in self.sessions:
            sess_time = dt_time(s["hour"], s["minute"], 0)
            target_dt = datetime.combine(today_date, sess_time, tzinfo=TZ_BA)
            session_key = f"{today_date.strftime('%Y%m%d')}_{s['bolsa']}"

            # Buscar la próxima sesión en la que la Bolsa de New York esté realmente abierta
            candidate_date = today_date
            candidate_dt = target_dt
            if ba_now >= candidate_dt or not self.is_nyse_open_day(candidate_date):
                candidate_date += timedelta(days=1)
                while not self.is_nyse_open_day(candidate_date):
                    candidate_date += timedelta(days=1)
                candidate_dt = datetime.combine(candidate_date, sess_time, tzinfo=TZ_BA)

            time_until = candidate_dt - ba_now
            if next_session_diff is None or time_until < next_session_diff:
                next_session_diff = time_until
                next_session = {
                    "bolsa": s["bolsa"],
                    "target_dt": candidate_dt,
                    "diff": time_until
                }

            # Si hoy la Bolsa de New York no está abierta (fin de semana o feriado bursátil), no operar
            if not self.is_nyse_open_day(today_date):
                continue

            # Diferencia en segundos respecto a la hora de apertura
            diff_seconds = (ba_now - target_dt).total_seconds()

            # Ventana de evaluación de la vela de apertura:
            # La vela abre a las 10:30:00 y cierra a las 10:31:00 (60 segundos).
            # Analizamos entre los 60 y 300 segundos posteriores a la apertura.
            if 60 <= diff_seconds <= 300:
                if session_key not in self.processed_sessions:
                    candle = self.fetch_opening_candle(target_dt)
                    if candle and candle["is_closed"]:
                        candle_info = candle
                        if candle["close"] < candle["open"]:
                            # Vela roja -> Entrada en LONG
                            signal = "LONG"
                            signal_bolsa = s["bolsa"]
                        elif candle["close"] > candle["open"]:
                            # Vela verde -> Entrada en SHORT
                            signal = "SHORT"
                            signal_bolsa = s["bolsa"]
                        else:
                            # Vela doji neutral
                            logging.info(f"Vela de apertura {s['bolsa']} doji neutral ({candle['open']} == {candle['close']}).")
                            self.processed_sessions.add(session_key)

                        if signal:
                            self.processed_sessions.add(session_key)

        return {
            "strategy_name": self.strategy_name,
            "current_price": current_price,
            "ba_now": ba_now,
            "signal": signal,
            "signal_bolsa": signal_bolsa,
            "candle_info": candle_info,
            "next_session": next_session
        }

    def open_position(self, side, current_price, bolsa):
        """
        Ejecuta apertura de posición:
        - Modo Aislado
        - Apalancamiento 10x
        - Monto: 100% de USDT de la cuenta futuros (o margen configurado)
        """
        margin_to_use = self.get_target_margin_usdt()
        notional_val = margin_to_use * self.leverage
        qty = self._format_quantity(notional_val / current_price)
        self.actual_margin_used = (qty * current_price) / self.leverage

        if qty < self.min_qty or self.actual_margin_used <= 0:
            err_msg = f"Margen insuficiente para operar {self.symbol} (Disponible: ${margin_to_use:.2f} USDT, Mínimo requerido: {self.min_qty} {self.symbol})"
            logging.error(err_msg)
            self.last_execution_error = err_msg
            return False

        logging.info(f"EJECUTANDO ENTRADA {side} ({bolsa}): Margen ${self.actual_margin_used:.2f} USDT ({self.margin_pct}%) x {self.leverage}x = ${qty * current_price:.2f} USDT ({qty} {self.symbol}) @ ${current_price:.2f}")

        if self.dry_run:
            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.entry_time = datetime.now()
            self.max_gain_pct = 0.0
            self.max_loss_pct = 0.0
            self.last_execution_error = None
            return True

        try:
            # Cancelar órdenes abiertas previas antes de entrar
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            except Exception:
                pass

            order_side = 'BUY' if side == 'LONG' else 'SELL'
            order = self.client.futures_create_order(
                symbol=self.symbol,
                side=order_side,
                type='MARKET',
                quantity=qty
            )
            logging.info(f"Orden de apertura enviada a Binance: OrderID={order.get('orderId')}")

            # Pausa para confirmar llenado
            time.sleep(1)
            active_side, real_entry, real_qty = self.get_active_position()
            if real_entry > 0:
                current_price = real_entry
                qty = real_qty

            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.actual_margin_used = (qty * current_price) / self.leverage
            self.entry_time = datetime.now()
            self.max_gain_pct = 0.0
            self.max_loss_pct = 0.0
            self.last_execution_error = None
            return True

        except Exception as e:
            self.last_execution_error = f"Error Binance: {e}"
            logging.error(f"Error al abrir posición en Binance Futures: {e}")
            return False

    def check_exit_condition(self, current_price):
        """
        Reglas de salida:
        - TP: 2% de ganancia descontando comisiones
        - SL: 6% de perdida incluyendo comisiones
        """
        if not self.current_position or self.entry_price <= 0:
            return False, None

        # ROE bruto sobre el margen (%):
        if self.current_position == 'LONG':
            gross_pnl_pct = ((current_price - self.entry_price) / self.entry_price) * self.leverage * 100.0
        else:
            gross_pnl_pct = ((self.entry_price - current_price) / self.entry_price) * self.leverage * 100.0

        # Impacto de comisiones (apertura + cierre) sobre el margen (%):
        # 2 órdenes * fee_rate * apalancamiento
        fee_impact_pct = 2.0 * self.fee_rate_pct * self.leverage

        # PnL neto en % sobre el margen
        net_pnl_pct = gross_pnl_pct - fee_impact_pct

        # TP: 2% de ganancia descontando comisiones
        if net_pnl_pct >= self.take_profit_pct:
            return True, f"TP alcanzado (Neto: +{net_pnl_pct:.2f}% >= +{self.take_profit_pct:.2f}%)"

        # SL: 6% de perdida incluyendo comisiones
        if net_pnl_pct <= -abs(self.stop_loss_pct):
            return True, f"SL alcanzado (Neto: {net_pnl_pct:.2f}% <= -{abs(self.stop_loss_pct):.2f}%)"

        return False, None

    def close_position(self, current_price, reason="Take Profit alcanzado"):
        """Cierra la posición actual a mercado y registra en 2ganadas.txt o 2perdidas.txt."""
        if not self.current_position:
            return

        side = self.current_position
        logging.info(f"CERRANDO POSICION {side} por {reason} @ ${current_price:.2f}...")

        exit_time = datetime.now()
        dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

        if not self.dry_run and self.client:
            try:
                side_to_close = 'SELL' if side == 'LONG' else 'BUY'
                qty = self._format_quantity(self.position_qty)
                self.client.futures_create_order(
                    symbol=self.symbol,
                    side=side_to_close,
                    type='MARKET',
                    quantity=qty,
                    reduceOnly=True
                )
                logging.info(f"Orden MARKET de cierre {side} ejecutada.")
            except Exception as e:
                logging.error(f"Error ejecutando orden de cierre en Binance: {e}")

        # Cálculo de PnL bruto
        if side == 'LONG':
            gross_pnl_usdt = (current_price - self.entry_price) * self.position_qty
        else:
            gross_pnl_usdt = (self.entry_price - current_price) * self.position_qty

        # Comisión estimada total (apertura y cierre)
        entry_notional = self.entry_price * self.position_qty
        exit_notional = current_price * self.position_qty
        total_fees = (entry_notional + exit_notional) * (self.fee_rate_pct / 100.0)

        # PnL neto considerando comisiones
        net_pnl_usdt = gross_pnl_usdt - total_fees

        if self.dry_run:
            self.simulated_balance += net_pnl_usdt

        # Actualizar métricas y guardar en archivo correspondiente (2ganadas.txt o 2perdidas.txt)
        self._record_and_save_trade(
            pnl_usdt=net_pnl_usdt,
            max_gain_pct=self.max_gain_pct,
            max_loss_pct=self.max_loss_pct,
            dur_mins=dur_mins,
            exit_time=exit_time
        )

        # Resetear estado de posición
        self.current_position = None
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.entry_time = None
        self.max_gain_pct = 0.0
        self.max_loss_pct = 0.0

    def _record_and_save_trade(self, pnl_usdt, max_gain_pct, max_loss_pct, dur_mins, exit_time):
        """
        DETALLES 2:
        - 2ganadas.txt donde van las operaciones que se ganaron
        - 2perdidas.txt donde van las operaciones que se perdieron
        Columnas alineadas:
        dia, hora, % ganancia maximo, % perdida maximo, duracion de la operacion
        """
        if pnl_usdt > 0:
            self.winning_trades += 1
            self.money_won += pnl_usdt
            filename = "2ganadas.txt"
        else:
            self.losing_trades += 1
            self.money_lost += abs(pnl_usdt)
            filename = "2perdidas.txt"

        dia_str = exit_time.strftime('%Y-%m-%d')
        hora_str = exit_time.strftime('%H:%M:%S')

        gain_str = f"+{max_gain_pct:.2f}%"
        loss_str = f"{max_loss_pct:.2f}%"
        dur_str = f"{dur_mins:.1f} min"

        line = f"{dia_str:<10} | {hora_str:<8} | {gain_str:<15} | {loss_str:<15} | {dur_str:<10}\n"

        try:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line)
            logging.info(f"Operación registrada en {filename}: {line.strip()}")
        except Exception as e:
            logging.error(f"Error escribiendo en {filename}: {e}")

    def render_screen(self, strat_data, active_pos, entry, pnl_pct, dur_mins):
        """
        DETALLES 3:
        - Mantener cabecera siempre visible en pantalla.
        - Mantener visible en pantalla unicamente el estado actual.
        - No utilizar colores en todo el texto visualizado en pantalla.
        - Reposicionar el cursor al inicio de la pantalla antes de actualizar en vez de borrar la pantalla por completo (\033[H).
        - Hacer operaciones con dinero real.

        El formato del estado actual para estrategia:
        en una linea: nombre de estrategia
        en otra linea: precio
        en otra linea: horario
        en otra linea: posicion
        """
        # Reposicionar el cursor al inicio de la pantalla (evita parpadeos y mantiene la visualización limpia)
        sys.stdout.write("\033[H")

        # Consultar balances para la cabecera
        if self.dry_run:
            wallet_bal = self.simulated_balance
            avail_bal = self.simulated_balance
            unrealized = 0.0
            has_keys = False
        elif self.client and self.api_key and self.api_secret:
            try:
                acc = self.client.futures_account()
                wallet_bal = float(acc.get('totalWalletBalance', 0.0))
                avail_bal = float(acc.get('availableBalance', 0.0))
                unrealized = float(acc.get('totalUnrealizedProfit', 0.0))
                has_keys = True
            except Exception:
                wallet_bal, avail_bal, unrealized, has_keys = 0.0, 0.0, 0.0, True
        else:
            wallet_bal, avail_bal, unrealized, has_keys = 0.0, 0.0, 0.0, False

        uptime_hours = (time.time() - self.bot_start_time) / 3600.0

        # Formatear línea de posición
        curr_price = strat_data['current_price']
        if active_pos and entry > 0:
            dur_str = f" ({dur_mins:.1f} min)"
            pnl_sign = "+" if pnl_pct >= 0 else ""
            fee_impact_pct = 2.0 * self.fee_rate_pct * self.leverage
            # Precios objetivo neto
            if active_pos == 'LONG':
                tp_price = entry * (1.0 + ((self.take_profit_pct + fee_impact_pct) / (self.leverage * 100.0)))
                sl_price = entry * (1.0 - ((self.stop_loss_pct - fee_impact_pct) / (self.leverage * 100.0)))
            else:
                tp_price = entry * (1.0 - ((self.take_profit_pct + fee_impact_pct) / (self.leverage * 100.0)))
                sl_price = entry * (1.0 + ((self.stop_loss_pct - fee_impact_pct) / (self.leverage * 100.0)))

            pos_line = f"{active_pos} @ ${entry:.2f} | ROE: {pnl_sign}{pnl_pct:.2f}%{dur_str} | TP: ${tp_price:.2f} (+{self.take_profit_pct:.1f}%) | SL: ${sl_price:.2f} (-{self.stop_loss_pct:.1f}%)"
        else:
            pos_line = "SIN POSICION"

        # Formatear línea de horario
        ba_now = strat_data['ba_now']
        now_str = ba_now.strftime('%Y-%m-%d %H:%M:%S')

        next_sess = strat_data.get('next_session')
        if next_sess:
            rem_secs = int(next_sess['diff'].total_seconds())
            h = rem_secs // 3600
            m = (rem_secs % 3600) // 60
            s = rem_secs % 60
            countdown_str = f" | Proxima apertura: {next_sess['bolsa']} 10:30 hs en {h:02d}h {m:02d}m {s:02d}s"
        else:
            countdown_str = ""

        horario_line = f"{now_str} (Buenos Aires){countdown_str}"

        # Construir líneas sin ningún código de color ANSI (texto plano monocromo)
        lines = []
        lines.append("======================================================================")
        lines.append("     BOT DE TRADING AUTOMATICO BINANCE - ESTRATEGIA VELA APERTURA     ")
        lines.append("======================================================================")
        monto_str = f"{self.margin_usdt:.2f} USDT" if self.margin_usdt > 0 else f"{self.margin_pct:.0f}% USDT ({avail_bal:.2f} USDT disp.)"
        lines.append(f"Simbolo: {self.symbol} | Modo: {self.margin_type} | Apalancamiento: {self.leverage}x | Monto: {monto_str}")
        lines.append(f"Modo de Ejecucion: {'SIMULACION (DRY-RUN)' if self.dry_run else 'DINERO REAL (Binance Futures)'}")
        lines.append("Apertura: New York 10:30 hs (Horario Buenos Aires)")
        lines.append(f"Reglas: Vela roja -> LONG | Vela verde -> SHORT | TP: {self.take_profit_pct:.1f}% neto | SL: {self.stop_loss_pct:.1f}%")
        lines.append("----------------------------------------------------------------------")
        if has_keys:
            lines.append(f"Saldo Wallet: {wallet_bal:.2f} USDT | Disponible: {avail_bal:.2f} USDT | PnL No Realizado: {unrealized:.2f} USDT")
        else:
            lines.append(f"Saldo Wallet: {wallet_bal:.2f} USDT (Simulado)")
        if self.last_execution_error:
            lines.append(f"Aviso de ejecucion: {self.last_execution_error}")
        lines.append(f"Resumen: Tiempo: {uptime_hours:.2f}h | Ganadas: {self.winning_trades} (+{self.money_won:.2f} USDT) | Perdidas: {self.losing_trades} (-{self.money_lost:.2f} USDT)")
        lines.append("======================================================================")

        # Formato del estado actual para estrategia (4 líneas requeridas):
        # en una linea: nombre de estrategia
        # en otra linea: precio
        # en otra linea: horario
        # en otra linea: posicion
        lines.append(f"nombre de estrategia: {strat_data['strategy_name']}")
        lines.append(f"precio: ${curr_price:.2f}")
        lines.append(f"horario: {horario_line}")
        lines.append(f"posicion: {pos_line}")
        lines.append("======================================================================")

        # Borrar hasta fin de línea (\033[K) y fin de pantalla (\033[J) sin alterar cursor
        rendered = "\n".join(l + "\033[K" for l in lines) + "\033[J\n"
        sys.stdout.write(rendered)
        sys.stdout.flush()

    def run(self):
        """Bucle principal de ejecución del bot."""
        logging.info("Bucle principal de monitoreo Vela apertura iniciado.")

        # Limpiar la pantalla por única vez al arrancar
        os.system('cls' if os.name == 'nt' else 'clear')

        while True:
            try:
                ba_now = datetime.now(TZ_BA)
                curr_price = self.get_latest_price()

                if curr_price == 0.0:
                    time.sleep(self.poll_interval)
                    continue

                # 1. Consultar si hay una posición activa actualmente
                active_pos, entry, qty = self.get_active_position()

                # Si no está en dry_run pero externamente se cerró la posición en Binance
                if not self.dry_run and active_pos is None and self.current_position is not None:
                    exit_time = datetime.now()
                    dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0
                    pnl = (curr_price - self.entry_price) * self.position_qty if self.current_position == 'LONG' else (self.entry_price - curr_price) * self.position_qty
                    self._record_and_save_trade(pnl, self.max_gain_pct, self.max_loss_pct, dur_mins, exit_time)
                    self.current_position = None
                    self.entry_price = 0.0
                    self.position_qty = 0.0
                    self.entry_time = None
                    self.max_gain_pct = 0.0
                    self.max_loss_pct = 0.0

                pnl_pct = 0.0
                dur_mins = 0.0
                if active_pos and entry > 0:
                    dur_mins = (datetime.now() - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0
                    if active_pos == 'LONG':
                        pnl_pct = ((curr_price - entry) / entry) * self.leverage * 100.0
                    else:
                        pnl_pct = ((entry - curr_price) / entry) * self.leverage * 100.0

                    if pnl_pct > self.max_gain_pct:
                        self.max_gain_pct = pnl_pct
                    if pnl_pct < self.max_loss_pct:
                        self.max_loss_pct = pnl_pct

                # 2. Analizar vela de apertura de New York 10:30 hs (BA)
                strat_data = self.analyze_strategy(curr_price, ba_now)

                # 3. Renderizar pantalla monocroma con cabecera y estado actual
                self.render_screen(
                    strat_data=strat_data,
                    active_pos=active_pos,
                    entry=entry,
                    pnl_pct=pnl_pct,
                    dur_mins=dur_mins
                )

                # 4. Lógica de salidas:
                # - TP: 2% de ganancia descontando comisiones
                # - SL: 6% de perdida incluyendo comisiones
                should_close, close_reason = self.check_exit_condition(curr_price)
                if active_pos and should_close:
                    self.close_position(curr_price, reason=close_reason)
                    active_pos = None

                # 5. Lógica de entradas:
                # - Hacer una sola entrada a la vez, no hacer varias entradas en simultáneo
                # - Si se detectó señal en la vela de apertura de New York 10:30 hs
                if active_pos is None and strat_data["signal"]:
                    sig = strat_data["signal"]
                    bolsa = strat_data["signal_bolsa"]
                    self.open_position(side=sig, current_price=curr_price, bolsa=bolsa)

                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                print("\n[!] Bot detenido por el usuario.")
                break
            except Exception as e:
                logging.error(f"Excepción en bucle principal: {e}")
                time.sleep(self.poll_interval)


if __name__ == "__main__":
    bot = BinanceOpeningCandleBot()
    bot.run()
