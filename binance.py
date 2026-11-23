#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

# Zonas horarias
TZ_BA = ZoneInfo("America/Argentina/Buenos_Aires")
TZ_NY = ZoneInfo("America/New_York")

# 1. Habilitar soporte VT/ANSI en consola Windows
if os.name == 'nt':
    os.system('')
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# 2. Configuración de Logging a bot.log (mantiene limpia la pantalla)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)

# 3. Cargar variables de entorno: .envprivado y .envpublico
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
    Client = binance_pkg.Client
    BinanceAPIException = binance_pkg.exceptions.BinanceAPIException
finally:
    sys.path = sys_path_bak
    if local_bin_module is not None:
        sys.modules['binance'] = local_bin_module


class BinanceOpeningCandleBot:
    # Columnas en 2 líneas para achicar el ancho de las columnas en 2datos.txt
    # (encabezado línea 1, encabezado línea 2, ancho de columna)
    DATA_COLUMNS = [
        ("dia", "", 10),
        ("hora", "", 8),
        ("long/", "short", 6),
        ("duracion", "operacion", 9),
        ("% ganancia", "maximo", 10),
        ("% perdida", "maximo", 9),
        ("ROI", "", 11),
        ("% ROI", "", 8),
        ("comisiones", "", 11),
        ("% comi-", "siones", 7),
        ("verde/", "roja", 6),
        ("cuerpo", "", 8),
        ("mechasup", "", 8),
        ("mechainf", "", 8),
    ]

    def __init__(self):
        # 5) Claves API desde .envprivado
        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        # 5) Parámetros desde .envpublico
        self.symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
        self.margin_type = os.getenv("MARGIN_TYPE", "ISOLATED").upper()
        self.leverage = int(os.getenv("LEVERAGE", "30"))
        self.margin_pct = float(os.getenv("MARGIN_PCT", "90.0"))
        self.margin_usdt_fixed = float(os.getenv("MARGIN_USDT", "0.0"))

        self.strategy_name = os.getenv("STRATEGY_NAME", "Vela apertura")
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.min_body = float(os.getenv("MIN_BODY", "30.0"))

        # 7) TP y SL por ROI
        self.take_profit_pct = float(os.getenv("TAKE_PROFIT_PCT", os.getenv("TP_ROI", "8.0")))
        self.stop_loss_pct = float(os.getenv("STOP_LOSS_PCT", os.getenv("SL_ROI", "10.0")))

        # Estimación de comisión para comisiones de salida/entrada (0.05% taker Binance Futures)
        self.fee_rate_pct = float(os.getenv("FEE_RATE_PCT", "0.05"))

        # 4) Horario de apertura de la Bolsa de New York (09:30 hs horario New York)
        self.nyse_open_hour = int(os.getenv("NYSE_OPEN_HOUR", "9"))
        self.nyse_open_minute = int(os.getenv("NYSE_OPEN_MINUTE", "30"))

        # Modos de ejecución
        self.dry_run = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")
        self.poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))

        # Cliente Binance y filtros dinámicos
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001
        self.last_execution_error = None
        self.actual_margin_used = 0.0

        # Control de velas analizadas (clave: "YYYY-MM-DD_NYSE")
        self.processed_sessions = set()

        # 3) Estado de posición activa (máximo 1 entrada a la vez)
        self.current_position = None  # None, 'LONG', 'SHORT'
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.entry_time = None
        self.simulated_balance = 50.0

        # Métricas de la operación en curso
        self.max_gain_pct = 0.0
        self.max_loss_pct = 0.0
        self.entry_color = ""
        self.entry_cuerpo = 0.0
        self.entry_mechasup = 0.0
        self.entry_mechainf = 0.0

        # Métricas históricas de la sesión
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0

        # Inicialización de archivo 2datos.txt y conexión
        self._init_data_file()
        self._load_historical_metrics()
        self._initialize_client()

    def _init_data_file(self):
        """DETALLES 5: Inicializa 2datos.txt con cabecera de 2 líneas alineadas para achicar ancho de columnas."""
        widths = [col[2] for col in self.DATA_COLUMNS]
        h_line1 = " | ".join(col[0].ljust(col[2]) for col in self.DATA_COLUMNS) + "\n"
        h_line2 = " | ".join(col[1].ljust(col[2]) for col in self.DATA_COLUMNS) + "\n"
        separator = "-+-".join("-" * w for w in widths) + "\n"
        filename = "2datos.txt"
        if not os.path.exists(filename) or os.path.getsize(filename) == 0:
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(h_line1)
                    f.write(h_line2)
                    f.write(separator)
            except Exception as e:
                logging.error(f"Error inicializando {filename}: {e}")

    def _load_historical_metrics(self):
        """Carga métricas históricas desde 2datos.txt para mantener contadores al reiniciar."""
        filename = "2datos.txt"
        if not os.path.exists(filename):
            return
        try:
            with open(filename, "r", encoding="utf-8") as f:
                for line in f:
                    parts = [p.strip() for p in line.split("|")]
                    if len(parts) >= 14:
                        try:
                            val_str = parts[6].replace("USDT", "").replace("+", "").strip()
                            roi_val = float(val_str)
                            if roi_val >= 0:
                                self.winning_trades += 1
                                self.money_won += roi_val
                            else:
                                self.losing_trades += 1
                                self.money_lost += abs(roi_val)
                        except Exception:
                            pass
        except Exception as e:
            logging.warning(f"Nota al leer métricas históricas: {e}")

    def _initialize_client(self):
        """Inicializa cliente Binance, configura modo AISLADO 30x y cierra posiciones previas."""
        logging.info("Iniciando Bot Binance Vela apertura (NYSE 09:30 hs New York)...")
        logging.info(f"Símbolo: {self.symbol} | Modo: {self.margin_type} | Apalancamiento: {self.leverage}x | Monto: {self.margin_pct}% de la cuenta USDT")

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
                logging.info("Modo de simulación (DRY-RUN) activo.")

            # DETALLES 1: Cerrar posiciones abiertas al iniciar bot
            self.close_existing_positions()

        except Exception as e:
            logging.error(f"Error al inicializar cliente Binance: {e}")
            if not self.dry_run:
                logging.info("Cambiando automáticamente a modo DRY-RUN debido a error de autenticación/conexión.")
                self.dry_run = True

    def _update_symbol_precision(self):
        """Obtiene filtros de precio y cantidad para el símbolo desde Binance Futures."""
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
        """Ajusta cantidad hacia abajo (floor) para asegurar que nunca exceda el margen disponible."""
        if self.step_size <= 0:
            return qty
        steps = math.floor(qty / self.step_size)
        formatted_qty = round(steps * self.step_size, self.qty_precision)
        if self.qty_precision == 0:
            formatted_qty = int(formatted_qty)
        return max(formatted_qty, self.min_qty)

    def _setup_futures_account(self):
        """Configura margen AISLADO (ISOLATED) y apalancamiento 30x en Binance Futures."""
        try:
            # 1) Modo Aislado
            try:
                self.client.futures_change_margin_type(symbol=self.symbol, marginType='ISOLATED')
                logging.info(f"Margen configurado a ISOLATED para {self.symbol}.")
            except BinanceAPIException as e:
                if e.code != -4046 and "No need to change" not in str(e):
                    logging.warning(f"Nota configuración margen aislado: {e.message}")

            # 1) Apalancamiento 30x
            self.client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            logging.info(f"Apalancamiento configurado a {self.leverage}x para {self.symbol}.")
        except Exception as e:
            logging.error(f"Error configurando cuenta de futuros: {e}")

    def get_futures_positions(self, symbol=None):
        """Consulta posiciones activas mediante endpoint v2 con fallback para máxima compatibilidad."""
        params = {'symbol': symbol} if symbol else {}
        try:
            res = self.client._request_futures_api('get', 'positionRisk', True, 2, data=params)
            if isinstance(res, list) and len(res) > 0:
                return res
        except Exception:
            pass

        try:
            res = self.client.futures_position_information(**params)
            if isinstance(res, list) and len(res) > 0:
                return res
        except Exception:
            pass

        try:
            acc = self.client.futures_account()
            positions = acc.get('positions', [])
            if symbol:
                return [p for p in positions if p.get('symbol') == symbol]
            return positions
        except Exception as e:
            logging.error(f"Error consultando posiciones: {e}")
            return []

    def close_existing_positions(self):
        """DETALLES 1: Cerrar posiciones abiertas al iniciar bot."""
        logging.info("DETALLES 1: Verificando y cerrando cualquier posición abierta al iniciar el bot...")
        if self.dry_run or not self.client or not self.api_key or not self.api_secret:
            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_gain_pct = 0.0
            self.max_loss_pct = 0.0
            return

        try:
            # Cancelar órdenes pendientes del símbolo
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                logging.info(f"Órdenes pendientes canceladas para {self.symbol}.")
            except Exception as e:
                logging.warning(f"Nota cancelando órdenes previas de {self.symbol}: {e}")

            # Cancelar posibles órdenes condicionales
            try:
                self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": self.symbol})
            except Exception:
                pass

            # Detectar y cerrar cualquier posición abierta
            positions = self.get_futures_positions()
            closed_any = False
            for pos in positions:
                amt = float(pos.get('positionAmt', 0.0))
                sym = pos.get('symbol')
                if amt != 0:
                    side_to_close = 'SELL' if amt > 0 else 'BUY'
                    qty = self._format_quantity(abs(amt)) if sym == self.symbol else abs(amt)
                    pos_type = 'LONG' if amt > 0 else 'SHORT'
                    logging.info(f"Cerrando posición abierta previa {pos_type} de {qty} en {sym}...")
                    try:
                        self.client.futures_cancel_all_open_orders(symbol=sym)
                    except Exception:
                        pass
                    self.client.futures_create_order(
                        symbol=sym,
                        side=side_to_close,
                        type='MARKET',
                        quantity=qty,
                        reduceOnly=True
                    )
                    closed_any = True
                    logging.info(f"Posición previa {pos_type} en {sym} cerrada a mercado con éxito.")

            if not closed_any:
                logging.info("Sin posiciones abiertas previas en la cuenta de futuros.")

            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_gain_pct = 0.0
            self.max_loss_pct = 0.0

        except Exception as e:
            logging.error(f"Error al verificar/cerrar posiciones existentes: {e}")

    def get_active_position(self):
        """Consulta la posición actualmente abierta para el símbolo en Binance Futures."""
        if self.dry_run:
            return self.current_position, self.entry_price, self.position_qty

        try:
            positions = self.get_futures_positions(self.symbol)
            for pos in positions:
                amt = float(pos.get('positionAmt', 0.0))
                if amt != 0:
                    side = 'LONG' if amt > 0 else 'SHORT'
                    entry = float(pos.get('entryPrice', 0.0))
                    qty = abs(amt)
                    return side, entry, qty
            return None, 0.0, 0.0
        except Exception as e:
            logging.error(f"Error consultando posición activa de {self.symbol}: {e}")
            return self.current_position, self.entry_price, self.position_qty

    def get_available_balance(self):
        """Consulta el saldo USDT disponible en la cuenta de Binance Futures."""
        if self.dry_run or not self.client or not self.api_key or not self.api_secret:
            return self.simulated_balance

        try:
            acc = self.client.futures_account()
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
        """Calcula el margen a utilizar en USDT (90% de la cuenta de futuros)."""
        if self.margin_usdt_fixed > 0.0:
            return self.margin_usdt_fixed
        avail = self.get_available_balance()
        target = avail * (self.margin_pct / 100.0)
        # Factor de seguridad del 99.5% para garantizar aprobación inmediata de margen
        return max(0.0, target * 0.995)

    def get_latest_price(self):
        """Obtiene el precio más reciente de Binance Futures."""
        try:
            ticker = self.client.futures_symbol_ticker(symbol=self.symbol)
            return float(ticker['price'])
        except Exception as e:
            logging.error(f"Error al consultar precio de {self.symbol}: {e}")
            return 0.0

    def fetch_opening_candle(self, target_dt_ny):
        """
        Obtiene la vela de 1 minuto correspondiente a las 09:30 hs (horario New York).
        target_dt_ny: datetime con TZ_NY del minuto exacto de apertura (09:30:00).
        Retorna: dict con {open, high, low, close, is_closed} o None.
        """
        try:
            start_ts = int(target_dt_ny.timestamp() * 1000)
            end_ts = start_ts + 60000

            klines = self.client.futures_klines(
                symbol=self.symbol,
                interval=self.timeframe,
                startTime=start_ts,
                endTime=end_ts,
                limit=2
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

            now_ms = int(time.time() * 1000)
            is_closed = now_ms >= candle_close_time

            # 5) cuerpo, mechasup, mechainf en vela apertura 1 min
            cuerpo = abs(open_p - close_p)
            mechasup = abs(high_p - max(open_p, close_p))
            mechainf = abs(low_p - min(open_p, close_p))

            return {
                "open_time": candle_open_time,
                "close_time": candle_close_time,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p,
                "cuerpo": cuerpo,
                "mechasup": mechasup,
                "mechainf": mechainf,
                "is_closed": is_closed
            }
        except Exception as e:
            logging.error(f"Error al obtener vela de apertura para {target_dt_ba}: {e}")
            return None

    @staticmethod
    def _easter_date(year):
        """Calcula el Domingo de Pascua para determinar Viernes Santo."""
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
            if d.weekday() == 5:
                holidays.add(d - timedelta(days=1))
            elif d.weekday() == 6:
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
        4) Analizar vela apertura de bolsa de New York 09:30 hs (horario New York)
           únicamente los días que está abierta la bolsa de New York.
        2) Temporalidad 1 min.
        6) Entrada en long: si la vela apertura es roja.
           Entrada en short: si la vela apertura es verde.
        """
        ny_now = datetime.now(TZ_NY)
        today_date_ny = ny_now.date()
        target_time = dt_time(self.nyse_open_hour, self.nyse_open_minute, 0)
        target_dt_ny = datetime.combine(today_date_ny, target_time, tzinfo=TZ_NY)
        session_key = f"{today_date_ny.strftime('%Y%m%d')}_NYSE"

        # Buscar próxima sesión NYSE
        candidate_date = today_date_ny
        candidate_dt = target_dt_ny
        if ny_now >= candidate_dt or not self.is_nyse_open_day(candidate_date):
            candidate_date += timedelta(days=1)
            while not self.is_nyse_open_day(candidate_date):
                candidate_date += timedelta(days=1)
            candidate_dt = datetime.combine(candidate_date, target_time, tzinfo=TZ_NY)

        next_session_diff = candidate_dt - ny_now
        next_session = {
            "bolsa": "NEW YORK",
            "target_dt": candidate_dt,
            "diff": next_session_diff
        }

        signal = None
        candle_info = None

        # Verificar si hoy está abierta la Bolsa de New York
        if not self.is_nyse_open_day(today_date_ny):
            return {
                "strategy_name": self.strategy_name,
                "current_price": current_price,
                "ba_now": ba_now,
                "ny_now": ny_now,
                "signal": None,
                "candle_info": None,
                "next_session": next_session
            }

        # Diferencia de tiempo respecto a la apertura (09:30:00 New York)
        diff_seconds = (ny_now - target_dt_ny).total_seconds()

        # Ventana de evaluación: entre el cierre de la vela (60s = 09:31:00) y 300s (09:35:00)
        if 60 <= diff_seconds <= 300:
            if session_key not in self.processed_sessions:
                candle = self.fetch_opening_candle(target_dt_ny)
                if candle and candle["is_closed"]:
                    candle_info = candle
                    cuerpo = candle.get("cuerpo", abs(candle["open"] - candle["close"]))
                    mechasup = candle.get("mechasup", abs(candle["high"] - max(candle["open"], candle["close"])))
                    mechainf = candle.get("mechainf", abs(candle["low"] - min(candle["open"], candle["close"])))

                    logging.info(f"Vela apertura NYSE 1 min analizada -> Open: {candle['open']}, High: {candle['high']}, Low: {candle['low']}, Close: {candle['close']} | cuerpo: {cuerpo:.2f}, mechasup: {mechasup:.2f}, mechainf: {mechainf:.2f}")

                    # 6) entrada en long = si la vela apertura analizada es una vela roja y cuerpo > 30
                    if candle["close"] < candle["open"]:
                        candle["color"] = "roja"
                        if cuerpo > self.min_body:
                            signal = "LONG"
                            logging.info(f"Señal LONG confirmada: Vela ROJA con cuerpo {cuerpo:.2f} > {self.min_body:.0f}.")
                        else:
                            logging.info(f"Sin entrada: Vela ROJA pero cuerpo {cuerpo:.2f} <= {self.min_body:.0f}.")
                    # 6) entrada en short = si la vela apertura analizada es una vela verde y cuerpo > 30
                    elif candle["close"] > candle["open"]:
                        candle["color"] = "verde"
                        if cuerpo > self.min_body:
                            signal = "SHORT"
                            logging.info(f"Señal SHORT confirmada: Vela VERDE con cuerpo {cuerpo:.2f} > {self.min_body:.0f}.")
                        else:
                            logging.info(f"Sin entrada: Vela VERDE pero cuerpo {cuerpo:.2f} <= {self.min_body:.0f}.")
                    else:
                        candle["color"] = "doji"
                        logging.info(f"Vela de apertura NYSE doji neutral ({candle['open']} == {candle['close']}). No se entra.")

                    self.processed_sessions.add(session_key)

        return {
            "strategy_name": self.strategy_name,
            "current_price": current_price,
            "ba_now": ba_now,
            "ny_now": ny_now,
            "signal": signal,
            "candle_info": candle_info,
            "next_session": next_session
        }

    def open_position(self, side, current_price, candle=None):
        """
        1) Modo Aislado, Apalancamiento 30x.
        5) Monto 90% de la cuenta de futuros.
        3) Máximo 1 entrada a la vez.
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

        # Guardar métricas de la vela de entrada para el registro en 2datos.txt
        if candle:
            self.entry_color = candle.get("color", "roja" if side == "LONG" else "verde")
            self.entry_cuerpo = candle.get("cuerpo", 0.0)
            self.entry_mechasup = candle.get("mechasup", 0.0)
            self.entry_mechainf = candle.get("mechainf", 0.0)
        else:
            self.entry_color = "roja" if side == "LONG" else "verde"
            self.entry_cuerpo = 0.0
            self.entry_mechasup = 0.0
            self.entry_mechainf = 0.0

        logging.info(f"EJECUTANDO ENTRADA {side}: Margen ${self.actual_margin_used:.2f} USDT ({self.margin_pct}%) x {self.leverage}x = ${qty * current_price:.2f} USDT ({qty} {self.symbol}) @ ${current_price:.2f} | vela: {self.entry_color}, cuerpo: {self.entry_cuerpo:.2f}, mechasup: {self.entry_mechasup:.2f}, mechainf: {self.entry_mechainf:.2f}")

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
            # Cancelar órdenes previas antes de enviar nueva orden
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
            logging.info(f"Orden MARKET {side} enviada a Binance: OrderID={order.get('orderId')}")

            # Confirmar ejecución real en Binance
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
        6) TP = ROI 5%
           SL = ROI -20%
        """
        if not self.current_position or self.entry_price <= 0:
            return False, None

        # ROI (%) sobre el margen de la posición
        if self.current_position == 'LONG':
            roi_pct = ((current_price - self.entry_price) / self.entry_price) * self.leverage * 100.0
        else:
            roi_pct = ((self.entry_price - current_price) / self.entry_price) * self.leverage * 100.0

        # 6) TP = ROI 5%
        if roi_pct >= self.take_profit_pct:
            return True, f"TP alcanzado (ROI: +{roi_pct:.2f}% >= +{self.take_profit_pct:.2f}%)"

        # 6) SL = ROI -20%
        if roi_pct <= -abs(self.stop_loss_pct):
            return True, f"SL alcanzado (ROI: {roi_pct:.2f}% <= -{abs(self.stop_loss_pct):.2f}%)"

        return False, None

    def close_position(self, current_price, reason="TP alcanzado"):
        """Cierra la posición activa a mercado y registra la operación en 2datos.txt."""
        if not self.current_position:
            return

        side = self.current_position
        logging.info(f"CERRANDO POSICION {side} por {reason} @ ${current_price:.2f}...")

        exit_time = datetime.now()
        dur_secs = (exit_time - self.entry_time).total_seconds() if self.entry_time else 0.0

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

        # Cálculos de resultados:
        if side == 'LONG':
            gross_pnl_usdt = (current_price - self.entry_price) * self.position_qty
        else:
            gross_pnl_usdt = (self.entry_price - current_price) * self.position_qty

        entry_notional = self.entry_price * self.position_qty
        exit_notional = current_price * self.position_qty
        total_fees = (entry_notional + exit_notional) * (self.fee_rate_pct / 100.0)
        actual_margin = self.actual_margin_used if self.actual_margin_used > 0 else (entry_notional / self.leverage)

        roi_pct = (gross_pnl_usdt / actual_margin) * 100.0 if actual_margin > 0 else 0.0
        fees_pct = (total_fees / actual_margin) * 100.0 if actual_margin > 0 else 0.0

        if self.dry_run:
            self.simulated_balance += (gross_pnl_usdt - total_fees)

        # Registrar en 2datos.txt
        self._record_trade_data(
            roi_usdt=gross_pnl_usdt,
            roi_pct=roi_pct,
            total_fees_usdt=total_fees,
            fees_pct=fees_pct,
            max_gain_pct=self.max_gain_pct,
            max_loss_pct=self.max_loss_pct,
            dur_secs=dur_secs,
            exit_time=exit_time,
            pos_side=side,
            color=self.entry_color,
            cuerpo=self.entry_cuerpo,
            mechasup=self.entry_mechasup,
            mechainf=self.entry_mechainf
        )

        # Resetear variables de posición
        self.current_position = None
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.entry_time = None
        self.max_gain_pct = 0.0
        self.max_loss_pct = 0.0
        self.entry_color = ""
        self.entry_cuerpo = 0.0
        self.entry_mechasup = 0.0
        self.entry_mechainf = 0.0

    def _record_trade_data(self, roi_usdt, roi_pct, total_fees_usdt, fees_pct, max_gain_pct, max_loss_pct, dur_secs, exit_time, pos_side="LONG", color="", cuerpo=0.0, mechasup=0.0, mechainf=0.0):
        """
        DETALLES 5: Graba en 2datos.txt con columnas alineadas y cabecera de 2 líneas:
        dia, hora, long/short, duracion operacion, % ganancia maximo, % perdida maximo, ROI, % ROI, comisiones, % comisiones, verde/roja, cuerpo, mechasup, mechainf
        """
        # Determinación de ganada/perdida según ROI
        is_win = (roi_pct >= self.take_profit_pct) or (roi_usdt >= 0)

        if is_win:
            self.winning_trades += 1
            self.money_won += roi_usdt
            roi_str = f"+{roi_usdt:.2f} USDT"
            roi_pct_str = f"+{roi_pct:.2f}%"
        else:
            self.losing_trades += 1
            self.money_lost += abs(roi_usdt)
            roi_str = f"{roi_usdt:.2f} USDT"
            roi_pct_str = f"{roi_pct:.2f}%"

        comisiones_str = f"{total_fees_usdt:.2f} USDT"
        pct_comisiones_str = f"{fees_pct:.2f}%"

        dia_str = exit_time.strftime('%Y-%m-%d')
        hora_str = exit_time.strftime('%H:%M:%S')
        pos_side_str = str(pos_side).upper()

        mins = int(dur_secs // 60)
        secs = int(dur_secs % 60)
        dur_str = f"{mins:02d}m {secs:02d}s"

        gain_max_str = f"+{max_gain_pct:.2f}%"
        loss_max_str = f"{max_loss_pct:.2f}%" if max_loss_pct <= 0 else f"-{max_loss_pct:.2f}%"

        color_str = str(color).lower()
        cuerpo_str = f"{cuerpo:.2f}"
        mechasup_str = f"{mechasup:.2f}"
        mechainf_str = f"{mechainf:.2f}"

        row = [
            dia_str,
            hora_str,
            pos_side_str,
            dur_str,
            gain_max_str,
            loss_max_str,
            roi_str,
            roi_pct_str,
            comisiones_str,
            pct_comisiones_str,
            color_str,
            cuerpo_str,
            mechasup_str,
            mechainf_str
        ]

        widths = [col[2] for col in self.DATA_COLUMNS]
        line = " | ".join(val.ljust(w) for val, w in zip(row, widths)) + "\n"

        filename = "2datos.txt"
        try:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line)
            logging.info(f"Operación registrada en {filename}: {line.strip()}")
        except Exception as e:
            logging.error(f"Error escribiendo en {filename}: {e}")

    def render_screen(self, strat_data, active_pos, entry, roi_pct, dur_secs):
        """
        DETALLES 2, 3 y 4:
        - Mantener cabecera siempre visible en pantalla (líneas 1 a 5).
        - Mantener visible en pantalla unicamente el estado actual (líneas 1 a 4).
        - No utilizar colores en todo el texto visualizado en pantalla.
        - Reposicionar el cursor al inicio de la pantalla (\033[H) antes de actualizar en vez de borrar por completo.

        Formato de cabecera:
           linea 1: titulo
           linea 2: simbolo, modo, apalancamiento, monto
           linea 3: saldo wallet, disponible, pnl no realizado
           linea 4: dias y horarios que se aplica la estrategia
           linea 5: reglas detalladas de la estrategia

        Formato del estado actual para estrategia:
           linea 1: nombre de estrategia
           linea 2: precio
           linea 3: horario incluyendo contador regresivo en formato hs:min:seg
           linea 4: posicion
        """
        # Reposicionar el cursor al inicio de la pantalla sin borrar todo (evita parpadeo)
        sys.stdout.write("\033[H")

        # Balances para la cabecera
        if self.dry_run:
            wallet_bal = self.simulated_balance
            avail_bal = self.simulated_balance
            unrealized = 0.0
        elif self.client and self.api_key and self.api_secret:
            try:
                acc = self.client.futures_account()
                wallet_bal = float(acc.get('totalWalletBalance', 0.0))
                avail_bal = float(acc.get('availableBalance', 0.0))
                unrealized = float(acc.get('totalUnrealizedProfit', 0.0))
            except Exception:
                wallet_bal, avail_bal, unrealized = 0.0, 0.0, 0.0
        else:
            wallet_bal, avail_bal, unrealized = 0.0, 0.0, 0.0

        # Formatear línea de posición (estado actual)
        curr_price = strat_data['current_price']
        if active_pos and entry > 0:
            mins = int(dur_secs // 60)
            secs = int(dur_secs % 60)
            dur_str = f"{mins:02d}m {secs:02d}s"

            # Niveles de precio objetivo por ROI
            if active_pos == 'LONG':
                tp_price = entry * (1.0 + (self.take_profit_pct / (self.leverage * 100.0)))
                sl_price = entry * (1.0 - (self.stop_loss_pct / (self.leverage * 100.0)))
            else:
                tp_price = entry * (1.0 - (self.take_profit_pct / (self.leverage * 100.0)))
                sl_price = entry * (1.0 + (self.stop_loss_pct / (self.leverage * 100.0)))

            pnl_sign = "+" if roi_pct >= 0 else ""
            pos_line = (
                f"{active_pos} | Cantidad: {self.position_qty} {self.symbol} | Entrada: ${entry:.2f} | "
                f"ROI: {pnl_sign}{roi_pct:.2f}% | "
                f"TP: ${tp_price:.2f} (+{self.take_profit_pct:.1f}%) | "
                f"SL: ${sl_price:.2f} (-{self.stop_loss_pct:.1f}%) | "
                f"Duracion: {dur_str}"
            )
        else:
            pos_line = "SIN POSICION"

        # Formatear línea de horario con contador regresivo en formato hs:min:seg (estado actual)
        ba_now = strat_data['ba_now']
        now_str = ba_now.strftime('%Y-%m-%d %H:%M:%S')

        next_sess = strat_data.get('next_session')
        if next_sess:
            rem_secs = max(0, int(next_sess['diff'].total_seconds()))
            h = rem_secs // 3600
            m = (rem_secs % 3600) // 60
            s = rem_secs % 60
            countdown_fmt = f"{h:02d}:{m:02d}:{s:02d}"
            target_str = next_sess['target_dt'].strftime('%Y-%m-%d %H:%M')
            horario_line = f"{now_str} (BA) | Proxima apertura NYSE 09:30 NY ({target_str}): cuenta regresiva {countdown_fmt}"
        else:
            horario_line = f"{now_str} (BA)"

        # Construcción de pantalla monocroma (sin códigos de colores ANSI)
        lines = []

        # 2) El formato de cabecera:
        # linea 1: titulo
        lines.append("BOT DE TRADING AUTOMATICO BINANCE - ESTRATEGIA VELA APERTURA")
        # linea 2: simbolo, modo, apalancamiento, monto
        lines.append(f"simbolo: {self.symbol} | modo: {self.margin_type} | apalancamiento: {self.leverage}x | monto: {self.margin_pct:.0f}% de la cuenta futuros")
        # linea 3: saldo wallet, disponible, pnl no realizado
        lines.append(f"saldo wallet: {wallet_bal:.2f} USDT | disponible: {avail_bal:.2f} USDT | pnl no realizado: {unrealized:.2f} USDT")
        # linea 4: dias y horarios que se aplica la estrategia
        lines.append("dias y horarios: Lunes a Viernes dias habiles de bolsa de New York a las 09:30 hs (horario New York por la mañana)")
        # linea 5: reglas detalladas de la estrategia
        lines.append(f"reglas: Vela apertura 1 min | roja y cuerpo > {self.min_body:.0f} -> LONG | verde y cuerpo > {self.min_body:.0f} -> SHORT | TP: ROI {self.take_profit_pct:.1f}% | SL: ROI -{self.stop_loss_pct:.1f}% | maximo 1 entrada")
        # linea 6: espacio (no linea horizontal)
        lines.append("")

        # 3) El formato del estado actual para estrategia:
        # linea 1: nombre de estrategia
        lines.append(f"nombre de estrategia: {strat_data['strategy_name']}")
        # linea 2: precio
        lines.append(f"precio: ${curr_price:.2f}")
        # linea 3: horario incluyendo contador regresivo en formato hs:min:seg
        lines.append(f"horario: {horario_line}")
        # linea 4: posicion
        lines.append(f"posicion: {pos_line}")
        # linea 5: espacio (no linea horizontal)
        lines.append("")

        # Ajuste de ancho para sobreescribir líneas previas limpiamente sin parpadeos
        output_buffer = "\n".join(l.ljust(114) for l in lines) + "\n\033[J"
        sys.stdout.write(output_buffer)
        sys.stdout.flush()

    def run(self):
        """Bucle principal de ejecución del bot."""
        logging.info("Bucle principal de monitoreo Vela apertura iniciado.")

        # Limpiar la pantalla por única vez al inicio
        os.system('cls' if os.name == 'nt' else 'clear')

        while True:
            try:
                ba_now = datetime.now(TZ_BA)
                curr_price = self.get_latest_price()

                if curr_price == 0.0:
                    time.sleep(self.poll_interval)
                    continue

                # 1. Consultar estado de posición activa
                active_pos, entry, qty = self.get_active_position()

                # Detección de cierre externo de la posición en Binance
                if not self.dry_run and active_pos is None and self.current_position is not None:
                    exit_time = datetime.now()
                    dur_secs = (exit_time - self.entry_time).total_seconds() if self.entry_time else 0.0
                    side = self.current_position
                    gross_pnl = (curr_price - self.entry_price) * self.position_qty if side == 'LONG' else (self.entry_price - curr_price) * self.position_qty
                    entry_notional = self.entry_price * self.position_qty
                    exit_notional = curr_price * self.position_qty
                    total_fees = (entry_notional + exit_notional) * (self.fee_rate_pct / 100.0)
                    actual_margin = self.actual_margin_used if self.actual_margin_used > 0 else (entry_notional / self.leverage)
                    roi_pct = (gross_pnl / actual_margin) * 100.0 if actual_margin > 0 else 0.0
                    fees_pct = (total_fees / actual_margin) * 100.0 if actual_margin > 0 else 0.0

                    self._record_trade_data(
                        roi_usdt=gross_pnl,
                        roi_pct=roi_pct,
                        total_fees_usdt=total_fees,
                        fees_pct=fees_pct,
                        max_gain_pct=self.max_gain_pct,
                        max_loss_pct=self.max_loss_pct,
                        dur_secs=dur_secs,
                        exit_time=exit_time,
                        pos_side=side,
                        color=self.entry_color,
                        cuerpo=self.entry_cuerpo,
                        mechasup=self.entry_mechasup,
                        mechainf=self.entry_mechainf
                    )
                    self.current_position = None
                    self.entry_price = 0.0
                    self.position_qty = 0.0
                    self.entry_time = None
                    self.max_gain_pct = 0.0
                    self.max_loss_pct = 0.0
                    self.entry_color = ""
                    self.entry_cuerpo = 0.0
                    self.entry_mechasup = 0.0
                    self.entry_mechainf = 0.0

                roi_pct = 0.0
                dur_secs = 0.0
                if active_pos and entry > 0:
                    dur_secs = (datetime.now() - self.entry_time).total_seconds() if self.entry_time else 0.0
                    if active_pos == 'LONG':
                        roi_pct = ((curr_price - entry) / entry) * self.leverage * 100.0
                    else:
                        roi_pct = ((entry - curr_price) / entry) * self.leverage * 100.0

                    # Actualizar métricas máximas de la operación
                    if roi_pct > self.max_gain_pct:
                        self.max_gain_pct = roi_pct
                    if roi_pct < self.max_loss_pct:
                        self.max_loss_pct = roi_pct

                # 2. Analizar vela de apertura NYSE 10:30 hs (BA)
                strat_data = self.analyze_strategy(curr_price, ba_now)

                # 3. Renderizar pantalla monocroma con cabecera y estado actual
                self.render_screen(
                    strat_data=strat_data,
                    active_pos=active_pos,
                    entry=entry,
                    roi_pct=roi_pct,
                    dur_secs=dur_secs
                )

                # 4. Lógica de salidas:
                # TP = ROI 5% | SL = ROI -20%
                should_close, close_reason = self.check_exit_condition(curr_price)
                if active_pos and should_close:
                    self.close_position(curr_price, reason=close_reason)
                    active_pos = None

                # 5. Lógica de entradas:
                # 3) Máximo 1 entrada a la vez
                # 6) Vela roja y cuerpo > 30 -> LONG | Vela verde y cuerpo > 30 -> SHORT | Monto 90%
                if active_pos is None and strat_data["signal"]:
                    sig = strat_data["signal"]
                    candle = strat_data.get("candle_info")
                    self.open_position(side=sig, current_price=curr_price, candle=candle)

                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                print("\n[!] Bot detenido por el usuario.")
                logging.info("Bot detenido manualmente por el usuario (KeyboardInterrupt).")
                break
            except Exception as e:
                logging.error(f"Excepción en bucle principal: {e}")
                time.sleep(self.poll_interval)


if __name__ == "__main__":
    bot = BinanceOpeningCandleBot()
    bot.run()
