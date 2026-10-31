#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de trading automatico en Binance.com

Estrategia: Vela apertura

1) Modo Aislado
   Apalancamiento 60x 
   Monto usdt de la cuenta futuros

2) analizar la vela apertura en temporalidad 1 min

3) hacer maximo 2 entradas a la vez, o sea en simultaneo

4) analizar vela apertura de bolsa de new york 10:30 hs (horario buenos aires por la mañana) unicamente los dias que esta abierta la bolsa de new york

5) ganancia bruta = ganancia con comisiones
   ganancia real  = ganancia sin comisiones
   perdida real   = perdida con comisiones

6) entrada(1) en long  = si la vela apertura analizada es una vela roja,  monto 45% de la cuenta futuros
   entrada(1) en short = si la vela apertura analizada es una vela verde, monto 45% de la cuenta futuros

7) TP(1) = 9% de ganancia real
   SL(1) = 30% de perdida real

8) si perdida real > 15% = hacer entrada(2) long/short igual direccion a la entrada abierta actual,  monto 45% de la cuenta futuros
   TP(1) = cambiarlo a valor entrada(1)
   SL(1) = no cambia
   TP(2) = valor entrada(1)
   SL(2) = SL(1)
   
Detalles:

1) El formato del estado actual para estrategia:
   en una linea: nombre de estrategia
   en otra linea: precio
   en otra linea: horario
   en otra linea: posicion

2) Cerrar posiciones abiertas al iniciar bot

3) hacer archivo 2datos.txt donde van las operaciones que se ganaron y las operaciones que se perdieron, con columnas alineadas, con los datos:
   dia, hora, ganada/perdida, duracion de la operacion, % ganancia maximo, % perdida maximo, % ganancia real, %ganancia bruta, %perdida real

4) Mantener cabecera siempre visible en pantalla
   Mantener visible en pantalla unicamente el estado actual
   No utilizar colores en todo el texto visualizado en pantalla
   Reposicionar el cursor al inicio de la pantalla antes de actualizar la visualizacion de datos en vez de borrar la pantalla por completo
   Hacer operaciones con dinero real

5) .envprivado con la configuracion de api de binance 
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

# Zona horaria de Buenos Aires (UTC-3)
TZ_BA = ZoneInfo("America/Argentina/Buenos_Aires")

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
        self.entry1_pct = float(os.getenv("ENTRY1_PCT", "45.0"))
        self.entry2_pct = float(os.getenv("ENTRY2_PCT", "45.0"))
        self.leverage = int(os.getenv("LEVERAGE", "60"))
        self.margin_type = os.getenv("MARGIN_TYPE", "ISOLATED").upper()
        self.strategy_name = os.getenv("STRATEGY_NAME", "Vela apertura")
        self.timeframe = os.getenv("TIMEFRAME", "1m")

        # Reglas de salida y comisiones
        self.tp1_gain_real_pct = float(os.getenv("TP1_GAIN_REAL_PCT", "9.0"))
        self.sl1_loss_real_pct = float(os.getenv("SL1_LOSS_REAL_PCT", "30.0"))
        self.entry2_trigger_loss_real_pct = float(os.getenv("ENTRY2_TRIGGER_LOSS_REAL_PCT", "15.0"))
        self.fee_rate_pct = float(os.getenv("FEE_RATE_PCT", "0.05"))
        self.poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))

        # Modos de ejecución (dinero real por defecto)
        self.dry_run = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Apertura: New York 10:30 hs (Horario Buenos Aires)
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

        # Control de velas analizadas (clave: "YYYY-MM-DD_BOLSA")
        self.processed_sessions = set()

        # Estructura de posiciones (máximo 2 entradas simultáneas)
        # entry_1: {'side': 'LONG'/'SHORT', 'price': float, 'qty': float, 'margin': float, 'time': datetime}
        # entry_2: {'side': 'LONG'/'SHORT', 'price': float, 'qty': float, 'margin': float, 'time': datetime}
        self.entry_1 = None
        self.entry_2 = None
        self.simulated_balance = 50.0

        # Métricas de la operación en curso
        self.trade_start_time = None
        self.max_gain_pct = 0.0
        self.max_loss_pct = 0.0

        # Métricas históricas de la sesión
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0

        # Inicialización de archivos y conexión
        self._init_trade_log_file()
        self._initialize_client()

    def _init_trade_log_file(self):
        """DETALLES 3: Inicializar 2datos.txt con columnas alineadas."""
        filename = "2datos.txt"
        header = (
            f"{'Dia':<10} | {'Hora':<8} | {'Estado':<10} | {'Duracion':<10} | "
            f"{'% Gan Max':<10} | {'% Perd Max':<10} | {'% Gan Real':<11} | {'% Gan Bruta':<11} | {'% Perd Real':<11}\n"
        )
        separator = "-" * 105 + "\n"
        if not os.path.exists(filename) or os.path.getsize(filename) == 0:
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(header)
                    f.write(separator)
            except Exception as e:
                logging.error(f"Error inicializando {filename}: {e}")

    def _initialize_client(self):
        """Inicializa cliente Binance, configura modo AISLADO 60x y cierra posiciones previas."""
        logging.info("Iniciando Bot Binance Vela apertura (New York 10:30 hs)...")
        logging.info(f"Símbolo: {self.symbol} | Margen: {self.margin_type} | Apalancamiento: {self.leverage}x | Entradas: 45% + 45%")

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

            # DETALLES 2: Cerrar posiciones abiertas al iniciar bot
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
        """Configura margen AISLADO (ISOLATED) y apalancamiento 60x en Binance Futures."""
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
        """DETALLES 2: Cerrar posiciones abiertas al iniciar bot."""
        logging.info("DETALLES 2: Verificando y cerrando cualquier posición abierta al iniciar el bot...")
        if self.dry_run:
            self._reset_position_state()
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

            self._reset_position_state()

        except Exception as e:
            logging.error(f"Error al cerrar posiciones existentes: {e}")

    def _reset_position_state(self):
        self.entry_1 = None
        self.entry_2 = None
        self.trade_start_time = None
        self.max_gain_pct = 0.0
        self.max_loss_pct = 0.0

    def get_binance_open_position(self):
        """Consulta la posición neta actualmente abierta en Binance Futures."""
        if self.dry_run:
            if self.entry_1:
                tot_qty = self.entry_1['qty'] + (self.entry_2['qty'] if self.entry_2 else 0.0)
                tot_margin = self.entry_1['margin'] + (self.entry_2['margin'] if self.entry_2 else 0.0)
                avg_entry = (self.entry_1['price'] * self.entry_1['qty'] + (self.entry_2['price'] * self.entry_2['qty'] if self.entry_2 else 0.0)) / tot_qty
                return self.entry_1['side'], avg_entry, tot_qty
            return None, 0.0, 0.0

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
            if self.entry_1:
                tot_qty = self.entry_1['qty'] + (self.entry_2['qty'] if self.entry_2 else 0.0)
                avg_entry = (self.entry_1['price'] * self.entry_1['qty'] + (self.entry_2['price'] * self.entry_2['qty'] if self.entry_2 else 0.0)) / tot_qty
                return self.entry_1['side'], avg_entry, tot_qty
            return None, 0.0, 0.0

    def get_account_balance(self):
        """Consulta el saldo USDT de la cuenta futuros (totalWalletBalance y availableBalance)."""
        if self.dry_run or not self.client or not self.api_key or not self.api_secret:
            return self.simulated_balance, self.simulated_balance

        try:
            acc = self.client.futures_account()
            total_bal = float(acc.get('totalWalletBalance', 0.0))
            avail = float(acc.get('availableBalance', 0.0))
            for asset in acc.get('assets', []):
                if asset.get('asset') == 'USDT':
                    total_bal = float(asset.get('walletBalance', total_bal))
                    avail = float(asset.get('availableBalance', avail))
                    break
            return total_bal, avail
        except Exception as e:
            logging.error(f"Error consultando balance USDT: {e}")
            return 0.0, 0.0

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
        Obtiene la vela de 1 minuto de la apertura (horario Buenos Aires).
        Retorna dict con {open, high, low, close, is_closed} o None.
        """
        try:
            start_ts = int(target_dt_ba.timestamp() * 1000)
            end_ts = start_ts + 60000

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
        - Únicamente los días que está abierta la bolsa de new york
        - Temporalidad: 1 min
        - Entrada(1) en long: vela roja (close < open)
        - Entrada(1) en short: vela verde (close > open)
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

            # Próxima sesión bursátil abierta
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

            # Si hoy la Bolsa de New York no está abierta, no operar
            if not self.is_nyse_open_day(today_date):
                continue

            diff_seconds = (ba_now - target_dt).total_seconds()

            # La vela abre a las 10:30:00 y cierra a las 10:31:00.
            # Evaluamos entre 60 y 300 segundos posteriores a la apertura.
            if 60 <= diff_seconds <= 300:
                if session_key not in self.processed_sessions:
                    candle = self.fetch_opening_candle(target_dt)
                    if candle and candle["is_closed"]:
                        candle_info = candle
                        if candle["close"] < candle["open"]:
                            signal = "LONG"
                            signal_bolsa = s["bolsa"]
                        elif candle["close"] > candle["open"]:
                            signal = "SHORT"
                            signal_bolsa = s["bolsa"]
                        else:
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

    def execute_entry(self, entry_num, side, current_price):
        """
        Ejecuta entrada 1 o entrada 2:
        - Modo Aislado
        - Apalancamiento 60x
        - Monto: 45% de la cuenta futuros
        """
        total_bal, avail_bal = self.get_account_balance()
        # Monto 45% de la cuenta futuros
        base_bal = total_bal if total_bal > 0 else avail_bal
        target_margin = base_bal * 0.45
        target_margin = min(target_margin, avail_bal * 0.98)

        notional = target_margin * self.leverage
        qty = self._format_quantity(notional / current_price)
        actual_margin = (qty * current_price) / self.leverage

        if qty < self.min_qty or actual_margin <= 0:
            err_msg = f"Margen insuficiente para entrada({entry_num}) (Disp: ${avail_bal:.2f} USDT, Mínimo requerido: {self.min_qty} {self.symbol})"
            logging.error(err_msg)
            self.last_execution_error = err_msg
            return False

        logging.info(f"EJECUTANDO ENTRADA({entry_num}) {side}: Margen ${actual_margin:.2f} USDT (45%) x {self.leverage}x = ${qty * current_price:.2f} USDT ({qty} {self.symbol}) @ ${current_price:.2f}")

        now = datetime.now()
        entry_data = {
            'side': side,
            'price': current_price,
            'qty': qty,
            'margin': actual_margin,
            'time': now
        }

        if self.dry_run:
            if entry_num == 1:
                self.entry_1 = entry_data
                self.entry_2 = None
                self.trade_start_time = now
                self.max_gain_pct = 0.0
                self.max_loss_pct = 0.0
            elif entry_num == 2:
                self.entry_2 = entry_data
            self.last_execution_error = None
            return True

        try:
            order_side = 'BUY' if side == 'LONG' else 'SELL'
            order = self.client.futures_create_order(
                symbol=self.symbol,
                side=order_side,
                type='MARKET',
                quantity=qty
            )
            logging.info(f"Orden entrada({entry_num}) enviada: OrderID={order.get('orderId')}")

            time.sleep(1)
            # Consultar precio y posición real
            active_side, real_entry, real_qty = self.get_binance_open_position()
            if entry_num == 1:
                if real_entry > 0:
                    entry_data['price'] = real_entry
                    entry_data['qty'] = real_qty
                    entry_data['margin'] = (real_qty * real_entry) / self.leverage
                self.entry_1 = entry_data
                self.entry_2 = None
                self.trade_start_time = now
                self.max_gain_pct = 0.0
                self.max_loss_pct = 0.0
            elif entry_num == 2:
                # Para la entrada 2, calculamos el precio y cantidad incremental agregada
                if self.entry_1 and real_qty > self.entry_1['qty']:
                    delta_qty = self._format_quantity(real_qty - self.entry_1['qty'])
                    entry_data['qty'] = delta_qty
                    entry_data['margin'] = (delta_qty * current_price) / self.leverage
                self.entry_2 = entry_data

            self.last_execution_error = None
            return True

        except Exception as e:
            self.last_execution_error = f"Error Binance entrada({entry_num}): {e}"
            logging.error(f"Error abriendo entrada({entry_num}): {e}")
            return False

    def calculate_pnl(self, current_price):
        """
        Cálculo riguroso de PnL según especificación:
        - ganancia bruta = ganancia con comisiones (antes de descontar comisiones)
        - ganancia real  = ganancia sin comisiones (ganancia pura del movimiento del precio, libre de comisiones)
        - perdida real   = perdida con comisiones (perdida del movimiento + costo de comisiones)

        Retorna diccionario con métricas en % sobre el margen y en USDT para la posición completa.
        """
        if not self.entry_1:
            return None

        entries = [self.entry_1]
        if self.entry_2:
            entries.append(self.entry_2)

        tot_margin = sum(e['margin'] for e in entries)
        tot_qty = sum(e['qty'] for e in entries)
        if tot_margin <= 0 or tot_qty <= 0:
            return None

        side = self.entry_1['side']

        # PnL puro de movimiento (Price Delta USDT)
        price_delta_usdt = 0.0
        for e in entries:
            if side == 'LONG':
                price_delta_usdt += (current_price - e['price']) * e['qty']
            else:
                price_delta_usdt += (e['price'] - current_price) * e['qty']

        # Comisión acumulada estimada (apertura + cierre)
        # Tasa de comision * notional
        open_notional = sum(e['price'] * e['qty'] for e in entries)
        close_notional = current_price * tot_qty
        total_fees_usdt = (open_notional + close_notional) * (self.fee_rate_pct / 100.0)

        # Reglas:
        # Ganancia bruta: antes de comisiones (price_delta)
        # Ganancia real: ganancia sin comisiones (price_delta - fees)
        # Perdida real: perdida con comisiones (|price_delta| + fees si está en contra)
        gross_pnl_usdt = price_delta_usdt
        net_pnl_usdt = price_delta_usdt - total_fees_usdt

        # Porcentajes sobre el margen invertido
        gross_pnl_pct = (gross_pnl_usdt / tot_margin) * 100.0
        fee_pct = (total_fees_usdt / tot_margin) * 100.0

        gain_gross_pct = max(0.0, gross_pnl_pct)
        gain_real_pct = max(0.0, gross_pnl_pct - fee_pct)

        if gross_pnl_pct < 0:
            loss_real_pct = abs(gross_pnl_pct) + fee_pct
        else:
            loss_real_pct = max(0.0, fee_pct - gross_pnl_pct) if (gross_pnl_pct - fee_pct) < 0 else 0.0

        # ROE general (neto)
        net_roe_pct = gross_pnl_pct - fee_pct

        return {
            'side': side,
            'tot_margin': tot_margin,
            'tot_qty': tot_qty,
            'gross_pnl_usdt': gross_pnl_usdt,
            'net_pnl_usdt': net_pnl_usdt,
            'fees_usdt': total_fees_usdt,
            'gain_gross_pct': gain_gross_pct,
            'gain_real_pct': gain_real_pct,
            'loss_real_pct': loss_real_pct,
            'net_roe_pct': net_roe_pct
        }

    def calculate_single_entry_loss_real(self, entry_dict, current_price):
        """Calcula la pérdida real (% con comisiones) de una entrada individual."""
        side = entry_dict['side']
        margin = entry_dict['margin']
        qty = entry_dict['qty']
        price = entry_dict['price']

        if side == 'LONG':
            delta_usdt = (current_price - price) * qty
        else:
            delta_usdt = (price - current_price) * qty

        fees_usdt = (price * qty + current_price * qty) * (self.fee_rate_pct / 100.0)
        net_usdt = delta_usdt - fees_usdt

        if net_usdt < 0:
            return (abs(net_usdt) / margin) * 100.0
        return 0.0

    def check_position_rules(self, current_price):
        """
        Evalúa reglas 7 y 8:
        Si solo entrada(1) activa:
          - TP(1) = 9% de ganancia real
          - SL(1) = 30% de perdida real
          - Si perdida real > 15% -> ejecutar entrada(2) mismo sentido (monto 45%)
            Al activarse entrada(2):
            TP(1) = valor entrada(1)
            SL(1) = no cambia (30% de pérdida real de entrada 1)
            TP(2) = valor entrada(1)
            SL(2) = SL(1)

        Si ambas entradas activas:
          - TP: cuando el precio alcance el valor de entrada(1)
            (En Long: current_price >= entry_1['price']; En Short: current_price <= entry_1['price'])
          - SL: cuando la pérdida real alcance el SL(1) correspondiente.
        """
        if not self.entry_1:
            return None, None

        pnl = self.calculate_pnl(current_price)
        if not pnl:
            return None, None

        side = self.entry_1['side']

        # Caso A: Solo entrada 1 activa
        if self.entry_2 is None:
            # 1. Chequear TP(1) = 9% ganancia real
            if pnl['gain_real_pct'] >= self.tp1_gain_real_pct:
                return "CLOSE", f"TP(1) alcanzado: Ganancia real {pnl['gain_real_pct']:.2f}% >= {self.tp1_gain_real_pct:.1f}%"

            # 2. Chequear si perdida real > 15% para abrir entrada 2
            if pnl['loss_real_pct'] > self.entry2_trigger_loss_real_pct:
                # Comprobar primero que no haya saltado SL(1) directamente
                if pnl['loss_real_pct'] >= self.sl1_loss_real_pct:
                    return "CLOSE", f"SL(1) alcanzado: Perdida real {pnl['loss_real_pct']:.2f}% >= {self.sl1_loss_real_pct:.1f}%"
                return "OPEN_ENTRY_2", f"Perdida real {pnl['loss_real_pct']:.2f}% > 15% -> Gatillar entrada(2)"

            # 3. Chequear SL(1) = 30% perdida real
            if pnl['loss_real_pct'] >= self.sl1_loss_real_pct:
                return "CLOSE", f"SL(1) alcanzado: Perdida real {pnl['loss_real_pct']:.2f}% >= {self.sl1_loss_real_pct:.1f}%"

        # Caso B: Ambas entradas activas
        else:
            target_entry1_price = self.entry_1['price']

            # TP conjunto: valor de entrada(1)
            if side == 'LONG' and current_price >= target_entry1_price:
                return "CLOSE", f"TP(1 & 2) alcanzado: Precio ${current_price:.2f} >= Entrada(1) ${target_entry1_price:.2f}"
            elif side == 'SHORT' and current_price <= target_entry1_price:
                return "CLOSE", f"TP(1 & 2) alcanzado: Precio ${current_price:.2f} <= Entrada(1) ${target_entry1_price:.2f}"

            # SL conjunto: SL(2) = SL(1) = 30% de perdida real sobre el trade
            entry1_loss = self.calculate_single_entry_loss_real(self.entry_1, current_price)
            if entry1_loss >= self.sl1_loss_real_pct or pnl['loss_real_pct'] >= self.sl1_loss_real_pct:
                return "CLOSE", f"SL(1 & 2) alcanzado: Perdida real {pnl['loss_real_pct']:.2f}% >= SL {self.sl1_loss_real_pct:.1f}%"

        return None, None

    def close_all_positions(self, current_price, reason="Salida de posicion"):
        """Cierra la totalidad de la posición en Binance y registra métricas en 2datos.txt."""
        if not self.entry_1:
            return

        pnl = self.calculate_pnl(current_price)
        side = self.entry_1['side']
        tot_qty = self.entry_1['qty'] + (self.entry_2['qty'] if self.entry_2 else 0.0)

        logging.info(f"CERRANDO POSICION TOTAL {side} ({tot_qty} {self.symbol}) por {reason} @ ${current_price:.2f}...")

        exit_time = datetime.now()
        dur_mins = (exit_time - self.trade_start_time).total_seconds() / 60.0 if self.trade_start_time else 0.0

        if not self.dry_run and self.client:
            try:
                side_to_close = 'SELL' if side == 'LONG' else 'BUY'
                qty = self._format_quantity(tot_qty)
                self.client.futures_create_order(
                    symbol=self.symbol,
                    side=side_to_close,
                    type='MARKET',
                    quantity=qty,
                    reduceOnly=True
                )
                logging.info(f"Orden MARKET de cierre total ({qty} {self.symbol}) ejecutada con éxito.")
            except Exception as e:
                logging.error(f"Error enviando orden de cierre a Binance: {e}")

        # PnL y registro en archivo
        net_usdt = pnl['net_pnl_usdt'] if pnl else 0.0
        gain_real = pnl['gain_real_pct'] if pnl else 0.0
        gain_gross = pnl['gain_gross_pct'] if pnl else 0.0
        loss_real = pnl['loss_real_pct'] if pnl else 0.0

        if self.dry_run:
            self.simulated_balance += net_usdt

        # Registrar en 2datos.txt
        self._record_trade_in_2datos(
            exit_time=exit_time,
            dur_mins=dur_mins,
            net_usdt=net_usdt,
            gain_real_pct=gain_real,
            gain_gross_pct=gain_gross,
            loss_real_pct=loss_real
        )

        # Resetear estado
        self._reset_position_state()

    def _record_trade_in_2datos(self, exit_time, dur_mins, net_usdt, gain_real_pct, gain_gross_pct, loss_real_pct):
        """
        DETALLES 3:
        hacer archivo 2datos.txt donde van las operaciones que se ganaron y las operaciones que se perdieron,
        con columnas alineadas, con los datos:
        dia, hora, ganada/perdida, duracion de la operacion, % ganancia maximo, % perdida maximo, % ganancia real, %ganancia bruta, %perdida real
        """
        filename = "2datos.txt"
        is_won = net_usdt > 0
        if is_won:
            self.winning_trades += 1
            self.money_won += net_usdt
            estado_str = "GANADA"
        else:
            self.losing_trades += 1
            self.money_lost += abs(net_usdt)
            estado_str = "PERDIDA"

        dia_str = exit_time.strftime('%Y-%m-%d')
        hora_str = exit_time.strftime('%H:%M:%S')
        dur_str = f"{dur_mins:.1f} min"
        max_gain_str = f"+{self.max_gain_pct:.2f}%"
        max_loss_str = f"-{self.max_loss_pct:.2f}%"
        gain_real_str = f"+{gain_real_pct:.2f}%" if gain_real_pct > 0 else "0.00%"
        gain_gross_str = f"+{gain_gross_pct:.2f}%" if gain_gross_pct > 0 else "0.00%"
        loss_real_str = f"-{loss_real_pct:.2f}%" if loss_real_pct > 0 else "0.00%"

        line = (
            f"{dia_str:<10} | {hora_str:<8} | {estado_str:<10} | {dur_str:<10} | "
            f"{max_gain_str:<10} | {max_loss_str:<10} | {gain_real_str:<11} | {gain_gross_str:<11} | {loss_real_str:<11}\n"
        )

        try:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line)
            logging.info(f"Operación registrada en {filename}: {line.strip()}")
        except Exception as e:
            logging.error(f"Error escribiendo en {filename}: {e}")

    def render_screen(self, strat_data, pnl, dur_mins):
        """
        DETALLES 1 & 4:
        - El formato del estado actual para estrategia:
          en una linea: nombre de estrategia
          en otra linea: precio
          en otra linea: horario
          en otra linea: posicion
        - Mantener cabecera siempre visible en pantalla.
        - Mantener visible en pantalla unicamente el estado actual.
        - No utilizar colores en todo el texto visualizado en pantalla.
        - Reposicionar el cursor al inicio de la pantalla (\033[H) antes de actualizar.
        - Hacer operaciones con dinero real.
        """
        # Reposicionar el cursor al inicio de la pantalla
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
        curr_price = strat_data['current_price']

        # Formatear línea de posicion
        if self.entry_1:
            side = self.entry_1['side']
            dur_str = f" ({dur_mins:.1f} min)"
            num_entries = 2 if self.entry_2 else 1
            tot_qty = self.entry_1['qty'] + (self.entry_2['qty'] if self.entry_2 else 0.0)

            if pnl:
                roe_sign = "+" if pnl['net_roe_pct'] >= 0 else ""
                pnl_str = f"ROE Neto: {roe_sign}{pnl['net_roe_pct']:.2f}% | Gan Real: +{pnl['gain_real_pct']:.2f}% | Gan Bruta: +{pnl['gain_gross_pct']:.2f}% | Perd Real: -{pnl['loss_real_pct']:.2f}%"
            else:
                pnl_str = ""

            if num_entries == 1:
                e1_price = self.entry_1['price']
                tp_val = e1_price * (1.0 + (self.tp1_gain_real_pct / (self.leverage * 100.0))) if side == 'LONG' else e1_price * (1.0 - (self.tp1_gain_real_pct / (self.leverage * 100.0)))
                sl_val = e1_price * (1.0 - (self.sl1_loss_real_pct / (self.leverage * 100.0))) if side == 'LONG' else e1_price * (1.0 + (self.sl1_loss_real_pct / (self.leverage * 100.0)))
                target_str = f" | TP(1): ${tp_val:.2f} (+{self.tp1_gain_real_pct:.1f}% real) | SL(1): ${sl_val:.2f} (-{self.sl1_loss_real_pct:.1f}% real)"
                pos_line = f"{side} (Entrada 1/2) @ ${e1_price:.2f} [{tot_qty} {self.symbol}]{dur_str} | {pnl_str}{target_str}"
            else:
                e1_p = self.entry_1['price']
                e2_p = self.entry_2['price']
                avg_p = (e1_p * self.entry_1['qty'] + e2_p * self.entry_2['qty']) / tot_qty
                target_str = f" | TP(1&2): ${e1_p:.2f} (Valor Entrada 1) | SL(1&2): -{self.sl1_loss_real_pct:.1f}% real"
                pos_line = f"{side} (Entrada 1+2 Simultaneas) Avg @ ${avg_p:.2f} [E1: ${e1_p:.2f}, E2: ${e2_p:.2f} | {tot_qty} {self.symbol}]{dur_str} | {pnl_str}{target_str}"
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
        lines.append(f"Simbolo: {self.symbol} | Modo: {self.margin_type} | Apalancamiento: {self.leverage}x | Entradas: 45% + 45% (Max 2 simultaneas)")
        lines.append(f"Modo de Ejecucion: {'SIMULACION (DRY-RUN)' if self.dry_run else 'DINERO REAL (Binance Futures)'}")
        lines.append("Apertura: New York 10:30 hs (Horario Buenos Aires) - Solo dias habiles bursatiles")
        lines.append(f"Reglas: Vela roja -> LONG | Vela verde -> SHORT | TP1: +{self.tp1_gain_real_pct:.1f}% real | SL1: -{self.sl1_loss_real_pct:.1f}% real")
        lines.append(f"Regla E2: Si perdida real > {self.entry2_trigger_loss_real_pct:.1f}% -> Entrada 2 (45%) | TP1&2 = Valor Entrada 1 | SL2 = SL1")
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
        """Bucle principal de monitoreo y ejecución del bot."""
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

                # 1. Comprobar posición externa en Binance si no estamos en dry_run
                if not self.dry_run and self.client and self.api_key and self.api_secret:
                    active_side, real_entry, real_qty = self.get_binance_open_position()
                    if active_side is None and self.entry_1 is not None:
                        # La posición se cerró externamente en Binance (por ej. liquidación o cierre manual)
                        pnl = self.calculate_pnl(curr_price)
                        exit_time = datetime.now()
                        dur_mins = (exit_time - self.trade_start_time).total_seconds() / 60.0 if self.trade_start_time else 0.0
                        net_usdt = pnl['net_pnl_usdt'] if pnl else 0.0
                        gain_real = pnl['gain_real_pct'] if pnl else 0.0
                        gain_gross = pnl['gain_gross_pct'] if pnl else 0.0
                        loss_real = pnl['loss_real_pct'] if pnl else 0.0
                        self._record_trade_in_2datos(exit_time, dur_mins, net_usdt, gain_real, gain_gross, loss_real)
                        self._reset_position_state()

                # 2. Calcular PnL y actualizar métricas de ganancia/pérdida máxima
                pnl = None
                dur_mins = 0.0
                if self.entry_1:
                    dur_mins = (datetime.now() - self.trade_start_time).total_seconds() / 60.0 if self.trade_start_time else 0.0
                    pnl = self.calculate_pnl(curr_price)
                    if pnl:
                        if pnl['gain_real_pct'] > self.max_gain_pct:
                            self.max_gain_pct = pnl['gain_real_pct']
                        if pnl['loss_real_pct'] > self.max_loss_pct:
                            self.max_loss_pct = pnl['loss_real_pct']

                # 3. Analizar vela de apertura New York 10:30 hs (BA)
                strat_data = self.analyze_strategy(curr_price, ba_now)

                # 4. Renderizar pantalla monocroma con cabecera y estado actual
                self.render_screen(
                    strat_data=strat_data,
                    pnl=pnl,
                    dur_mins=dur_mins
                )

                # 5. Evaluar reglas de gestión y salida si hay posición activa
                if self.entry_1:
                    action, reason = self.check_position_rules(curr_price)
                    if action == "CLOSE":
                        self.close_all_positions(curr_price, reason=reason)
                    elif action == "OPEN_ENTRY_2":
                        # Ejecutar entrada(2): igual dirección, monto 45%
                        logging.info(f"Disparando entrada(2): {reason}")
                        success = self.execute_entry(entry_num=2, side=self.entry_1['side'], current_price=curr_price)
                        if success:
                            logging.info(f"Entrada(2) abierta exitosamente. TP actualizado a valor de Entrada(1): ${self.entry_1['price']:.2f}")

                # 6. Evaluar apertura de entrada(1) si no hay posición abierta y hay señal
                elif strat_data["signal"]:
                    sig = strat_data["signal"]
                    logging.info(f"Señal de apertura detectada: {sig} en {strat_data['signal_bolsa']}. Ejecutando Entrada(1)...")
                    self.execute_entry(entry_num=1, side=sig, current_price=curr_price)

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
