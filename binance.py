#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de Trading Automático en Binance Futures (USDT-M)
Estrategia: Apertura de Mercado (Euronext, Tokio, New York)

Parámetros:
- Modo: Aislado (Isolated)
- Apalancamiento: 10x
- Monto por Operación: 5 USDT
- Velas de Apertura de 1 min:
  1) Bolsa New York: 10:30 hs (Horario Buenos Aires UTC-3)
  2) Bolsa Tokio: 21:00 hs (Horario Buenos Aires UTC-3)
  3) Bolsa Euronext: 04:00 hs (Horario Buenos Aires UTC-3)
- Tendencia en Velas de 5 min (EMA 20 vs EMA 50).
- Impulso a favor de tendencia -> Retroceso Fibonacci en 5m.
- Retroceso contra tendencia -> Extensión Fibonacci en 5m.
- Regla LONG: Vela de apertura ROJA -> Entrada en 1ra línea Fibonacci, TP en 1ra línea Fibonacci, SL en 3ra línea Fibonacci.
- Regla SHORT: Vela de apertura VERDE -> Entrada en 1ra línea Fibonacci, TP en 1ra línea Fibonacci, SL en 3ra línea Fibonacci.
- Operar únicamente en horario de bolsa de EEUU, Tokio y Euronext.
- Visualización: Monocroma (sin colores ANSI), cabecera visible, borrar pantalla antes de actualizar.
- Registros: tp.txt y sl.txt con columnas alineadas (día, hora, bolsa, % ganancia máx, % pérdida máx, duración).
"""

import os
import sys
import time
import math
import logging
from datetime import datetime, timezone, timedelta, time as dtime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# 1. Configuración de encoding para consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# 2. Configuración de Logging a bot.log (para mantener consola de usuario limpia)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)

# 3. Cargar variables de entorno (.envprivado para API Keys, .envpublico para parámetros)
for env_priv in [".envprivado", "envprivado", "envprivado.env"]:
    if os.path.exists(env_priv):
        load_dotenv(env_priv)

for env_pub in [".envpublico", "envpublico", ".env", "env"]:
    if os.path.exists(env_pub):
        load_dotenv(env_pub)

# 4. Importar biblioteca python-binance evitando sombreado local por el archivo binance.py
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


class BinanceAperturaBot:
    def __init__(self):
        # Cargar credenciales API
        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        # Parámetros del Bot
        self.symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
        self.margin_usdt = float(os.getenv("MARGIN_USDT", "5.0"))
        self.leverage = int(os.getenv("LEVERAGE", "10"))

        # Modos de Ejecución
        self.dry_run = os.getenv("DRY_RUN", "True").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Filtro Horario
        self.enable_schedule = os.getenv("ENABLE_SCHEDULE_FILTER", "True").lower() in ("true", "1", "yes")
        self.schedule_weekdays_only = os.getenv("SCHEDULE_WEEKDAYS_ONLY", "True").lower() in ("true", "1", "yes")

        # Cliente Binance y reglas de mercado
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001

        # Estado de la Posición Activa
        self.current_position = None  # None, 'LONG', 'SHORT'
        self.active_bolsa = "NINGUNA"  # 'EURONEXT', 'TOKYO', 'NY'
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.tp_price = 0.0
        self.sl_price = 0.0
        self.entry_time = None
        self.simulated_balance = 100.0

        # Métricas de Operación
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0
        self.max_pnl_pct = 0.0
        self.min_pnl_pct = 0.0

        # Inicialización
        self._initialize_client()
        self._init_trade_log_files()

    def _init_trade_log_files(self):
        """DETALLES 2: Crear/Inicializar archivos tp.txt y sl.txt con columnas alineadas si no existen o están vacíos."""
        header = "Dia        | Hora     | Bolsa    | % Ganancia Max | % Perdida Max | Duracion  \n--------------------------------------------------------------------------------\n"
        for filename in ["tp.txt", "sl.txt"]:
            if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                try:
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(header)
                except Exception as e:
                    logging.error(f"Error inicializando {filename}: {e}")

    def _initialize_client(self):
        """Inicializa cliente Binance, valida API keys y configura entorno."""
        logging.info("Inicializando Bot de Trading Binance - Estrategia Apertura...")
        logging.info(f"Símbolo: {self.symbol} | Margen: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt} USDT")

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
                logging.info("Conexión autenticada exitosamente a Binance Futures API.")
            else:
                logging.info("Modo de simulación (DRY-RUN) activo.")

            # DETALLES 1: Cerrar posiciones abiertas al iniciar bot
            self.close_existing_positions()

        except Exception as e:
            logging.error(f"Error al inicializar cliente Binance: {e}")
            if not self.dry_run:
                logging.info("Cambiando automáticamente a modo DRY-RUN por error de conexión.")
                self.dry_run = True

    def _update_symbol_precision(self):
        """Obtiene precisión de precio y cantidad para el símbolo."""
        try:
            info = self.client.futures_exchange_info()
            for s in info['symbols']:
                if s['symbol'] == self.symbol:
                    for f in s['filters']:
                        if f['filterType'] == 'PRICE_FILTER':
                            self.tick_size = float(f['tickSize'])
                            self.price_precision = self._precision_from_step(f['tickSize'])
                        elif f['filterType'] == 'LOT_SIZE':
                            self.step_size = float(f['stepSize'])
                            self.min_qty = float(f['minQty'])
                            self.qty_precision = self._precision_from_step(f['stepSize'])
                    break
        except Exception as e:
            logging.warning(f"No se pudieron obtener precisiones dinámicas ({e}). Usando por defecto.")

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
        """Configura margen AISLADO y apalancamiento 10x."""
        try:
            try:
                self.client.futures_change_margin_type(symbol=self.symbol, marginType='ISOLATED')
                logging.info(f"Margen configurado a ISOLATED para {self.symbol}.")
            except BinanceAPIException as e:
                if e.code != -4046 and "No need to change" not in str(e):
                    logging.warning(f"Nota sobre margen aislado: {e.message}")

            self.client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            logging.info(f"Apalancamiento configurado a {self.leverage}x para {self.symbol}.")
        except Exception as e:
            logging.error(f"Error configurando cuenta de futuros: {e}")

    def close_existing_positions(self):
        """DETALLES 1: Cerrar posiciones abiertas al iniciar bot y cancelar órdenes previas."""
        logging.info("DETALLES 1: Verificando y cerrando posiciones abiertas al iniciar bot...")
        if self.dry_run:
            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            logging.info("Modo Simulación: Posición inicial limpia.")
            return

        if not self.client or not self.api_key or not self.api_secret:
            return

        try:
            # Cancelar órdenes limit y algo previas
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            except Exception as e:
                logging.warning(f"Nota al cancelar órdenes abiertas previas: {e}")

            try:
                self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": self.symbol})
            except Exception as e:
                logging.warning(f"Nota al cancelar órdenes algo previas: {e}")

            # Buscar y cerrar posiciones de mercado activas
            positions = self.client.futures_position_information(symbol=self.symbol)
            closed_any = False
            for pos in positions:
                amt = float(pos['positionAmt'])
                if amt != 0:
                    side_to_close = 'SELL' if amt > 0 else 'BUY'
                    qty = self._format_quantity(abs(amt))
                    pos_type = 'LONG' if amt > 0 else 'SHORT'
                    logging.info(f"Posición previa detectada ({pos_type} {qty} {self.symbol}). Cerrando a MARKET...")
                    
                    self.client.futures_create_order(
                        symbol=self.symbol,
                        side=side_to_close,
                        type='MARKET',
                        quantity=qty,
                        reduceOnly=True
                    )
                    closed_any = True
                    logging.info(f"Posición {pos_type} previa cerrada correctamente.")

            if not closed_any:
                logging.info(f"Sin posiciones abiertas previas para {self.symbol}.")

            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0

        except Exception as e:
            logging.error(f"Error al cerrar posiciones abiertas iniciales: {e}")

    def fetch_klines(self, timeframe='1m', limit=300):
        """Obtiene klines OHLCV desde Binance Futures."""
        try:
            klines = self.client.futures_klines(symbol=self.symbol, interval=timeframe, limit=limit)
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            return df
        except Exception as e:
            logging.error(f"Error al obtener klines ({timeframe}): {e}")
            return None

    def is_market_open_and_session(self):
        """
        Punto 9: Operar únicamente en horario de bolsa de EEUU, Tokio y Euronext (Buenos Aires UTC-3).
        1) Euronext: 04:00 a 12:30 hs ART
        2) Tokio: 21:00 a 03:00 hs ART
        3) NY: 10:30 a 17:00 hs ART
        """
        if not self.enable_schedule:
            return True, "TODAS (Filtro Desactivado)"

        ba_tz = timezone(timedelta(hours=-3))
        now_ba = datetime.now(ba_tz)

        if self.schedule_weekdays_only and now_ba.weekday() >= 5:
            return False, "CERRADO (Fin de Semana)"

        t_now = now_ba.time()

        # Chequear sesiones
        in_euronext = dtime(4, 0) <= t_now < dtime(12, 30)
        in_tokyo = t_now >= dtime(21, 0) or t_now < dtime(3, 0)
        in_ny = dtime(10, 30) <= t_now < dtime(17, 0)

        active_sessions = []
        if in_euronext:
            active_sessions.append("Euronext [04:00-12:30]")
        if in_tokyo:
            active_sessions.append("Tokio [21:00-03:00]")
        if in_ny:
            active_sessions.append("NY [10:30-17:00]")

        if active_sessions:
            return True, " / ".join(active_sessions)
        else:
            return False, "CERRADO (Fuera de Horario de Bolsas)"

    def analyze_strategy_apertura(self):
        """
        Análisis de la Estrategia de Apertura:
        1) Vela de 1 min de apertura Euronext (04:00 hs), Tokio (21:00 hs) y NY (10:30 hs) ART.
        2) Tendencia en velas de 5 min (EMA 20 vs EMA 50).
        3) Impulso a favor de tendencia -> Retroceso Fibonacci en 5m.
        4) Retroceso contra tendencia -> Extensión Fibonacci en 5m.
        5) LONG si vela de apertura ROJA -> Entrada en 1ra línea Fib, TP 1ra línea Fib, SL 3ra línea Fib.
        6) SHORT si vela de apertura VERDE -> Entrada en 1ra línea Fib, TP 1ra línea Fib, SL 3ra línea Fib.
        """
        ba_tz = timezone(timedelta(hours=-3))
        now_ba = datetime.now(ba_tz)

        df_1m = self.fetch_klines(timeframe='1m', limit=500)
        df_5m = self.fetch_klines(timeframe='5m', limit=100)

        default_result = {
            'bolsa_nombre': 'NINGUNA',
            'vela_apertura_str': 'ESPERANDO APERTURA',
            'vela_color': 'NINGUNA',
            'trend': 'NEUTRAL',
            'mov_tipo': 'IMPULSO',
            'signal': None,
            'fib_s1': 0.0,
            'fib_s3': 0.0,
            'fib_r1': 0.0,
            'fib_r3': 0.0,
            'current_price': 0.0
        }

        if df_1m is None or df_5m is None or len(df_1m) == 0 or len(df_5m) == 0:
            return default_result

        curr_price = df_1m['close'].iloc[-1]
        default_result['current_price'] = curr_price

        # Convertir timestamps a Buenos Aires (UTC-3)
        df_1m['timestamp_ba'] = df_1m['timestamp'].dt.tz_localize('UTC').dt.tz_convert(ba_tz)
        df_5m['timestamp_ba'] = df_5m['timestamp'].dt.tz_localize('UTC').dt.tz_convert(ba_tz)

        # 1. Buscar velas de 1m de apertura de mercado (Euronext 04:00, Tokio 21:00, NY 10:30)
        openings = [
            {'bolsa': 'NY', 'hour': 10, 'minute': 30, 'label': 'NY (10:30 1m)'},
            {'bolsa': 'Tokio', 'hour': 21, 'minute': 0, 'label': 'Tokio (21:00 1m)'},
            {'bolsa': 'Euronext', 'hour': 4, 'minute': 0, 'label': 'Euronext (04:00 1m)'}
        ]

        # Priorizar sesión activa según la hora actual
        t_now = now_ba.time()
        if dtime(10, 30) <= t_now < dtime(17, 0):
            openings.sort(key=lambda x: 0 if x['bolsa'] == 'NY' else 1)
        elif t_now >= dtime(21, 0) or t_now < dtime(3, 0):
            openings.sort(key=lambda x: 0 if x['bolsa'] == 'Tokio' else 1)
        elif dtime(4, 0) <= t_now < dtime(12, 30):
            openings.sort(key=lambda x: 0 if x['bolsa'] == 'Euronext' else 1)

        active_open_candle = None
        selected_bolsa = "NINGUNA"
        selected_label = ""

        for op in openings:
            candles = df_1m[
                (df_1m['timestamp_ba'].dt.hour == op['hour']) &
                (df_1m['timestamp_ba'].dt.minute == op['minute'])
            ]
            if len(candles) > 0:
                active_open_candle = candles.iloc[-1]
                selected_bolsa = op['bolsa']
                selected_label = op['label']
                break

        if active_open_candle is not None:
            c_open = active_open_candle['open']
            c_close = active_open_candle['close']
            if c_close < c_open:
                vela_color = "ROJA"
                vela_apertura_str = f"ROJA [{selected_label}]"
            else:
                vela_color = "VERDE"
                vela_apertura_str = f"VERDE [{selected_label}]"
        else:
            vela_color = "NINGUNA"
            vela_apertura_str = "ESPERANDO APERTURA (04:00, 10:30, 21:00 ART)"
            selected_bolsa = "GENERAL"

        # 2. Tendencia para velas de 5 min (EMA 20 vs EMA 50)
        df_5m['ema20'] = df_5m['close'].ewm(span=20, adjust=False).mean()
        df_5m['ema50'] = df_5m['close'].ewm(span=50, adjust=False).mean()
        last_ema20 = df_5m['ema20'].iloc[-1]
        last_ema50 = df_5m['ema50'].iloc[-1]
        trend = "ALCISTA" if last_ema20 >= last_ema50 else "BAJISTA"

        # 3. Determinar Impulso a favor de tendencia vs Retroceso contra tendencia
        # En tendencia alcista: precio >= EMA20 -> Impulso; precio < EMA20 -> Retroceso
        # En tendencia bajista: precio <= EMA20 -> Impulso; precio > EMA20 -> Retroceso
        if trend == "ALCISTA":
            is_impulso = curr_price >= last_ema20
        else:
            is_impulso = curr_price <= last_ema20

        mov_tipo = "IMPULSO (A Favor)" if is_impulso else "RETROCESO (En Contra)"

        # 4. Cálculo de Fibonacci en velas de 5 min (Swing High / Swing Low en 30 velas)
        swing_high = df_5m['high'].tail(30).max()
        swing_low = df_5m['low'].tail(30).min()
        range_fib = swing_high - swing_low

        if range_fib > 0:
            if is_impulso:
                # Impulso a favor -> Retroceso de Fibonacci (38.2% y 61.8%)
                if trend == "ALCISTA":
                    fib_s1 = swing_high - (0.382 * range_fib)  # 1ra linea soporte
                    fib_s3 = swing_high - (0.618 * range_fib)  # 3ra linea soporte
                    fib_r1 = swing_high                        # 1ra linea resistencia TP
                    fib_r3 = swing_high + (0.618 * range_fib)  # 3ra linea resistencia SL
                else:
                    fib_r1 = swing_low + (0.382 * range_fib)   # 1ra linea resistencia
                    fib_r3 = swing_low + (0.618 * range_fib)   # 3ra linea resistencia
                    fib_s1 = swing_low                         # 1ra linea soporte TP
                    fib_s3 = swing_low - (0.618 * range_fib)   # 3ra linea soporte SL
            else:
                # Retroceso contra la tendencia -> Extensión de Fibonacci (127.2% y 161.8%)
                if trend == "ALCISTA":
                    fib_r1 = swing_high + (0.272 * range_fib)  # 1ra extension resistencia
                    fib_r3 = swing_high + (0.618 * range_fib)  # 3ra extension resistencia
                    fib_s1 = swing_low - (0.272 * range_fib)   # 1ra extension soporte
                    fib_s3 = swing_low - (0.618 * range_fib)   # 3ra extension soporte
                else:
                    fib_s1 = swing_low - (0.272 * range_fib)   # 1ra extension soporte
                    fib_s3 = swing_low - (0.618 * range_fib)   # 3ra extension soporte
                    fib_r1 = swing_high + (0.272 * range_fib)  # 1ra extension resistencia
                    fib_r3 = swing_high + (0.618 * range_fib)  # 3ra extension resistencia
        else:
            fib_s1 = curr_price * 0.998
            fib_s3 = curr_price * 0.995
            fib_r1 = curr_price * 1.002
            fib_r3 = curr_price * 1.005

        # 5. Señal de entrada (Puntos 7 y 8)
        signal = None
        if vela_color == "ROJA":
            # Puntu 7: Entrada LONG cuando vela de apertura es ROJA
            # Entrada en 1ra línea Fib (S1), TP en 1ra línea Fib (R1), SL en 3ra línea Fib (S3)
            if curr_price <= fib_s1 * 1.0005:
                signal = "LONG"
        elif vela_color == "VERDE":
            # Punto 8: Entrada SHORT cuando vela de apertura es VERDE
            # Entrada en 1ra línea Fib (R1), TP en 1ra línea Fib (S1), SL en 3ra línea Fib (R3)
            if curr_price >= fib_r1 * 0.9995:
                signal = "SHORT"

        return {
            'bolsa_nombre': selected_bolsa,
            'vela_apertura_str': vela_apertura_str,
            'vela_color': vela_color,
            'trend': trend,
            'mov_tipo': mov_tipo,
            'signal': signal,
            'fib_s1': self._format_price(fib_s1),
            'fib_s3': self._format_price(fib_s3),
            'fib_r1': self._format_price(fib_r1),
            'fib_r3': self._format_price(fib_r3),
            'current_price': curr_price
        }

    def get_active_position(self):
        """Consulta la posición actualmente abierta."""
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

    def open_position(self, side, current_price, fib_data, bolsa_nombre):
        """
        Entrada en posición según reglas:
        Long: Entrada en 1er soporte Fib ($S_1$), TP en 1ra resistencia Fib ($R_1$), SL en 3er soporte Fib ($S_3$).
        Short: Entrada en 1ra resistencia Fib ($R_1$), TP en 1er soporte Fib ($S_1$), SL en 3ra resistencia Fib ($R_3$).
        """
        notional_val = self.margin_usdt * self.leverage
        qty = self._format_quantity(notional_val / current_price)

        if side == 'LONG':
            tp_price = fib_data['fib_r1']
            sl_price = fib_data['fib_s3']
        else:
            tp_price = fib_data['fib_s1']
            sl_price = fib_data['fib_r3']

        logging.info(f"ENTRADA APERTURA [{bolsa_nombre}]: {side} a ${current_price:.2f} | TP: ${tp_price} | SL: ${sl_price}")

        if self.dry_run:
            self.current_position = side
            self.active_bolsa = bolsa_nombre
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.entry_time = datetime.now()
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            return True

        try:
            # Cancelar órdenes previas
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": self.symbol})
            except Exception:
                pass

            order_side = 'BUY' if side == 'LONG' else 'SELL'
            self.client.futures_create_order(
                symbol=self.symbol,
                side=order_side,
                type='MARKET',
                quantity=qty
            )

            time.sleep(1)
            active_side, real_entry, real_qty = self.get_active_position()
            if real_entry > 0:
                current_price = real_entry

            exit_side = 'SELL' if side == 'LONG' else 'BUY'

            # Take Profit en orden condicional
            try:
                self.client._request_futures_api(
                    "post", "algoOrder", signed=True,
                    data={
                        'algoType': 'CONDITIONAL',
                        'symbol': self.symbol,
                        'side': exit_side,
                        'type': 'TAKE_PROFIT_MARKET',
                        'triggerPrice': str(tp_price),
                        'closePosition': 'true'
                    }
                )
            except Exception as e:
                logging.error(f"Error creando TP algo: {e}")

            # Stop Loss en orden condicional
            try:
                self.client._request_futures_api(
                    "post", "algoOrder", signed=True,
                    data={
                        'algoType': 'CONDITIONAL',
                        'symbol': self.symbol,
                        'side': exit_side,
                        'type': 'STOP_MARKET',
                        'triggerPrice': str(sl_price),
                        'closePosition': 'true'
                    }
                )
            except Exception as e:
                logging.error(f"Error creando SL algo: {e}")

            self.current_position = side
            self.active_bolsa = bolsa_nombre
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.entry_time = datetime.now()
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            return True

        except Exception as e:
            logging.error(f"Error al ejecutar orden en Binance: {e}")
            return False

    def _record_trade_result(self, pnl):
        if pnl > 0:
            self.winning_trades += 1
            self.money_won += pnl
        elif pnl < 0:
            self.losing_trades += 1
            self.money_lost += abs(pnl)

    def _save_trade_to_file(self, exit_type, max_gain_pct, max_loss_pct, dur_mins, exit_time=None):
        """
        DETALLES 2: Guardar en tp.txt o sl.txt con columnas alineadas:
        dia, hora, bolsa, % ganancia maximo, % perdida maximo, duracion de la operacion
        """
        filename = "tp.txt" if exit_type.lower() == "tp" else "sl.txt"
        if exit_time is None:
            exit_time = datetime.now()

        dia_str = exit_time.strftime('%Y-%m-%d')
        hora_str = exit_time.strftime('%H:%M:%S')
        bolsa_str = self.active_bolsa if self.active_bolsa else "GENERAL"

        gain_str = f"+{max_gain_pct:.2f}%"
        loss_str = f"{max_loss_pct:.2f}%"
        dur_str = f"{dur_mins:.1f}m"

        line = f"{dia_str:<10} | {hora_str:<8} | {bolsa_str:<8} | {gain_str:<14} | {loss_str:<13} | {dur_str:<10}\n"

        try:
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line)
            logging.info(f"Registro guardado en {filename}: {line.strip()}")
        except Exception as e:
            logging.error(f"Error escribiendo en {filename}: {e}")

    def check_simulated_exit(self, current_price):
        """Evalúa TP y SL en simulación."""
        if not self.dry_run or not self.current_position:
            return

        pos = self.current_position
        tp = self.tp_price
        sl = self.sl_price

        hit_tp = (pos == 'LONG' and current_price >= tp) or (pos == 'SHORT' and current_price <= tp)
        hit_sl = (pos == 'LONG' and current_price <= sl) or (pos == 'SHORT' and current_price >= sl)

        if hit_tp or hit_sl:
            exit_time = datetime.now()
            dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

            if hit_tp:
                pnl = self.margin_usdt * ((abs(tp - self.entry_price) / self.entry_price) * self.leverage)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                self._save_trade_to_file("tp", self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
            elif hit_sl:
                pnl = -self.margin_usdt * ((abs(sl - self.entry_price) / self.entry_price) * self.leverage)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                self._save_trade_to_file("sl", self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)

            self.current_position = None
            self.active_bolsa = "NINGUNA"
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0

    def render_screen(self, strat_data, active_pos, entry, qty, pnl_pct, dur_mins, schedule_ok, schedule_reason):
        """
        DETALLES 3:
        1) Mantener cabecera siempre visible en pantalla.
        2) Mantener visible en pantalla únicamente el estado actual.
        3) No utilizar colores en todo el texto visualizado en pantalla.
        4) Borrar pantalla antes de actualizar la visualización de datos.

        Formato del estado actual (exactamente 4 líneas):
        en una linea: nombre de estrategia
        en otra linea: precio, vela apertura
        en otra linea: horario
        en otra linea: posicion
        """
        # 1. Borrar pantalla antes de actualizar
        os.system('cls' if os.name == 'nt' else 'clear')

        # 2. Consultar saldo
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

        # Formato de la posición
        if active_pos:
            dur_str = f" ({dur_mins:.1f}m)"
            pnl_sign = "+" if pnl_pct >= 0 else ""
            pos_str = f"{active_pos} @ ${entry:.2f} [TP: ${self.tp_price:.2f}, SL: ${self.sl_price:.2f}] [{pnl_sign}{pnl_pct:.2f}% ROI]{dur_str}"
        else:
            pos_str = "SIN POSICION"

        # Construcción del texto de consola (sin secuencias de colores ANSI)
        lines = []
        lines.append("======================================================================")
        lines.append("       BOT DE TRADING AUTOMATICO BINANCE - ESTADO ACTUAL")
        lines.append("======================================================================")
        lines.append(f"Simbolo: {self.symbol} | Modo: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt:.2f} USDT")
        lines.append(f"Modo de Ejecucion: {'DRY-RUN (Simulacion)' if self.dry_run else 'REAL API'}")
        lines.append("----------------------------------------------------------------------")
        if has_keys:
            lines.append(f"Saldo Wallet: {wallet_bal:.2f} USDT | Disponible: {avail_bal:.2f} USDT | PnL No Realizado: {unrealized:.2f} USDT")
        else:
            lines.append(f"Saldo Wallet (Simulado): {wallet_bal:.2f} USDT")
        lines.append(f"Resumen General: Tiempo: {uptime_hours:.2f}h | Ganadas: {self.winning_trades} (+{self.money_won:.2f} USDT) | Perdidas: {self.losing_trades} (-{self.money_lost:.2f} USDT)")
        lines.append("======================================================================")

        # Formato exacto de 4 líneas para el estado actual de la estrategia:
        # Linea 1: nombre de estrategia
        # Linea 2: precio, vela apertura
        # Linea 3: horario
        # Linea 4: posicion
        lines.append("Estrategia: Apertura Market Open (Euronext 04:00 / NY 10:30 / Tokio 21:00 hs)")
        lines.append(f"Precio: ${strat_data['current_price']:.2f} | Vela Apertura: {strat_data['vela_apertura_str']}")
        lines.append(f"Horario: {schedule_reason}")
        lines.append(f"Posicion: {pos_str}")
        lines.append("======================================================================")

        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()

    def run(self):
        """Bucle principal de ejecución del bot."""
        logging.info("Iniciando monitoreo de Estrategia de Apertura...")

        while True:
            try:
                # 1. Analizar estrategia de apertura
                strat_data = self.analyze_strategy_apertura()
                curr_price = strat_data['current_price']

                if curr_price == 0.0:
                    time.sleep(5)
                    continue

                # 2. Consultar posición activa
                active_pos, entry, qty = self.get_active_position()

                if self.dry_run and active_pos:
                    self.check_simulated_exit(curr_price)
                    active_pos, entry, qty = self.get_active_position()
                elif not self.dry_run and active_pos is None and self.current_position:
                    # Cierre real por TP/SL
                    exit_time = datetime.now()
                    dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

                    try:
                        self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                        self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": self.symbol})
                    except Exception:
                        pass

                    pnl = (curr_price - self.entry_price) * self.position_qty if self.current_position == 'LONG' else (self.entry_price - curr_price) * self.position_qty
                    target_type = "tp" if pnl >= 0 else "sl"
                    self._save_trade_to_file(target_type, self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
                    self._record_trade_result(pnl)

                    self.current_position = None
                    self.active_bolsa = "NINGUNA"
                    self.entry_time = None
                    self.max_pnl_pct = 0.0
                    self.min_pnl_pct = 0.0

                pnl_pct = 0.0
                dur_mins = 0.0
                if active_pos and entry > 0:
                    dur_mins = (datetime.now() - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0
                    if active_pos == 'LONG':
                        pnl_pct = ((curr_price - entry) / entry) * self.leverage * 100.0
                    else:
                        pnl_pct = ((entry - curr_price) / entry) * self.leverage * 100.0

                    if pnl_pct > self.max_pnl_pct:
                        self.max_pnl_pct = pnl_pct
                    if pnl_pct < self.min_pnl_pct:
                        self.min_pnl_pct = pnl_pct

                # 3. Validar horario de operación
                schedule_ok, schedule_reason = self.is_market_open_and_session()

                # 4. Renderizar pantalla monocromática con estado actual
                self.render_screen(
                    strat_data=strat_data,
                    active_pos=active_pos,
                    entry=entry,
                    qty=qty,
                    pnl_pct=pnl_pct,
                    dur_mins=dur_mins,
                    schedule_ok=schedule_ok,
                    schedule_reason=schedule_reason
                )

                # 5. Abrir posición si no hay ninguna activa y el horario es válido
                if active_pos is None and schedule_ok and strat_data['signal']:
                    self.open_position(
                        side=strat_data['signal'],
                        current_price=curr_price,
                        fib_data=strat_data,
                        bolsa_nombre=strat_data['bolsa_nombre']
                    )

                time.sleep(5)

            except KeyboardInterrupt:
                print("\n[!] Bot detenido por el usuario. Exiting...")
                break
            except Exception as e:
                logging.error(f"Excepción en el bucle principal: {e}")
                time.sleep(5)


if __name__ == "__main__":
    bot = BinanceAperturaBot()
    bot.run()
