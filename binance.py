#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de trading automatico en Binance.com

Modo Aislado
Apalancamienot 1x 
Monto 5 usdt

Estrategia: Oracle numeris
1) operar las 24 hs los 7 dias de la semana
2) hacer todos los calculos en temporalidad de 1 min
3) hacer una sola entrada a la vez, no hacer varias entradas en simultaneo
4) entrada en long: cuando el indicador oracle numeris marca señal de compra al finalizar la vela de 1 min
   cerrar operacion cuando el precio bajo usdt 100 desde el punto de entrada
   no colocar SL
5) entrada en short: cuando el indicador oracle numeris marca señal de venta al finalizar la vela de 1 min
   cerrar operacion cuando el precio sube usdt 100 desde el punto de entrada
   no colocar SL

El formato del estado actual para estrategia:
en una linea: nombre de estrategia
en otra linea: precio
en otra linea: horario
en otra linea: posicion

Detalles:
1) Cerrar posiciones abiertas al iniciar bot

2) hacer archivo 2ganadas.txt donde van las operaciones que se ganaron y archivo 2perdidas.txt donde van las opereciones que se perdieron, con columnas alineadas, con los datos:
dia, hora, bolsa, % ganancia maximo, % perdida maximo, duracion de la operacion

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
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# 1. Configuración de consola Windows para soporte ANSI/VT
if os.name == 'nt':
    os.system('')
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# 2. Configuración de Logging a bot.log (para mantener la consola limpia y monocroma)
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

# 4. Importar biblioteca python-binance evitando conflicto con el nombre de archivo local
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


class BinanceOracleNumerisBot:
    def __init__(self):
        # Claves API desde .envprivado
        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        # Parámetros desde .envpublico
        self.symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
        self.margin_usdt = float(os.getenv("MARGIN_USDT", "5.0"))
        self.leverage = int(os.getenv("LEVERAGE", "1"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.close_diff_usdt = float(os.getenv("CLOSE_DIFF_USDT", "100.0"))
        self.poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "2"))
        self.strategy_name = os.getenv("STRATEGY_NAME", "Oracle numeris")

        # Modos de ejecución (dinero real por defecto)
        self.dry_run = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Cliente Binance y reglas de precisión
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001

        # Control de velas cerradas (para evitar repetición de señales en la misma vela)
        self.last_evaluated_candle_time = None

        # Estado de la posición activa (una sola entrada a la vez)
        self.current_position = None  # None, 'LONG', 'SHORT'
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.entry_time = None
        self.simulated_balance = 100.0

        # Métricas de la operación en curso
        self.max_pnl_pct = 0.0
        self.min_pnl_pct = 0.0

        # Métricas de la sesión
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0

        # Inicialización de archivos y cliente
        self._init_trade_log_files()
        self._initialize_client()

    def _init_trade_log_files(self):
        """DETALLES 2: Inicializar 2ganadas.txt y 2perdidas.txt con columnas alineadas."""
        header = "Dia        | Hora     | Bolsa    | % Ganancia Max | % Perdida Max | Duracion  \n--------------------------------------------------------------------------------\n"
        for filename in ["2ganadas.txt", "2perdidas.txt"]:
            if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                try:
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(header)
                except Exception as e:
                    logging.error(f"Error inicializando {filename}: {e}")

    def _initialize_client(self):
        """Inicializa cliente Binance, configura modo AISLADO 1x y cierra posiciones abiertas iniciales."""
        logging.info("Iniciando Bot Binance Oracle Numeris (24/7)...")
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
                logging.info("Conexión autenticada exitosamente a Binance Futures API con dinero real.")
            else:
                logging.info("Modo de simulación (DRY-RUN) activo o sin API keys.")

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
            logging.warning(f"No se pudieron obtener precisiones dinámicas ({e}). Usando valores por defecto.")

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
        """Configura margen AISLADO y apalancamiento 1x en Binance Futures."""
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
            # Cancelar órdenes pendientes previas
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

    def fetch_klines(self, limit=120):
        """Obtiene klines OHLCV en temporalidad de 1 minuto (1m) desde Binance Futures."""
        try:
            klines = self.client.futures_klines(symbol=self.symbol, interval=self.timeframe, limit=limit)
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            return df
        except Exception as e:
            logging.error(f"Error al obtener klines ({self.timeframe}): {e}")
            return None

    def calculate_oracle_numeris_indicator(self, df):
        """
        Emulación cuantitativa del indicador Oracle Numeris:
        Integra los componentes clave del script:
        1. Medias Móviles Exponenciales (EMA 9 rápida y EMA 21 lenta) para dirección tendencial.
        2. Medias de soporte/resistencia dinámica y confirmación por Bandas de Volatilidad (Desviación Típica 20).
        3. Oscilador de Momentum / Divergencia de Volumen relativo (Oracle Oscillator).
        4. Señal evaluada de forma ESTRICTA al cierre/finalización de la vela (última vela cerrada df.iloc[-2]).
        """
        # Medias Exponenciales
        df['ema_fast'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=21, adjust=False).mean()
        df['ema_trend'] = df['close'].ewm(span=50, adjust=False).mean()

        # Bandas de Volatilidad (SMA 20 + 2 StDev)
        df['sma20'] = df['close'].rolling(window=20).mean()
        df['std20'] = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['sma20'] + (df['std20'] * 2.0)
        df['bb_lower'] = df['sma20'] - (df['std20'] * 2.0)

        # Filtro de volumen relativo
        df['vol_ma'] = df['volume'].rolling(window=20).mean()

        # Oscilador Oracle (RSI + Momentum Normalizado)
        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi'] = 100.0 - (100.0 / (1.0 + rs))

        # Condiciones de señal en vela cerrada (df.iloc[-2])
        # Compra: EMA rápida > EMA lenta, vela alcista cruzando o sobre soporte, RSI saliendo de sobreventa o con momentum alcista, volumen activo
        # Venta: EMA rápida < EMA lenta, vela bajista perdiendo o bajo resistencia, RSI perdiendo fuerza/sobrecompra
        return df

    def analyze_strategy(self):
        """
        Estrategia: Oracle numeris
        1) operar las 24 hs los 7 dias de la semana
        2) hacer todos los calculos en temporalidad de 1 min
        3) hacer una sola entrada a la vez, no hacer varias entradas en simultaneo
        4) entrada en long: cuando el indicador oracle numeris marca señal de compra al finalizar la vela de 1 min
           cerrar operacion cuando el precio bajo usdt 100 desde el punto de entrada
           no colocar SL
        5) entrada en short: cuando el indicador oracle numeris marca señal de venta al finalizar la vela de 1 min
           cerrar operacion cuando el precio sube usdt 100 desde el punto de entrada
           no colocar SL
        """
        df = self.fetch_klines(limit=120)
        default_result = {
            'strategy_name': self.strategy_name,
            'current_price': 0.0,
            'oracle_signal': 'NEUTRAL',
            'entry_signal': None,
            'candle_time': None
        }

        if df is None or len(df) < 55:
            return default_result

        df = self.calculate_oracle_numeris_indicator(df)

        # Vela actual en desarrollo (tiempo real)
        curr_candle = df.iloc[-1]
        curr_price = float(curr_candle['close'])
        default_result['current_price'] = curr_price

        # Vela anterior: es la vela de 1 minuto finalizada/cerrada
        closed_candle = df.iloc[-2]
        prev_closed_candle = df.iloc[-3]
        closed_time = closed_candle['timestamp']
        default_result['candle_time'] = closed_time

        # Determinar señal del indicador Oracle Numeris en la vela finalizada
        c_close = closed_candle['close']
        c_open = closed_candle['open']
        c_fast = closed_candle['ema_fast']
        c_slow = closed_candle['ema_slow']
        c_vol = closed_candle['volume']
        c_vol_ma = closed_candle['vol_ma']
        c_rsi = closed_candle['rsi']

        p_close = prev_closed_candle['close']
        p_fast = prev_closed_candle['ema_fast']
        p_slow = prev_closed_candle['ema_slow']

        # Detección de cruce o confirmación tendencial al finalizar la vela
        bullish_cross = (p_fast <= p_slow) and (c_fast > c_slow)
        bullish_continuation = (c_fast > c_slow) and (c_close > c_open) and (c_rsi > 50) and (c_vol > c_vol_ma * 0.8)

        bearish_cross = (p_fast >= p_slow) and (c_fast < c_slow)
        bearish_continuation = (c_fast < c_slow) and (c_close < c_open) and (c_rsi < 50) and (c_vol > c_vol_ma * 0.8)

        oracle_signal = "NEUTRAL"
        if bullish_cross or (bullish_continuation and c_rsi < 70 and p_close < c_close):
            oracle_signal = "COMPRA"
        elif bearish_cross or (bearish_continuation and c_rsi > 30 and p_close > c_close):
            oracle_signal = "VENTA"

        default_result['oracle_signal'] = oracle_signal

        # Validar si esta vela cerrada ya generó entrada para no duplicar en la misma vela de 1 min
        if self.last_evaluated_candle_time != closed_time:
            if oracle_signal == "COMPRA":
                default_result['entry_signal'] = 'LONG'
                self.last_evaluated_candle_time = closed_time
            elif oracle_signal == "VENTA":
                default_result['entry_signal'] = 'SHORT'
                self.last_evaluated_candle_time = closed_time
        else:
            default_result['entry_signal'] = None

        return default_result

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

    def open_position(self, side, current_price):
        """Ejecuta apertura de posición LONG o SHORT en Binance Futures a precio MARKET."""
        notional_val = self.margin_usdt * self.leverage
        qty = self._format_quantity(notional_val / current_price)

        logging.info(f"EJECUTANDO ENTRADA {side}: Monto {self.margin_usdt} USDT x {self.leverage}x = {notional_val} USDT ({qty} {self.symbol}) a ~${current_price:.2f}")

        if self.dry_run:
            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.entry_time = datetime.now()
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            return True

        try:
            # Cancelar cualquier orden residual antes de abrir
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
            logging.info(f"Orden de apertura completada: {order.get('orderId')}")

            time.sleep(1)
            active_side, real_entry, real_qty = self.get_active_position()
            if real_entry > 0:
                current_price = real_entry
                qty = real_qty

            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.entry_time = datetime.now()
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            return True

        except Exception as e:
            logging.error(f"Error al abrir posición en Binance Futures: {e}")
            return False

    def check_exit_condition(self, current_price):
        """
        Reglas de salida:
        - LONG: cerrar operacion cuando el precio bajo usdt 100 desde el punto de entrada (no colocar SL).
        - SHORT: cerrar operacion cuando el precio sube usdt 100 desde el punto de entrada (no colocar SL).
        """
        if not self.current_position or self.entry_price <= 0:
            return False, None

        if self.current_position == 'LONG':
            diff = self.entry_price - current_price
            if diff >= self.close_diff_usdt:
                return True, f"Precio bajo {diff:.2f} USDT desde entrada (umbral: {self.close_diff_usdt:.2f} USDT)"

        elif self.current_position == 'SHORT':
            diff = current_price - self.entry_price
            if diff >= self.close_diff_usdt:
                return True, f"Precio subio {diff:.2f} USDT desde entrada (umbral: {self.close_diff_usdt:.2f} USDT)"

        return False, None

    def close_position(self, current_price, reason="Condicion de salida alcanzada"):
        """Cierra la posición actual a mercado sin SL y registra en 2ganadas.txt o 2perdidas.txt."""
        if not self.current_position:
            return

        side = self.current_position
        logging.info(f"CERRANDO POSICION {side} por {reason} a ~${current_price:.2f}...")

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
                logging.info(f"Orden MARKET de cierre de {side} ejecutada.")
            except Exception as e:
                logging.error(f"Error enviando orden de cierre a Binance: {e}")

        # Calcular PnL de la operación con apalancamiento 1x
        if side == 'LONG':
            pnl_pct = ((current_price - self.entry_price) / self.entry_price) * self.leverage * 100.0
            pnl_usdt = self.margin_usdt * (pnl_pct / 100.0)
        else:
            pnl_pct = ((self.entry_price - current_price) / self.entry_price) * self.leverage * 100.0
            pnl_usdt = self.margin_usdt * (pnl_pct / 100.0)

        if self.dry_run:
            self.simulated_balance += pnl_usdt

        # Actualizar estadísticas y registrar en archivo correspondiente (2ganadas.txt o 2perdidas.txt)
        self._record_and_save_trade(pnl_usdt, self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)

        # Resetear estado de posición
        self.current_position = None
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.entry_time = None
        self.max_pnl_pct = 0.0
        self.min_pnl_pct = 0.0

    def _record_and_save_trade(self, pnl, max_gain_pct, max_loss_pct, dur_mins, exit_time):
        """
        DETALLES 2:
        - 2ganadas.txt para operaciones con ganancia (pnl > 0).
        - 2perdidas.txt para operaciones con pérdida (pnl <= 0).
        Columnas alineadas:
        dia, hora, bolsa, % ganancia maximo, % perdida maximo, duracion de la operacion
        """
        if pnl > 0:
            self.winning_trades += 1
            self.money_won += pnl
            filename = "2ganadas.txt"
        else:
            self.losing_trades += 1
            self.money_lost += abs(pnl)
            filename = "2perdidas.txt"

        dia_str = exit_time.strftime('%Y-%m-%d')
        hora_str = exit_time.strftime('%H:%M:%S')
        bolsa_str = "BINANCE"

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

    def render_screen(self, strat_data, active_pos, entry, qty, pnl_pct, dur_mins, now_dt):
        """
        DETALLES 3:
        - Mantener cabecera siempre visible en pantalla.
        - Mantener visible en pantalla únicamente el estado actual.
        - No utilizar colores en todo el texto visualizado en pantalla (Monocromo).
        - Reposicionar el cursor al inicio de la pantalla antes de actualizar la visualización de datos (\033[H).
        - El formato del estado actual para estrategia:
          en una linea: nombre de estrategia
          en otra linea: precio
          en otra linea: horario
          en otra linea: posicion
        """
        # Reposicionar el cursor al inicio de la pantalla (evita parpadeos de borrado completo)
        sys.stdout.write("\033[H")

        # Consultar balance
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

        # Formato de la posición actual
        curr_price = strat_data['current_price']
        if active_pos and entry > 0:
            dur_str = f" ({dur_mins:.1f}m)"
            pnl_sign = "+" if pnl_pct >= 0 else ""
            if active_pos == 'LONG':
                cierre_info = f"Cierre si precio baja a ${entry - self.close_diff_usdt:.2f} (-{self.close_diff_usdt:.0f} USDT)"
            else:
                cierre_info = f"Cierre si precio sube a ${entry + self.close_diff_usdt:.2f} (+{self.close_diff_usdt:.0f} USDT)"
            pos_line_str = f"{active_pos} @ ${entry:.2f} | PnL: {pnl_sign}{pnl_pct:.2f}%{dur_str} | {cierre_info} | Sin SL"
        else:
            pos_line_str = "SIN POSICION"

        curr_price_str = f"${curr_price:.2f}"
        horario_str = f"Operando 24/7 continuo | Hora actual: {now_dt.strftime('%Y-%m-%d %H:%M:%S')}"

        # Construcción del texto monocromo (sin códigos de colores ANSI)
        lines = []
        lines.append("======================================================================")
        lines.append("       BOT DE TRADING AUTOMATICO BINANCE - ESTRATEGIA ORACLE NUMERIS")
        lines.append("======================================================================")
        lines.append(f"Simbolo: {self.symbol} | Modo: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt:.2f} USDT")
        lines.append(f"Modo de Ejecucion: {'DRY-RUN (Simulacion)' if self.dry_run else 'REAL (Dinero Real en Binance Futures)'}")
        lines.append("----------------------------------------------------------------------")
        if has_keys:
            lines.append(f"Saldo Wallet: {wallet_bal:.2f} USDT | Disponible: {avail_bal:.2f} USDT | PnL No Realizado: {unrealized:.2f} USDT")
        else:
            lines.append(f"Saldo Wallet (Simulado): {wallet_bal:.2f} USDT")
        lines.append(f"Resumen: Tiempo: {uptime_hours:.2f}h | Ganadas: {self.winning_trades} (+{self.money_won:.2f} USDT) | Perdidas: {self.losing_trades} (-{self.money_lost:.2f} USDT)")
        lines.append("======================================================================")

        # Formato del estado actual para estrategia (4 líneas exactas requeridas):
        # en una linea: nombre de estrategia
        # en otra linea: precio
        # en otra linea: horario
        # en otra linea: posicion
        lines.append(f"nombre de estrategia: {strat_data['strategy_name']}")
        lines.append(f"precio: {curr_price_str}")
        lines.append(f"horario: {horario_str}")
        lines.append(f"posicion: {pos_line_str}")
        lines.append("======================================================================")

        # Borrar hasta el final de cada línea (\033[K) y de la pantalla (\033[J) para actualización limpia
        rendered_output = "\n".join(line + "\033[K" for line in lines) + "\033[J\n"
        sys.stdout.write(rendered_output)
        sys.stdout.flush()

    def run(self):
        """Bucle principal de ejecución del bot 24/7 en velas de 1 minuto."""
        logging.info("Bucle principal de monitoreo Oracle Numeris 24/7 iniciado.")

        while True:
            try:
                now_dt = datetime.now()

                # 1. Analizar estrategia Oracle Numeris en temporalidad de 1 minuto (1m)
                strat_data = self.analyze_strategy()
                curr_price = strat_data['current_price']

                if curr_price == 0.0:
                    time.sleep(self.poll_interval)
                    continue

                # 2. Consultar posición activa
                active_pos, entry, qty = self.get_active_position()

                # Si no está en dry_run pero externamente se cerró la posición en Binance
                if not self.dry_run and active_pos is None and self.current_position is not None:
                    exit_time = datetime.now()
                    dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0
                    pnl = (curr_price - self.entry_price) * self.position_qty if self.current_position == 'LONG' else (self.entry_price - curr_price) * self.position_qty
                    self._record_and_save_trade(pnl, self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
                    self.current_position = None
                    self.entry_price = 0.0
                    self.position_qty = 0.0
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

                # 3. Renderizar pantalla monocroma con cabecera y estado actual
                self.render_screen(
                    strat_data=strat_data,
                    active_pos=active_pos,
                    entry=entry,
                    qty=qty,
                    pnl_pct=pnl_pct,
                    dur_mins=dur_mins,
                    now_dt=now_dt
                )

                # 4. Lógica de salidas:
                # - Long: cerrar operacion cuando el precio bajo usdt 100 desde el punto de entrada (no colocar SL)
                # - Short: cerrar operacion cuando el precio sube usdt 100 desde el punto de entrada (no colocar SL)
                should_close, close_reason = self.check_exit_condition(curr_price)
                if active_pos and should_close:
                    self.close_position(curr_price, reason=close_reason)
                    active_pos = None

                # 5. Lógica de entradas:
                # 1) operar las 24 hs los 7 dias de la semana
                # 3) hacer una sola entrada a la vez, no hacer varias entradas en simultaneo
                # 4) entrada en long: cuando el indicador oracle numeris marca señal de compra al finalizar la vela de 1 min
                # 5) entrada en short: cuando el indicador oracle numeris marca señal de venta al finalizar la vela de 1 min
                if active_pos is None and strat_data['entry_signal']:
                    signal = strat_data['entry_signal']
                    self.open_position(side=signal, current_price=curr_price)

                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                print("\n[!] Bot detenido por el usuario.")
                break
            except Exception as e:
                logging.error(f"Excepción en el bucle principal: {e}")
                time.sleep(self.poll_interval)


if __name__ == "__main__":
    bot = BinanceOracleNumerisBot()
    bot.run()
