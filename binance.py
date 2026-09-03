#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de Trading Automático en Binance Futures (USDT-M)
Estrategia 1: Confluencia Divergencias RSI + Oscilador Oracle + VWAP (Velas de 3 Minutos)
Estrategia 2: Apertura Market Open 10:30 hs ART (Velas 1m / 5m + Tendencia + Extensiones y Retrocesos Fibonacci)

Configuración:
- Modo: Aislado (Isolated)
- Apalancamiento: 10x
- Monto por Operación: 50 USDT
- Take Profit (TP): +3% ROI sobre el monto de la operación
- Stop Loss (SL): -3% ROI sobre el monto de la operación
- Horario de Operaciones: 10:30 a 17:00 hs (Horario Buenos Aires)
- Monitoreo en Consola: Monocromo sin colores, cabecera fija, actualización de estado actual.
- Registro de Trades: tp.txt y sl.txt (estrategia, hora, % ganancia máximo, % pérdida máximo, duración)
- Variables de Entorno: envprivado (API Keys) y .env (resto de parámetros)
"""

import os
import sys
import time
import math
import logging
import shutil
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

# 2. Configuración de Logging (a bot.log para mantener la pantalla limpia)
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



# 4. Importar biblioteca python-binance evitando sombreado de módulo local
current_dir = sys.path.pop(0) if sys.path and sys.path[0] in ('', os.getcwd(), os.path.dirname(__file__)) else None
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
finally:
    if current_dir is not None:
        sys.path.insert(0, current_dir)


class BinanceMultiStrategyBot:
    def __init__(self):
        # Cargar API Keys y Configuración
        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        self.symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
        self.margin_usdt = float(os.getenv("MARGIN_USDT", "50.0"))
        self.leverage = int(os.getenv("LEVERAGE", "10"))
        
        # Selección de Estrategias: '1', '2', 'ALL'
        self.active_strategy_cfg = os.getenv("ACTIVE_STRATEGY", "ALL").upper()

        # TP y SL (3% ROI sobre el monto de la operación)
        self.tp_roi_pct = float(os.getenv("TP_ROI_PCT", "3.0"))
        self.sl_roi_pct = float(os.getenv("SL_ROI_PCT", "3.0"))

        # Configuración Estrategia 1 (3m)
        self.timeframe_strat1 = os.getenv("TIMEFRAME_STRAT1", "3m")
        self.rsi_period = int(os.getenv("RSI_PERIOD", "14"))
        self.pivot_left = int(os.getenv("PIVOT_LOOKBACK_LEFT", "5"))
        self.pivot_right = int(os.getenv("PIVOT_LOOKBACK_RIGHT", "2"))

        # Configuración Estrategia 2 (Apertura 10:30 hs)
        self.opening_time_str = os.getenv("OPENING_TIME", "10:30").strip()
        self.tf_strat2_open = os.getenv("TIMEFRAME_STRAT2_OPEN", "1m")
        self.tf_strat2_trend = os.getenv("TIMEFRAME_STRAT2_TREND", "5m")

        # Modos de Ejecución
        self.dry_run = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Configuración Horario de Operaciones (10:30 - 17:00 hs Buenos Aires)
        self.enable_schedule = os.getenv("ENABLE_SCHEDULE_FILTER", "True").lower() in ("true", "1", "yes")
        self.schedule_start_str = os.getenv("SCHEDULE_START", "10:30").strip()
        self.schedule_end_str = os.getenv("SCHEDULE_END", "17:00").strip()
        self.schedule_weekdays_only = os.getenv("SCHEDULE_WEEKDAYS_ONLY", "True").lower() in ("true", "1", "yes")

        # Cliente Binance y Reglas de Mercado
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001

        # Estado de posición activa en bot
        self.current_position = None  # None, 'LONG', 'SHORT'
        self.position_strategy = None  # 'Estrategia 1: Divergencia RSI+Oracle+VWAP' o 'Estrategia 2: Apertura'
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.tp_price = 0.0
        self.sl_price = 0.0
        self.entry_time = None
        self.position_start_time = 0
        self.simulated_balance = 100.0

        # Estadísticas y métricas de trade
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0
        self.max_pnl_pct = 0.0
        self.min_pnl_pct = 0.0

        # Inicialización
        self._initialize_client()

    def _initialize_client(self):
        """Inicializa el cliente Binance API, valida credenciales y configura el entorno."""
        logging.info("Inicializando Bot Multiestrategia Binance...")
        logging.info(f"Símbolo: {self.symbol} | Margen: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt} USDT")
        logging.info(f"TP: +{self.tp_roi_pct}% ROI | SL: -{self.sl_roi_pct}% ROI | Dry-Run: {self.dry_run}")

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
                logging.info("Conexión pública de mercado lista.")

            # REQUERIMIENTO DETALLES 1: Cerrar posiciones abiertas al iniciar bot
            self.close_existing_positions()

        except Exception as e:
            logging.error(f"Error al inicializar cliente Binance: {e}")
            if not self.dry_run:
                logging.info("Cambiando automáticamente a modo DRY-RUN por error de conexión/API Key.")
                self.dry_run = True

    def _update_symbol_precision(self):
        """Obtiene precisión y filtros de precio/cantidad para el símbolo especificado."""
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
            logging.info(f"Filtros {self.symbol} -> TickSize: {self.tick_size}, StepSize: {self.step_size}")
        except Exception as e:
            logging.warning(f"No se pudieron obtener precisiones dinámicas ({e}). Usando defaults.")

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
        """Configura margen AISLADO y apalancamiento 10x en Binance Futuros."""
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
            self.position_strategy = None
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
            # Cancelar órdenes límite y órdenes algorítmicas/condicionales previas
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
            self.position_strategy = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0

        except Exception as e:
            logging.error(f"Error al cerrar posiciones abiertas iniciales: {e}")

    def fetch_klines(self, timeframe='3m', limit=200):
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

    # ==========================================
    # CÁLCULOS ESTRATEGIA 1: RSI + ORACLE + VWAP
    # ==========================================
    def calculate_rsi(self, df):
        """Calcula el indicador RSI (14) estilo TradingView."""
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        
        avg_gain = gain.ewm(alpha=1.0/self.rsi_period, min_periods=self.rsi_period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1.0/self.rsi_period, min_periods=self.rsi_period, adjust=False).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    def detect_rsi_divergences(self, df):
        """Detecta divergencias de RSI alcistas y bajistas."""
        df = df.copy()
        df['rsi'] = self.calculate_rsi(df)
        n = len(df)
        if n < self.rsi_period + self.pivot_left + self.pivot_right + 5:
            return "Sin Div", df

        pivot_lows = []
        pivot_highs = []
        end_idx = n - 1

        for i in range(self.pivot_left, end_idx - self.pivot_right):
            w_low = df['low'].iloc[i - self.pivot_left : i + self.pivot_right + 1]
            if df['low'].iloc[i] == w_low.min():
                pivot_lows.append({'index': i, 'price': df['low'].iloc[i], 'rsi': df['rsi'].iloc[i]})

            w_high = df['high'].iloc[i - self.pivot_left : i + self.pivot_right + 1]
            if df['high'].iloc[i] == w_high.max():
                pivot_highs.append({'index': i, 'price': df['high'].iloc[i], 'rsi': df['rsi'].iloc[i]})

        signal = "Sin Div"

        # Bullish Divergence (LONG)
        if len(pivot_lows) >= 2:
            p1, p2 = pivot_lows[-2], pivot_lows[-1]
            if (end_idx - p2['index']) <= (self.pivot_right + 5):
                if p2['price'] <= p1['price'] and p2['rsi'] > p1['rsi']:
                    signal = "BULL_DIV"

        # Bearish Divergence (SHORT)
        if len(pivot_highs) >= 2:
            p1, p2 = pivot_highs[-2], pivot_highs[-1]
            if (end_idx - p2['index']) <= (self.pivot_right + 5):
                if p2['price'] >= p1['price'] and p2['rsi'] < p1['rsi']:
                    signal = "BEAR_DIV"

        return signal, df

    def calculate_oracle_oscillator(self, df):
        """Calcula el Oscilador Oracle (Stoch %K 35% + RSI 35% + Williams %R Norm 30% vs Signal 9 EMA)."""
        df = df.copy()
        period = 14
        if len(df) < period + 10:
            return "ORACLE_NEUTRAL", 50.0, 50.0

        low_min = df['low'].rolling(window=period).min()
        high_max = df['high'].rolling(window=period).max()
        denom = (high_max - low_min).replace(0, np.nan)
        
        stoch_k = (((df['close'] - low_min) / denom) * 100.0).fillna(50.0)
        rsi = self.calculate_rsi(df).fillna(50.0)
        williams_norm = (100.0 - (((high_max - df['close']) / denom) * 100.0)).fillna(50.0)

        oracle_line = (stoch_k * 0.35) + (rsi * 0.35) + (williams_norm * 0.30)
        signal_line = oracle_line.ewm(span=9, adjust=False).mean()

        last_oracle = oracle_line.iloc[-1]
        last_signal = signal_line.iloc[-1]

        if last_oracle > last_signal:
            status = "ORACLE_BULL"
        elif last_oracle < last_signal:
            status = "ORACLE_BEAR"
        else:
            status = "ORACLE_NEUTRAL"

        return status, last_oracle, last_signal

    def calculate_vwap(self, df):
        """Calcula el VWAP intradía acumulado."""
        df = df.copy()
        typical_price = (df['high'] + df['low'] + df['close']) / 3.0
        tp_volume = typical_price * df['volume']
        dates = df['timestamp'].dt.date

        cum_tp_vol = tp_volume.groupby(dates).cumsum()
        cum_vol = df['volume'].groupby(dates).cumsum()
        vwap = cum_tp_vol / cum_vol.replace(0, np.nan)
        return vwap

    # ==========================================
    # CÁLCULOS ESTRATEGIA 2: APERTURA (1m / 5m)
    # ==========================================
    def analyze_strategy_2_apertura(self):
        """
        Analiza Estrategia 2:
        1) Vela de 1 min de apertura de mercado a las 10:30 hs ART.
        2) Tendencia para velas de 5 min.
        3) Extensión y retroceso de Fibonacci para velas de 5 min.
        """
        ba_tz = timezone(timedelta(hours=-3))
        now_ba = datetime.now(ba_tz)

        # 1. Obtener klines de 1 min para buscar la vela de apertura de 10:30 hs
        df_1m = self.fetch_klines(timeframe='1m', limit=300)
        df_5m = self.fetch_klines(timeframe='5m', limit=100)

        if df_1m is None or df_5m is None or len(df_1m) == 0 or len(df_5m) == 0:
            return {
                'vela_apertura_str': "ESPERANDO VELA 10:30",
                'trend': "NEUTRAL",
                'signal': None,
                'fib_s1': 0.0,
                'fib_r1': 0.0
            }

        # Convertir timestamps a zona horaria de Buenos Aires
        df_1m['timestamp_ba'] = df_1m['timestamp'].dt.tz_localize('UTC').dt.tz_convert(ba_tz)
        df_5m['timestamp_ba'] = df_5m['timestamp'].dt.tz_localize('UTC').dt.tz_convert(ba_tz)

        # Filtrar la vela de apertura de las 10:30 hs del día actual
        today_date = now_ba.date()
        open_candles = df_1m[
            (df_1m['timestamp_ba'].dt.date == today_date) & 
            (df_1m['timestamp_ba'].dt.hour == 10) & 
            (df_1m['timestamp_ba'].dt.minute == 30)
        ]

        if len(open_candles) > 0:
            c_open = open_candles['open'].iloc[0]
            c_close = open_candles['close'].iloc[0]
            if c_close < c_open:
                vela_apertura_str = "ROJA (10:30 1m)"
                vela_color = "ROJA"
            else:
                vela_apertura_str = "VERDE (10:30 1m)"
                vela_color = "VERDE"
        else:
            vela_apertura_str = "ESPERANDO VELA 10:30"
            vela_color = "NINGUNA"

        # 2. Tendencia para velas de 5 min (EMA 20 vs EMA 50)
        df_5m['ema20'] = df_5m['close'].ewm(span=20, adjust=False).mean()
        df_5m['ema50'] = df_5m['close'].ewm(span=50, adjust=False).mean()
        
        last_ema20 = df_5m['ema20'].iloc[-1]
        last_ema50 = df_5m['ema50'].iloc[-1]
        trend = "ALCISTA" if last_ema20 >= last_ema50 else "BAJISTA"

        # 3. Retroceso y Extensión de Fibonacci en velas de 5 min (Swing High/Low últimos 50 periodos)
        swing_high = df_5m['high'].tail(50).max()
        swing_low = df_5m['low'].tail(50).min()
        rng = swing_high - swing_low

        if rng > 0:
            if trend == "ALCISTA":
                # Primer soporte: Retroceso 38.2% desde el alto
                fib_s1 = swing_high - (0.382 * rng)
                # Primera resistencia: Extensión 127.2% desde el bajo
                fib_r1 = swing_low + (1.272 * rng)
            else:
                # Primer soporte: Extensión 127.2% desde el alto
                fib_s1 = swing_high - (1.272 * rng)
                # Primera resistencia: Retroceso 38.2% desde el bajo
                fib_r1 = swing_low + (0.382 * rng)
        else:
            fib_s1 = swing_low
            fib_r1 = swing_high

        # 4. Reglas de Entrada Estrategia 2:
        # Long: Vela de apertura es ROJA -> Entrada en primer soporte según Fib
        # Short: Vela de apertura es VERDE -> Entrada en primera resistencia según Fib
        curr_price = df_1m['close'].iloc[-1]
        signal = None

        if vela_color == "ROJA":
            if curr_price <= fib_s1 * 1.0005:  # Próximo o tocando 1er soporte
                signal = "LONG"
        elif vela_color == "VERDE":
            if curr_price >= fib_r1 * 0.9995:  # Próximo o tocando 1ra resistencia
                signal = "SHORT"

        return {
            'vela_apertura_str': vela_apertura_str,
            'trend': trend,
            'signal': signal,
            'fib_s1': fib_s1,
            'fib_r1': fib_r1
        }

    # ==========================================
    # GESTIÓN DE POSICIONES Y METRICAS
    # ==========================================
    def calculate_tp_sl(self, side, entry_price):
        """Calcula TP y SL para ganar/perder 3% respecto al monto de la operación (ROI 3%)."""
        price_tp_pct = (self.tp_roi_pct / 100.0) / self.leverage
        price_sl_pct = (self.sl_roi_pct / 100.0) / self.leverage

        if side == 'LONG':
            tp = entry_price * (1.0 + price_tp_pct)
            sl = entry_price * (1.0 - price_sl_pct)
        else:
            tp = entry_price * (1.0 - price_tp_pct)
            sl = entry_price * (1.0 + price_sl_pct)

        return self._format_price(tp), self._format_price(sl)

    def get_active_position(self):
        """Consulta la posición actualmente abierta en Binance o memoria."""
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

    def open_position(self, strategy_name, side, current_price):
        """Abre posición LONG o SHORT con TP y SL configurados (3% ROI)."""
        notional_val = self.margin_usdt * self.leverage
        qty = self._format_quantity(notional_val / current_price)
        tp_price, sl_price = self.calculate_tp_sl(side, current_price)

        logging.info(f"ENTRADA [{strategy_name}]: {side} a ${current_price:.2f} | TP: ${tp_price} | SL: ${sl_price}")

        if self.dry_run:
            self.current_position = side
            self.position_strategy = strategy_name
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.position_start_time = int(time.time() * 1000)
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

            # Orden de Mercado
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
                tp_price, sl_price = self.calculate_tp_sl(side, current_price)

            exit_side = 'SELL' if side == 'LONG' else 'BUY'

            # Take Profit (3% ROI)
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

            # Stop Loss (3% ROI)
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
            self.position_strategy = strategy_name
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.position_start_time = int(time.time() * 1000)
            self.entry_time = datetime.now()
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            return True

        except Exception as e:
            logging.error(f"Error abriendo posición real en Binance: {e}")
            return False

    def _record_trade_result(self, pnl):
        if pnl > 0:
            self.winning_trades += 1
            self.money_won += pnl
        elif pnl < 0:
            self.losing_trades += 1
            self.money_lost += abs(pnl)

    def _save_trade_to_file(self, strategy_name, exit_type, max_gain_pct, max_loss_pct, dur_mins, exit_time=None):
        """
        DETALLES 2: Guardar en tp.txt o sl.txt con los datos:
        estrategia, hora, % ganancia maximo, % perdida maximo, duracion de la operacion
        """
        filename = "tp.txt" if exit_type.lower() == "tp" else "sl.txt"
        if exit_time is None:
            exit_time = datetime.now()
        hora_str = exit_time.strftime('%H:%M:%S')

        strat_label = strategy_name if strategy_name else "Estrategia General"
        
        try:
            line = f"{strat_label}, {hora_str}, % Ganancia Máximo: +{max_gain_pct:.2f}%, % Pérdida Máximo: {max_loss_pct:.2f}%, Duración: {dur_mins:.1f}m\n"
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line)
            logging.info(f"Registro guardado en {filename}: {line.strip()}")
        except Exception as e:
            logging.error(f"Error escribiendo en {filename}: {e}")

    def check_simulated_exit(self, current_price):
        """Evalúa salidas de TP y SL en modo simulación."""
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
                pnl = self.margin_usdt * (self.tp_roi_pct / 100.0)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                self._save_trade_to_file(self.position_strategy, "tp", self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
            elif hit_sl:
                pnl = -self.margin_usdt * (self.sl_roi_pct / 100.0)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                self._save_trade_to_file(self.position_strategy, "sl", self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)

            self.current_position = None
            self.position_strategy = None
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0

    def is_within_trading_hours(self):
        """Verifica si la hora actual en Buenos Aires está dentro de 10:30 a 17:00 hs."""
        if not self.enable_schedule:
            return True, "Filtro desactivado"

        ba_tz = timezone(timedelta(hours=-3))
        now_ba = datetime.now(ba_tz)

        if self.schedule_weekdays_only and now_ba.weekday() >= 5:
            return False, "Fin de semana"

        try:
            sh, sm = map(int, self.schedule_start_str.split(':'))
            eh, em = map(int, self.schedule_end_str.split(':'))
            start_time = dtime(sh, sm)
            end_time = dtime(eh, em)
        except Exception:
            return True, "Formato horario inválido"

        if start_time <= now_ba.time() < end_time:
            return True, f"Horario Bolsa ({self.schedule_start_str}-{self.schedule_end_str} ART)"
        else:
            return False, f"Fuera de Horario ({self.schedule_start_str}-{self.schedule_end_str} ART)"

    def render_screen(self, current_price, s1_data, s2_data, active_pos, entry, qty, pnl_pct, dur_mins, is_within_hours, schedule_reason):
        """
        DETALLES 3:
        - Mantener cabecera siempre visible en pantalla.
        - Mantener visible en pantalla unicamente el estado actual.
        - No utilizar colores en todo el texto visualizado en pantalla.
        - Borrar pantalla antes de actualizar la visualizacion de datos.
        """
        # 1. Borrar pantalla de consola antes de actualizar
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
        horario_badge = "[ABIERTO]" if is_within_hours else f"[CERRADO: {schedule_reason}]"

        if active_pos:
            dur_str = f" (Duración: {dur_mins:.1f}m)"
            pnl_sign = "+" if pnl_pct >= 0 else ""
            pos_str = f"{active_pos} @ {entry:.2f} ({pnl_sign}{pnl_pct:.2f}%){dur_str} [{self.position_strategy}]"
        else:
            pos_str = "SIN POSICIÓN"

        # Construcción de la pantalla fija (Sin Colores ANSI)
        lines = []
        lines.append("=" * 70)
        lines.append("   BOT DE TRADING AUTOMATICO BINANCE - MONITOREO DE ESTRATEGIAS")
        lines.append("=" * 70)
        lines.append(f"Símbolo: {self.symbol} | Modo: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt:.2f} USDT")
        lines.append(f"Take Profit: +{self.tp_roi_pct}% ROI | Stop Loss: -{self.sl_roi_pct}% ROI")
        lines.append(f"Filtro Horario: {self.schedule_start_str} a {self.schedule_end_str} hs (Buenos Aires)")
        lines.append(f"Modo Dry-Run (Simulación): {self.dry_run} | Testnet: {self.use_testnet}")
        lines.append("-" * 70)
        lines.append("SALDO EN CUENTA:")
        if has_keys:
            lines.append(f"   Wallet: {wallet_bal:.2f} USDT | Disponible: {avail_bal:.2f} USDT | PnL No Realizado: {unrealized:.2f} USDT")
        elif self.dry_run:
            lines.append(f"   Wallet (Simulado): {wallet_bal:.2f} USDT")
        else:
            lines.append("   No disponible (Faltan API Keys en envprivado)")
        lines.append("-" * 70)
        lines.append("RESUMEN DE OPERACIONES:")
        lines.append(f"Tiempo Total: {uptime_hours:.2f}h | Ganadas: {self.winning_trades} (+{self.money_won:.2f} USDT) | Perdidas: {self.losing_trades} (-{self.money_lost:.2f} USDT)")
        lines.append("=" * 70)
        lines.append("ESTADO ACTUAL:")

        # Formato Estado Actual Estrategia 1 (Punto 9 Estrategia 1):
        # 1: nombre de estrategia
        # 2: precio, rsi, oracle y vwap
        # 3: divergencia rsi, señal confluencia
        # 4: horario
        # 5: posicion
        if self.active_strategy_cfg in ("1", "ALL"):
            lines.append("Estrategia 1: Divergencia RSI + Oracle + VWAP (3m)")
            lines.append(f"Precio: ${current_price:.2f} | RSI: {s1_data['rsi']:.1f} | Oracle: {s1_data['oracle_val']:.1f} ({s1_data['oracle_sig']}) | VWAP: ${s1_data['vwap']:.2f}")
            lines.append(f"Divergencia RSI: {s1_data['rsi_div']} | Señal Confluencia: {s1_data['signal'] if s1_data['signal'] else 'ESPERANDO'}")
            lines.append(f"Horario: {horario_badge}")
            lines.append(f"Posición: {pos_str if self.position_strategy == 'Estrategia 1: Divergencia RSI + Oracle + VWAP (3m)' else 'SIN POSICIÓN'}")
            lines.append("-" * 70)

        # Formato Estado Actual Estrategia 2 (Punto 8 Estrategia 2):
        # 1: nombre de estrategia
        # 2: precio, vela apertura
        # 3: horario
        # 4: posicion
        if self.active_strategy_cfg in ("2", "ALL"):
            lines.append("Estrategia 2: Apertura Market Open (1m / 5m)")
            lines.append(f"Precio: ${current_price:.2f} | Vela Apertura: {s2_data['vela_apertura_str']}")
            lines.append(f"Horario: {horario_badge}")
            lines.append(f"Posición: {pos_str if self.position_strategy == 'Estrategia 2: Apertura Market Open' else 'SIN POSICIÓN'}")
            lines.append("-" * 70)

        lines.append("=" * 70)

        sys.stdout.write("\033[H\033[J" + "\n".join(lines) + "\n")
        sys.stdout.flush()

    def run(self):
        """Bucle principal de ejecución del bot."""
        logging.info("Iniciando monitoreo multiestrategia...")

        while True:
            try:
                # 1. Datos para Estrategia 1 (3m klines)
                df_3m = self.fetch_klines(timeframe=self.timeframe_strat1, limit=300)
                if df_3m is None or len(df_3m) == 0:
                    time.sleep(10)
                    continue

                current_price = df_3m['close'].iloc[-1]

                # Análisis Estrategia 1
                rsi_div, df_rsi = self.detect_rsi_divergences(df_3m)
                current_rsi = df_rsi['rsi'].iloc[-1] if 'rsi' in df_rsi else 50.0
                oracle_status, oracle_val, _ = self.calculate_oracle_oscillator(df_3m)
                vwap_series = self.calculate_vwap(df_3m)
                current_vwap = vwap_series.iloc[-1] if len(vwap_series) > 0 and not pd.isna(vwap_series.iloc[-1]) else current_price

                sig_oracle_str = "BULL" if oracle_status == 'ORACLE_BULL' else ("BEAR" if oracle_status == 'ORACLE_BEAR' else "NEUT")

                s1_signal = None
                if rsi_div == 'BULL_DIV' and oracle_status == 'ORACLE_BULL' and current_price > current_vwap:
                    s1_signal = 'LONG'
                elif rsi_div == 'BEAR_DIV' and oracle_status == 'ORACLE_BEAR' and current_price < current_vwap:
                    s1_signal = 'SHORT'

                s1_data = {
                    'rsi': current_rsi,
                    'oracle_val': oracle_val,
                    'oracle_sig': sig_oracle_str,
                    'vwap': current_vwap,
                    'rsi_div': rsi_div,
                    'signal': s1_signal
                }

                # 2. Datos para Estrategia 2 (Apertura)
                s2_data = self.analyze_strategy_2_apertura()

                # 3. Estado de posición actual
                active_pos, entry, qty = self.get_active_position()

                if self.dry_run and active_pos:
                    self.check_simulated_exit(current_price)
                    active_pos, entry, qty = self.get_active_position()
                elif not self.dry_run and active_pos is None and self.current_position:
                    # Cierre real en Binance por TP/SL
                    exit_time = datetime.now()
                    dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

                    try:
                        self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                        self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": self.symbol})
                    except Exception:
                        pass

                    pnl = (current_price - self.entry_price) * self.position_qty if self.current_position == 'LONG' else (self.entry_price - current_price) * self.position_qty
                    target_file = "tp" if pnl >= 0 else "sl"
                    self._save_trade_to_file(self.position_strategy, target_file, self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
                    self._record_trade_result(pnl)

                    self.current_position = None
                    self.position_strategy = None
                    self.entry_time = None
                    self.max_pnl_pct = 0.0
                    self.min_pnl_pct = 0.0

                pnl_pct = 0.0
                dur_mins = 0.0
                if active_pos and entry > 0:
                    dur_mins = (datetime.now() - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0
                    if active_pos == 'LONG':
                        pnl_pct = ((current_price - entry) / entry) * self.leverage * 100.0
                    else:
                        pnl_pct = ((entry - current_price) / entry) * self.leverage * 100.0

                    if pnl_pct > self.max_pnl_pct:
                        self.max_pnl_pct = pnl_pct
                    if pnl_pct < self.min_pnl_pct:
                        self.min_pnl_pct = pnl_pct

                is_within_hours, schedule_reason = self.is_within_trading_hours()

                # Renderizar pantalla monocroma con cabecera y estado actual
                self.render_screen(
                    current_price=current_price,
                    s1_data=s1_data,
                    s2_data=s2_data,
                    active_pos=active_pos,
                    entry=entry,
                    qty=qty,
                    pnl_pct=pnl_pct,
                    dur_mins=dur_mins,
                    is_within_hours=is_within_hours,
                    schedule_reason=schedule_reason
                )

                # 4. Abrir Posición si se cumplen condiciones y no hay posición activa
                if active_pos is None and is_within_hours:
                    if self.active_strategy_cfg in ("1", "ALL") and s1_signal:
                        self.open_position("Estrategia 1: Divergencia RSI + Oracle + VWAP (3m)", s1_signal, current_price)
                    elif self.active_strategy_cfg in ("2", "ALL") and s2_data['signal']:
                        self.open_position("Estrategia 2: Apertura Market Open", s2_data['signal'], current_price)

                time.sleep(10)

            except KeyboardInterrupt:
                print("\n[!] Bot detenido manualmente por el usuario. Exiting...")
                break
            except Exception as e:
                logging.error(f"Excepción en bucle principal: {e}")
                time.sleep(10)


if __name__ == "__main__":
    bot = BinanceMultiStrategyBot()
    bot.run()
