#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de Trading Automático en Binance Futures (USDT-M)
Estrategia: Confluencia Divergencias RSI + Oscilador Oracle + VWAP en Velas de 15 Minutos
Modo: Aislado (Isolated) | Apalancamiento: 10x | Margen: 50 USDT
Take Profit: +10% ROI (sobre el monto de la operación)
Stop Loss: -10% ROI (sobre el monto de la operación)
Horario de Operaciones: 10:30 a 17:00 hs (Horario Buenos Aires)
"""

import os
import sys
import time
import math
import logging
import re
import shutil
from datetime import datetime, timezone, timedelta, time as dtime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Configuración de encoding para consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Configuración de Logging (a archivo para no ensuciar la pantalla de consola)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)

# Cargar variables de entorno
load_dotenv()

# Evitar que el nombre de archivo binance.py oculte la biblioteca python-binance
current_dir = sys.path.pop(0) if sys.path and sys.path[0] in ('', os.getcwd(), os.path.dirname(__file__)) else None
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
finally:
    if current_dir is not None:
        sys.path.insert(0, current_dir)


class BinanceRsiDivergenceBot:
    def __init__(self):
        # Cargar parámetros desde variables de entorno o valores por defecto
        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        self.symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
        self.margin_usdt = float(os.getenv("MARGIN_USDT", "50.0"))
        self.leverage = int(os.getenv("LEVERAGE", "10"))
        self.timeframe = os.getenv("TIMEFRAME", "15m")
        self.tp_roi_pct = float(os.getenv("TP_ROI_PCT", "10.0"))
        self.sl_roi_pct = float(os.getenv("SL_ROI_PCT", "10.0"))
        self.rsi_period = int(os.getenv("RSI_PERIOD", "14"))
        self.pivot_left = int(os.getenv("PIVOT_LOOKBACK_LEFT", "5"))
        self.pivot_right = int(os.getenv("PIVOT_LOOKBACK_RIGHT", "2"))
        
        self.dry_run = os.getenv("DRY_RUN", "True").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Configuración de Filtro de Horario de Trading (Bolsa EE.UU. 10:30 - 17:00 hs Buenos Aires)
        self.enable_schedule = os.getenv("ENABLE_SCHEDULE_FILTER", "True").lower() in ("true", "1", "yes")
        self.schedule_start_str = os.getenv("SCHEDULE_START", "10:30").strip()
        self.schedule_end_str = os.getenv("SCHEDULE_END", "17:00").strip()
        self.schedule_weekdays_only = os.getenv("SCHEDULE_WEEKDAYS_ONLY", "True").lower() in ("true", "1", "yes")

        # Variables de estado interno
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001
        self.last_lines_printed = 1
        
        # Estado de posición (para Dry-Run y tracking)
        self.current_position = None  # None, 'LONG', 'SHORT'
        self.entry_price = 0.0
        self.position_qty = 0.0
        self.tp_price = 0.0
        self.sl_price = 0.0
        self.last_signal_time = None
        self.simulated_balance = 100.0  # Para modo simulación

        # Estadísticas de operaciones y tiempo de inicio
        self.bot_start_time = time.time()
        self.winning_trades = 0
        self.losing_trades = 0
        self.money_won = 0.0
        self.money_lost = 0.0
        self.position_start_time = 0
        self.entry_time = None

        # Seguimiento de % ganancia y % pérdida máximo durante la operación
        self.max_pnl_pct = 0.0
        self.min_pnl_pct = 0.0

        self._initialize_client()

    def _initialize_client(self):
        """Inicializa el cliente Binance API y obtiene precisión del símbolo."""
        logging.info("Inicializando Bot de Trading Binance...")
        logging.info(f"Símbolo: {self.symbol} | Margen: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt} USDT")
        logging.info(f"TP: +{self.tp_roi_pct}% ROI | SL: -{self.sl_roi_pct}% ROI | Dry-Run: {self.dry_run}")

        try:
            if self.api_key and self.api_secret:
                if self.use_testnet:
                    self.client = Client(self.api_key, self.api_secret, testnet=True)
                else:
                    self.client = Client(self.api_key, self.api_secret)
            else:
                # Cliente público sin autenticación para consultar klines y datos de mercado
                self.client = Client("", "")

            # Probar conexión pública y obtener reglas del símbolo
            self._update_symbol_precision()
            
            if not self.dry_run and self.api_key and self.api_secret:
                self._setup_futures_account()
                logging.info("Conexión autenticada exitosamente con Binance Futures API.")
            else:
                logging.info("Conexión de mercado iniciada correctamente.")

            # Cerrar cualquier posición abierta previa al iniciar
            self.close_existing_positions()

        except Exception as e:
            logging.error(f"Error conectando a Binance API: {e}")
            if not self.dry_run:
                logging.info("Cambiando automáticamente a modo DRY-RUN.")
                self.dry_run = True

    def get_account_balance(self):
        """Obtiene el saldo disponible, total y PnL REAL de la cuenta Binance Futuros en USDT."""
        if self.client and self.api_key and self.api_secret:
            try:
                account_info = self.client.futures_account()
                total_wallet = float(account_info.get('totalWalletBalance', 0.0))
                available = float(account_info.get('availableBalance', 0.0))
                unrealized = float(account_info.get('totalUnrealizedProfit', 0.0))
                return {
                    'wallet_balance': total_wallet,
                    'available_balance': available,
                    'unrealized_pnl': unrealized,
                    'has_keys': True
                }
            except Exception as e:
                logging.error(f"Error al consultar saldo real en Binance API: {e}")
                return {
                    'wallet_balance': 0.0,
                    'available_balance': 0.0,
                    'unrealized_pnl': 0.0,
                    'has_keys': True
                }

        # Si no hay API Keys configuradas
        return {
            'wallet_balance': 0.0,
            'available_balance': 0.0,
            'unrealized_pnl': 0.0,
            'has_keys': False
        }

    def _update_symbol_precision(self):
        """Obtiene la precisión decimal de precio y cantidad para el par elegido."""
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
            logging.info(f"Filtros de {self.symbol} -> TickSize: {self.tick_size} ({self.price_precision} dec), StepSize: {self.step_size} ({self.qty_precision} dec)")
        except Exception as e:
            logging.warning(f"No se pudieron obtener precisiones dinámicas: {e}. Usando valores predeterminados.")

    @staticmethod
    def _precision_from_step(step_str):
        """Calcula los decimales a partir de un valor flotante en cadena."""
        step = float(step_str)
        if step >= 1:
            return 0
        return int(round(-math.log10(step)))

    def _format_price(self, price):
        """Redondea el precio al tickSize permitido por Binance."""
        return round(round(price / self.tick_size) * self.tick_size, self.price_precision)

    def _format_quantity(self, qty):
        """Redondea la cantidad al stepSize permitido por Binance."""
        rounded = round(round(qty / self.step_size) * self.step_size, self.qty_precision)
        return max(rounded, self.min_qty)

    def _setup_futures_account(self):
        """Configura el apalancamiento 10x y el modo de margen Aislado (ISOLATED)."""
        try:
            # Configurar Modo Aislado
            try:
                self.client.futures_change_margin_type(symbol=self.symbol, marginType='ISOLATED')
                logging.info(f"Margen cambiado a ISOLATED para {self.symbol}.")
            except BinanceAPIException as e:
                if e.code == -4046 or "No need to change" in str(e):
                    pass
                else:
                    logging.warning(f"Nota sobre margen: {e.message}")

            # Configurar Apalancamiento 10x
            self.client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            logging.info(f"Apalancamiento configurado a {self.leverage}x para {self.symbol}.")

        except Exception as e:
            logging.error(f"Error al configurar cuenta de futuros: {e}")

    def close_existing_positions(self):
        """
        Cierra cualquier posición abierta previa al iniciar el bot y cancela órdenes pendientes.
        """
        logging.info("Verificando y cerrando posiciones abiertas al iniciar el bot...")
        if self.dry_run:
            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            logging.info("Modo Simulación (DRY-RUN): Posición inicial restablecida a SIN POSICIÓN.")
            return

        if not self.client or not self.api_key or not self.api_secret:
            logging.info("Sin API Keys configuradas. Omitiendo cierre de posiciones previas.")
            return

        try:
            # 1. Cancelar órdenes previas (Normales y Algo)
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            except Exception as e:
                logging.warning(f"Nota cancelando órdenes previas: {e}")
            try:
                self.futures_cancel_all_algo_orders(symbol=self.symbol)
            except Exception as e:
                logging.warning(f"Nota cancelando órdenes algo previas: {e}")

            # 2. Consultar posición activa
            positions = self.client.futures_position_information(symbol=self.symbol)
            closed_any = False
            for pos in positions:
                amt = float(pos['positionAmt'])
                if amt != 0:
                    side_to_close = 'SELL' if amt > 0 else 'BUY'
                    qty = self._format_quantity(abs(amt))
                    pos_type = 'LONG' if amt > 0 else 'SHORT'
                    logging.info(f"Posición previa detectada en Binance: {pos_type} de {qty} {self.symbol}. Cerrando a MARKET...")
                    
                    close_order = self.client.futures_create_order(
                        symbol=self.symbol,
                        side=side_to_close,
                        type='MARKET',
                        quantity=qty,
                        reduceOnly=True
                    )
                    logging.info(f"Posición {pos_type} previa cerrada exitosamente a MARKET. Order ID: {close_order.get('orderId')}")
                    closed_any = True

            if not closed_any:
                logging.info(f"Sin posiciones abiertas previas para {self.symbol} en Binance Futuros.")

            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0

        except Exception as e:
            logging.error(f"Error cerrando posiciones abiertas al iniciar: {e}")

    def futures_create_algo_order(self, **params):
        """
        Envía una orden algorítmica/condicional (STOP_MARKET, TAKE_PROFIT_MARKET, etc.)
        utilizando el endpoint POST /fapi/v1/algoOrder de Binance Futures.
        """
        return self.client._request_futures_api("post", "algoOrder", signed=True, data=params)

    def futures_cancel_all_algo_orders(self, symbol):
        """
        Cancela todas las órdenes algorítmicas/condicionales abiertas para un símbolo
        utilizando el endpoint DELETE /fapi/v1/algoOpenOrders de Binance Futures.
        """
        return self.client._request_futures_api("delete", "algoOpenOrders", signed=True, data={"symbol": symbol})

    def fetch_klines(self, limit=200):
        """Obtiene las últimas velas de 1 minuto desde Binance."""
        try:
            klines = self.client.futures_klines(symbol=self.symbol, interval=self.timeframe, limit=limit)
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
            logging.error(f"Error al obtener klines de Binance: {e}")
            return None

    def calculate_rsi(self, df):
        """Calcula el RSI (14) con el método de suavizado Wilder's EMA (TradingView)."""
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        
        avg_gain = gain.ewm(alpha=1.0/self.rsi_period, min_periods=self.rsi_period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1.0/self.rsi_period, min_periods=self.rsi_period, adjust=False).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    def detect_rsi_divergences(self, df):
        """
        Detecta divergencias RSI en velas completadas.
        Retorna: 'BULL_DIV', 'BEAR_DIV', o None.
        """
        df = df.copy()
        df['rsi'] = self.calculate_rsi(df)
        
        n = len(df)
        if n < self.rsi_period + self.pivot_left + self.pivot_right + 10:
            return None, df

        # Identificar Pivotes en Precio y RSI
        pivot_lows = []
        pivot_highs = []

        # Analizamos hasta n - 1 (para asegurar que la vela actual o en formación no dé falsos pivotes)
        end_idx = n - 1
        
        for i in range(self.pivot_left, end_idx - self.pivot_right):
            # Pivote Mínimo (Pivot Low)
            window_low = df['low'].iloc[i - self.pivot_left : i + self.pivot_right + 1]
            window_rsi = df['rsi'].iloc[i - self.pivot_left : i + self.pivot_right + 1]
            
            if df['low'].iloc[i] == window_low.min():
                pivot_lows.append({'index': i, 'price': df['low'].iloc[i], 'rsi': df['rsi'].iloc[i], 'time': df['timestamp'].iloc[i]})
            
            # Pivote Máximo (Pivot High)
            window_high = df['high'].iloc[i - self.pivot_left : i + self.pivot_right + 1]
            if df['high'].iloc[i] == window_high.max():
                pivot_highs.append({'index': i, 'price': df['high'].iloc[i], 'rsi': df['rsi'].iloc[i], 'time': df['timestamp'].iloc[i]})

        signal = None

        # 1. Verificar Bullish Divergence (LONG)
        if len(pivot_lows) >= 2:
            p1 = pivot_lows[-2]  # Pivote anterior
            p2 = pivot_lows[-1]  # Pivote más reciente
            
            # Si el pivote más reciente está dentro de los últimos (pivot_right + 5) periodos
            if (end_idx - p2['index']) <= (self.pivot_right + 5):
                # Precio hace Mínimo más Bajo (o Igual) Y RSI hace Mínimo más Alto
                if p2['price'] <= p1['price'] and p2['rsi'] > p1['rsi']:
                    signal = 'BULL_DIV'

        # 2. Verificar Bearish Divergence (SHORT)
        if len(pivot_highs) >= 2:
            p1 = pivot_highs[-2]  # Pivote anterior
            p2 = pivot_highs[-1]  # Pivote más reciente
            
            if (end_idx - p2['index']) <= (self.pivot_right + 5):
                # Precio hace Máximo más Alto (o Igual) Y RSI hace Máximo más Bajo
                if p2['price'] >= p1['price'] and p2['rsi'] < p1['rsi']:
                    signal = 'BEAR_DIV'

        return signal, df

    def calculate_oracle_oscillator(self, df):
        """
        Calcula el Oscilador Oracle (Ponderado: Stoch K + RSI + Williams %R) y su línea de señal EMA.
        Retorna: 'ORACLE_BULL', 'ORACLE_BEAR', o 'ORACLE_NEUTRAL'.
        """
        df = df.copy()
        period = 14
        if len(df) < period + 10:
            return 'ORACLE_NEUTRAL', 50.0, 50.0

        # 1. Stochastic %K (14)
        low_min = df['low'].rolling(window=period).min()
        high_max = df['high'].rolling(window=period).max()
        denom = (high_max - low_min).replace(0, np.nan)
        stoch_k = ((df['close'] - low_min) / denom) * 100.0
        stoch_k = stoch_k.fillna(50.0)

        # 2. RSI (14)
        rsi = self.calculate_rsi(df).fillna(50.0)

        # 3. Williams %R Normalizado [0, 100]
        williams_r = ((high_max - df['close']) / denom) * 100.0
        williams_norm = 100.0 - williams_r.fillna(50.0)

        # 4. Línea Principal del Oscilador Oracle
        oracle_line = (stoch_k * 0.35) + (rsi * 0.35) + (williams_norm * 0.30)

        # 5. Línea de Señal (EMA de 9 periodos de la Línea Oracle)
        signal_line = oracle_line.ewm(span=9, adjust=False).mean()

        last_oracle = oracle_line.iloc[-1]
        last_signal = signal_line.iloc[-1]

        if last_oracle > last_signal:
            oracle_status = 'ORACLE_BULL'
        elif last_oracle < last_signal:
            oracle_status = 'ORACLE_BEAR'
        else:
            oracle_status = 'ORACLE_NEUTRAL'

        return oracle_status, last_oracle, last_signal

    def calculate_vwap(self, df):
        """
        Calcula el VWAP (Volume Weighted Average Price) acumulado intradía.
        Resetea el acumulado diariamente a las 00:00 UTC (estándar TradingView).
        """
        df = df.copy()
        typical_price = (df['high'] + df['low'] + df['close']) / 3.0
        tp_volume = typical_price * df['volume']

        dates = df['timestamp'].dt.date
        cum_tp_vol = tp_volume.groupby(dates).cumsum()
        cum_vol = df['volume'].groupby(dates).cumsum()

        vwap = cum_tp_vol / cum_vol.replace(0, np.nan)
        return vwap

    def calculate_tp_sl(self, side, entry_price):
        """
        Calcula precios exactos de Take Profit (+10% ROI) y Stop Loss (-10% ROI).
        Apalancamiento 10x:
        +10% ROI = +1.0% de variación de precio
        -10% ROI = -1.0% de variación de precio
        """
        price_tp_pct = (self.tp_roi_pct / 100.0) / self.leverage
        price_sl_pct = (self.sl_roi_pct / 100.0) / self.leverage

        if side == 'LONG':
            tp = entry_price * (1.0 + price_tp_pct)
            sl = entry_price * (1.0 - price_sl_pct)
        else:  # SHORT
            tp = entry_price * (1.0 - price_tp_pct)
            sl = entry_price * (1.0 + price_sl_pct)

        return self._format_price(tp), self._format_price(sl)

    def get_active_position(self):
        """Consulta la posición activa actual en Binance Futures (o en memoria si Dry-Run)."""
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
            logging.error(f"Error al consultar posiciones activas en Binance: {e}")
            return self.current_position, self.entry_price, self.position_qty

    def open_position(self, side, current_price):
        """Abre posición LONG o SHORT en Binance Futures con órdenes TP y SL."""
        notional_val = self.margin_usdt * self.leverage
        raw_qty = notional_val / current_price
        qty = self._format_quantity(raw_qty)

        tp_price, sl_price = self.calculate_tp_sl(side, current_price)

        logging.info(f"SEÑAL ENCONTRADA: Entrada {side} detectada en {current_price}")
        logging.info(f"Margen: {self.margin_usdt} USDT | Apalancamiento: {self.leverage}x | Cantidad: {qty} {self.symbol}")
        logging.info(f"Take Profit: {tp_price} | Stop Loss: {sl_price}")

        if self.dry_run:
            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.position_start_time = int(time.time() * 1000)
            self.entry_time = datetime.now()
            self.max_pnl_pct = 0.0
            self.min_pnl_pct = 0.0
            logging.info(f"[SIMULACIÓN] Posición {side} abierta exitosamente a {current_price}")
            return True

        # Ejecución Real en Binance Futures
        try:
            # 0. Cancelar órdenes previas antes de enviar una nueva orden
            try:
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            except Exception as e:
                logging.warning(f"Nota: No se pudieron cancelar órdenes previas: {e}")
            try:
                self.futures_cancel_all_algo_orders(symbol=self.symbol)
            except Exception as e:
                logging.warning(f"Nota: No se pudieron cancelar órdenes algo previas: {e}")

            # 1. Enviar Orden Market de Entrada
            order_side = 'BUY' if side == 'LONG' else 'SELL'
            market_order = self.client.futures_create_order(
                symbol=self.symbol,
                side=order_side,
                type='MARKET',
                quantity=qty
            )
            market_id = market_order.get('orderId')

            # Obtener precio promedio de ejecución real
            time.sleep(1)
            active_side, real_entry, real_qty = self.get_active_position()
            if real_entry > 0:
                current_price = real_entry
                tp_price, sl_price = self.calculate_tp_sl(side, current_price)

            # 2. Orden de Take Profit (TAKE_PROFIT_MARKET via Algo API)
            exit_side = 'SELL' if side == 'LONG' else 'BUY'
            try:
                self.futures_create_algo_order(
                    algoType='CONDITIONAL',
                    symbol=self.symbol,
                    side=exit_side,
                    type='TAKE_PROFIT_MARKET',
                    triggerPrice=str(tp_price),
                    closePosition='true'
                )
            except Exception as e:
                logging.error(f"Error colocando Take Profit algo: {e}")

            # 3. Orden de Stop Loss (STOP_MARKET via Algo API)
            try:
                self.futures_create_algo_order(
                    algoType='CONDITIONAL',
                    symbol=self.symbol,
                    side=exit_side,
                    type='STOP_MARKET',
                    triggerPrice=str(sl_price),
                    closePosition='true'
                )
            except Exception as e:
                logging.error(f"Error colocando Stop Loss algo: {e}")

            logging.info(f"Orden MARKET ID: {market_id} | Take Profit: {tp_price} | Stop Loss: {sl_price}")

            self.current_position = side
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
            logging.error(f"Error abriendo posición en Binance: {e}")
            return False

    def _record_trade_result(self, pnl):
        """Registra el resultado de una operación cerrada (ganada/perdida y PnL)."""
        if pnl > 0:
            self.winning_trades += 1
            self.money_won += pnl
        elif pnl < 0:
            self.losing_trades += 1
            self.money_lost += abs(pnl)

    def _save_trade_to_file(self, exit_type, max_gain_pct, max_loss_pct, dur_mins, exit_time=None):
        """
        Guarda los datos de la operación cerrada en tp.txt o sl.txt.
        Datos: hora, %ganancia maximo, % perdida maximo, duracion de la operacion
        """
        filename = "tp.txt" if exit_type.lower() == "tp" else "sl.txt"
        if exit_time is None:
            exit_time = datetime.now()
        hora_str = exit_time.strftime('%H:%M:%S')
        try:
            line = f"Hora: {hora_str}, % Ganancia Máximo: +{max_gain_pct:.2f}%, % Pérdida Máximo: {max_loss_pct:.2f}%, Duración: {dur_mins:.1f}m\n"
            with open(filename, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            logging.error(f"Error al guardar datos en {filename}: {e}")

    @staticmethod
    def _fit_to_terminal(text, max_cols):
        """Trunca la línea en texto plano para que no supere max_cols en pantalla."""
        if len(text) <= max_cols:
            return text
        max_vis = max(10, max_cols - 4)
        return text[:max_vis] + "..."

    def show_trade_stats(self):
        """Registra el resumen de estadísticas de operaciones en el log."""
        uptime_hours = (time.time() - self.bot_start_time) / 3600.0
        logging.info(
            f"RESUMEN: Tiempo Total: {uptime_hours:.2f}h | "
            f"Ganadas: {self.winning_trades} (+{self.money_won:.2f} USDT) | "
            f"Perdidas: {self.losing_trades} (-{self.money_lost:.2f} USDT)"
        )

    def render_screen(self, current_price, current_vwap, current_rsi, oracle_val, sig_orc_str, rsi_signal, combined_signal, active_pos, entry, qty, pnl_pct, dur_mins, is_within_hours, schedule_reason):
        """Limpia la pantalla antes de actualizar y muestra únicamente la cabecera fija con el estado actual (sin colores)."""
        # Borrar pantalla de consola antes de actualizar la visualización de datos
        os.system('cls' if os.name == 'nt' else 'clear')

        # 1. Saldo de cuenta
        if self.dry_run:
            wallet_bal = self.simulated_balance
            avail_bal = self.simulated_balance
            unrealized = 0.0
            has_keys = False
        else:
            bal = self.get_account_balance()
            wallet_bal = bal['wallet_balance']
            avail_bal = bal['available_balance']
            unrealized = bal['unrealized_pnl']
            has_keys = bal['has_keys']

        # 2. Uptime
        uptime_hours = (time.time() - self.bot_start_time) / 3600.0

        # 3. Formato del estado actual
        sig_rsi_str = rsi_signal if rsi_signal else "Sin Div"
        sig_comb_str = combined_signal if combined_signal else "ESPERANDO"

        if not is_within_hours:
            horario_badge = f"[CERRADO: {schedule_reason}]"
        else:
            horario_badge = "[ABIERTO]"

        if active_pos:
            dur_str = f" (Duración: {dur_mins:.1f}m)"
            if pnl_pct >= 0:
                pnl_str = f"+{pnl_pct:.2f}%"
            else:
                pnl_str = f"{pnl_pct:.2f}%"
            pos_str = f"{active_pos} @ {entry:.2f} ({pnl_str}){dur_str}"
        else:
            pos_str = "SIN POSICIÓN"

        cols = shutil.get_terminal_size(fallback=(160, 24)).columns

        lines = []
        lines.append("=" * 65)
        lines.append("   BOT DE TRADING BINANCE - DIVERGENCIA RSI + ORACLE + VWAP (15m)")
        lines.append("=" * 65)
        lines.append(f"Símbolo: {self.symbol} | Modo: AISLADO | Apalancamiento: {self.leverage}x | Monto: {self.margin_usdt:.2f} USDT")
        lines.append(f"Take Profit (TP): +{self.tp_roi_pct}% ROI | Stop Loss (SL): -{self.sl_roi_pct}% ROI")
        if self.enable_schedule:
            week_str = " (Lunes a Viernes)" if self.schedule_weekdays_only else ""
            lines.append(f"Filtro Horario: {self.schedule_start_str} a {self.schedule_end_str} hs (Buenos Aires){week_str}")
        else:
            lines.append("Filtro Horario: DESACTIVADO")
        lines.append(f"Modo Dry-Run (Simulación): {self.dry_run} | Testnet: {self.use_testnet}")
        lines.append("-" * 65)
        lines.append("SALDO EN CUENTA:")
        if has_keys:
            lines.append(f"   Wallet: {wallet_bal:.2f} USDT | Disponible: {avail_bal:.2f} USDT | PnL No Realizado: {unrealized:.2f} USDT")
        elif self.dry_run:
            lines.append(f"   Wallet (Simulado): {wallet_bal:.2f} USDT")
        else:
            lines.append("   No disponible (Faltan API Keys en .env)")
        lines.append("-" * 65)
        lines.append("RESUMEN DE OPERACIONES:")
        lines.append(f"Tiempo Total: {uptime_hours:.2f}h | Ganadas: {self.winning_trades} (+{self.money_won:.2f} USDT) | Perdidas: {self.losing_trades} (-{self.money_lost:.2f} USDT)")
        lines.append("=" * 65)
        lines.append("ESTADO ACTUAL:")
        
        status_line = (
            f"{self.symbol}: ${current_price:.2f} | "
            f"VWAP: ${current_vwap:.2f} | "
            f"RSI: {current_rsi:.1f} | "
            f"Oracle: {oracle_val:.1f} ({sig_orc_str}) | "
            f"Div: {sig_rsi_str} | "
            f"Señal: {sig_comb_str} | "
            f"Horario: {horario_badge} | "
            f"Estado: {pos_str}"
        )
        status_line = self._fit_to_terminal(status_line, cols)
        lines.append(status_line)
        lines.append("=" * 65)

        output_buffer = "\033[H\033[J" + "\n".join(lines) + "\n"
        sys.stdout.write(output_buffer)
        sys.stdout.flush()

    def _get_last_realized_pnl(self):
        """Obtiene el PnL realizado de la posición recién cerrada desde Binance API."""
        if not self.client or not self.api_key or not self.api_secret:
            return None
        try:
            trades = self.client.futures_account_trades(symbol=self.symbol, limit=10)
            if trades:
                total_pnl = 0.0
                found = False
                for trade in trades:
                    trade_time = float(trade.get('time', 0))
                    if self.position_start_time > 0 and trade_time >= (self.position_start_time - 5000):
                        pnl = float(trade.get('realizedPnl', 0.0))
                        if pnl != 0.0:
                            total_pnl += pnl
                            found = True
                if found:
                    return total_pnl
        except Exception as e:
            logging.warning(f"No se pudo consultar PnL de Binance API: {e}")
        return None

    def check_simulated_exit(self, current_price):
        """En modo Dry-Run, comprueba si la posición tocó el TP o SL."""
        if not self.dry_run or not self.current_position:
            return

        pos = self.current_position
        tp = self.tp_price
        sl = self.sl_price

        hit_tp = False
        hit_sl = False

        if pos == 'LONG':
            if current_price >= tp:
                hit_tp = True
            elif current_price <= sl:
                hit_sl = True
        elif pos == 'SHORT':
            if current_price <= tp:
                hit_tp = True
            elif current_price >= sl:
                hit_sl = True

        if hit_tp or hit_sl:
            exit_time = datetime.now()
            dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

            if self.entry_price > 0:
                if pos == 'LONG':
                    exit_pnl_pct = ((current_price - self.entry_price) / self.entry_price) * self.leverage * 100.0
                else:
                    exit_pnl_pct = ((self.entry_price - current_price) / self.entry_price) * self.leverage * 100.0
                if exit_pnl_pct > self.max_pnl_pct:
                    self.max_pnl_pct = exit_pnl_pct
                if exit_pnl_pct < self.min_pnl_pct:
                    self.min_pnl_pct = exit_pnl_pct

            if hit_tp:
                pnl = self.margin_usdt * (self.tp_roi_pct / 100.0)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                logging.info(f"TAKE PROFIT ALCANZADO: Posición {pos} cerrada a {current_price}. PnL: +{self.tp_roi_pct:.2f}% | Max Gain: +{self.max_pnl_pct:.2f}% | Max Loss: {self.min_pnl_pct:.2f}% | Duración: {dur_mins:.1f}m")
                self._save_trade_to_file("tp", self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
                self.current_position = None
                self.entry_time = None
                self.max_pnl_pct = 0.0
                self.min_pnl_pct = 0.0
                self.show_trade_stats()
            elif hit_sl:
                pnl = -self.margin_usdt * (self.sl_roi_pct / 100.0)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                logging.info(f"STOP LOSS ALCANZADO: Posición {pos} cerrada a {current_price}. PnL: -{self.sl_roi_pct:.2f}% | Max Gain: +{self.max_pnl_pct:.2f}% | Max Loss: {self.min_pnl_pct:.2f}% | Duración: {dur_mins:.1f}m")
                self._save_trade_to_file("sl", self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)
                self.current_position = None
                self.entry_time = None
                self.max_pnl_pct = 0.0
                self.min_pnl_pct = 0.0
                self.show_trade_stats()

    def is_within_trading_hours(self):
        """
        Verifica si la hora actual en Buenos Aires (UTC-3) está dentro del horario de operaciones permitido.
        Por defecto: 10:30 a 17:00 hs ART, de Lunes a Viernes.
        """
        if not self.enable_schedule:
            return True, "Filtro desactivado"

        # Zona horaria Buenos Aires (UTC-3 constante)
        ba_tz = timezone(timedelta(hours=-3))
        now_ba = datetime.now(ba_tz)

        # Verificar días hábiles (Lunes = 0, Viernes = 4, Sábado = 5, Domingo = 6)
        if self.schedule_weekdays_only and now_ba.weekday() >= 5:
            day_name = "Sábado" if now_ba.weekday() == 5 else "Domingo"
            return False, f"Fin de semana ({day_name})"

        try:
            sh, sm = map(int, self.schedule_start_str.split(':'))
            eh, em = map(int, self.schedule_end_str.split(':'))
            start_time = dtime(sh, sm)
            end_time = dtime(eh, em)
        except Exception as e:
            logging.error(f"Error parseando horario de trading ({self.schedule_start_str}-{self.schedule_end_str}): {e}")
            return True, "Error formato horario"

        current_time = now_ba.time()
        is_inside = start_time <= current_time < end_time

        if is_inside:
            return True, f"Horario Bolsa ({self.schedule_start_str}-{self.schedule_end_str} ART)"
        else:
            return False, f"Fuera de Horario ({self.schedule_start_str}-{self.schedule_end_str} ART)"

    def run(self):
        """Bucle principal de ejecución del bot."""
        logging.info(f"Iniciando monitoreo de {self.symbol} ({self.timeframe})...")
        
        while True:
            try:
                # 1. Obtener datos de mercado
                df = self.fetch_klines(limit=150)
                if df is None or len(df) == 0:
                    time.sleep(10)
                    continue

                current_price = df['close'].iloc[-1]
                
                # 2. Detectar divergencias RSI, Oscilador Oracle y VWAP
                rsi_signal, df_rsi = self.detect_rsi_divergences(df)
                current_rsi = df_rsi['rsi'].iloc[-1] if 'rsi' in df_rsi else 0.0
                
                oracle_signal, oracle_val, oracle_sig_val = self.calculate_oracle_oscillator(df)

                vwap_series = self.calculate_vwap(df)
                current_vwap = vwap_series.iloc[-1] if len(vwap_series) > 0 and not pd.isna(vwap_series.iloc[-1]) else current_price

                # 3. Confluencia de señales para entrada (RSI Div + Oracle + Filtro VWAP)
                combined_signal = None
                if rsi_signal == 'BULL_DIV' and oracle_signal == 'ORACLE_BULL' and current_price > current_vwap:
                    combined_signal = 'LONG'
                elif rsi_signal == 'BEAR_DIV' and oracle_signal == 'ORACLE_BEAR' and current_price < current_vwap:
                    combined_signal = 'SHORT'

                # 4. Consultar posición activa
                active_pos, entry, qty = self.get_active_position()
                
                # 5. En modo simulación, comprobar salidas TP/SL
                if self.dry_run and active_pos:
                    self.check_simulated_exit(current_price)
                    active_pos, entry, qty = self.get_active_position()
                elif not self.dry_run and active_pos is None and self.current_position:
                    # En modo real, la posición se cerró en Binance por TP o SL
                    exit_time = datetime.now()
                    dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

                    logging.info(f"Posición {self.current_position} cerrada en Binance. Cancelando órdenes pendientes huérfanas...")
                    try:
                        self.client.futures_cancel_all_open_orders(symbol=self.symbol)
                    except Exception as e:
                        logging.warning(f"Error cancelando órdenes residuales: {e}")
                    try:
                        self.futures_cancel_all_algo_orders(symbol=self.symbol)
                    except Exception as e:
                        logging.warning(f"Error cancelando órdenes algo residuales: {e}")
                    
                    # Obtener PnL de la operación cerrada
                    pnl = self._get_last_realized_pnl()
                    if pnl is None:
                        if self.current_position == 'LONG':
                            pnl = (current_price - self.entry_price) * self.position_qty
                        else:
                            pnl = (self.entry_price - current_price) * self.position_qty

                    pnl_pct = (pnl / self.margin_usdt) * 100.0 if self.margin_usdt > 0 else 0.0
                    if pnl_pct > self.max_pnl_pct:
                        self.max_pnl_pct = pnl_pct
                    if pnl_pct < self.min_pnl_pct:
                        self.min_pnl_pct = pnl_pct

                    logging.info(f"Operación cerrada. PnL: {pnl_pct:.2f}% | Max Gain: +{self.max_pnl_pct:.2f}% | Max Loss: {self.min_pnl_pct:.2f}% | Duración: {dur_mins:.1f}m")

                    target_file = "tp" if pnl >= 0 else "sl"
                    self._save_trade_to_file(target_file, self.max_pnl_pct, self.min_pnl_pct, dur_mins, exit_time)

                    self._record_trade_result(pnl)
                    self.current_position = None
                    self.entry_time = None
                    self.max_pnl_pct = 0.0
                    self.min_pnl_pct = 0.0
                    self.show_trade_stats()

                # Formato de consola de estado en pantalla (Cabecera fija + Estado actual)
                pnl_pct = 0.0
                dur_mins = 0.0
                if active_pos:
                    dur_mins = (datetime.now() - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0
                    if entry > 0:
                        if active_pos == 'LONG':
                            pnl_pct = ((current_price - entry) / entry) * self.leverage * 100.0
                        else:  # SHORT
                            pnl_pct = ((entry - current_price) / entry) * self.leverage * 100.0

                    if pnl_pct > self.max_pnl_pct:
                        self.max_pnl_pct = pnl_pct
                    if pnl_pct < self.min_pnl_pct:
                        self.min_pnl_pct = pnl_pct

                sig_orc_str = "BULL" if oracle_signal == 'ORACLE_BULL' else ("BEAR" if oracle_signal == 'ORACLE_BEAR' else "NEUT")

                # Verificar horario de operaciones (Bolsa EE.UU. 10:30 a 17:00 Buenos Aires)
                is_within_hours, schedule_reason = self.is_within_trading_hours()

                self.render_screen(
                    current_price=current_price,
                    current_vwap=current_vwap,
                    current_rsi=current_rsi,
                    oracle_val=oracle_val,
                    sig_orc_str=sig_orc_str,
                    rsi_signal=rsi_signal,
                    combined_signal=combined_signal,
                    active_pos=active_pos,
                    entry=entry,
                    qty=qty,
                    pnl_pct=pnl_pct,
                    dur_mins=dur_mins,
                    is_within_hours=is_within_hours,
                    schedule_reason=schedule_reason
                )

                # 6. Lógica de entrada si no hay posición activa
                if active_pos is None and combined_signal:
                    if not (self.enable_schedule and not is_within_hours):
                        self.open_position(combined_signal, current_price)

                # Esperar 10 segundos antes de la siguiente verificación
                time.sleep(10)

            except KeyboardInterrupt:
                print("\n[!] Bot detenido manualmente por el usuario. Exiting...")
                break
            except Exception as e:
                logging.error(f"Excepción no controlada en el bucle principal: {e}")
                time.sleep(10)


if __name__ == "__main__":
    bot = BinanceRsiDivergenceBot()
    bot.run()
