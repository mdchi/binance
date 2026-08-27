#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de Trading Automático en Binance Futures (USDT-M)
Estrategia: Confluencia Divergencias RSI + Oscilador Oracle + VWAP en Velas de 1 Minuto
Modo: Aislado (Isolated) | Apalancamiento: 10x | Margen: 5 USDT
Take Profit: +3% ROI (sobre lo invertido)
Stop Loss: -3% ROI (sobre lo invertido)
"""

import os
import sys
import time
import math
import logging
import re
import shutil
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from colorama import init, Fore, Style

# Configuración de encoding para consola Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Inicializar colorama para mensajes formateados en consola
init(autoreset=True)

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
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
        self.margin_usdt = float(os.getenv("MARGIN_USDT", "5.0"))
        self.leverage = int(os.getenv("LEVERAGE", "10"))
        self.timeframe = os.getenv("TIMEFRAME", "1m")
        self.tp_roi_pct = float(os.getenv("TP_ROI_PCT", "3.0"))
        self.sl_roi_pct = float(os.getenv("SL_ROI_PCT", "3.0"))
        self.rsi_period = int(os.getenv("RSI_PERIOD", "14"))
        self.pivot_left = int(os.getenv("PIVOT_LOOKBACK_LEFT", "5"))
        self.pivot_right = int(os.getenv("PIVOT_LOOKBACK_RIGHT", "2"))
        
        self.dry_run = os.getenv("DRY_RUN", "True").lower() in ("true", "1", "yes")
        self.use_testnet = os.getenv("USE_TESTNET", "False").lower() in ("true", "1", "yes")

        # Variables de estado interno
        self.client = None
        self.price_precision = 2
        self.qty_precision = 3
        self.min_qty = 0.001
        self.tick_size = 0.01
        self.step_size = 0.001
        
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

        self._initialize_client()

    def _initialize_client(self):
        """Inicializa el cliente Binance API y obtiene precisión del símbolo."""
        print(Fore.CYAN + "=" * 65)
        print(Fore.CYAN + "   BOT DE TRADING BINANCE - DIVERGENCIA RSI + ORACLE + VWAP (1m) ")
        print(Fore.CYAN + "=" * 65)
        print(f"{Fore.YELLOW}Símbolo: {Style.BRIGHT}{self.symbol}")
        print(f"{Fore.YELLOW}Modo de Margen: {Style.BRIGHT}AISLADO (ISOLATED)")
        print(f"{Fore.YELLOW}Apalancamiento: {Style.BRIGHT}{self.leverage}x")
        print(f"{Fore.YELLOW}Monto por Operación: {Style.BRIGHT}{self.margin_usdt} USDT")
        print(f"{Fore.YELLOW}Take Profit (TP): {Style.BRIGHT}+{self.tp_roi_pct}% ROI ({(self.tp_roi_pct/self.leverage):.2f}% en precio)")
        print(f"{Fore.YELLOW}Stop Loss (SL): {Style.BRIGHT}-{self.sl_roi_pct}% ROI ({(self.sl_roi_pct/self.leverage):.2f}% en precio)")
        print(f"{Fore.YELLOW}Modo Dry-Run (Simulación): {Style.BRIGHT}{self.dry_run}")
        print(f"{Fore.YELLOW}Binance Testnet: {Style.BRIGHT}{self.use_testnet}")

        if not self.api_key or not self.api_secret:
            print(Fore.YELLOW + "AVISO: No se configuraron BINANCE_API_KEY / BINANCE_API_SECRET en el archivo .env.")
            print(Fore.YELLOW + "       Para visualizar tu saldo REAL de Binance, ingresa tus claves en el archivo .env")

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
                print(Fore.GREEN + "[OK] Conexión autenticada exitosamente con Binance Futures API.")
            else:
                print(Fore.GREEN + "[OK] Conexión de mercado iniciada correctamente.")

            # Cerrar cualquier posición abierta previa al iniciar
            self.close_existing_positions()

        except Exception as e:
            print(Fore.RED + f"[!] Error conectando a Binance API: {e}")
            if not self.dry_run:
                print(Fore.YELLOW + "[i] Cambiando automáticamente a modo DRY-RUN.")
                self.dry_run = True

        # Mostrar saldo real de la cuenta en pantalla
        bal = self.get_account_balance()
        print(f"{Fore.CYAN}-----------------------------------------------------------------")
        if bal['has_keys']:
            print(f"{Fore.GREEN}{Style.BRIGHT}💰 SALDO REAL EN CUENTA BINANCE FUTUROS:")
            print(f"   • Balance Billetera: {Style.BRIGHT}{bal['wallet_balance']:.2f} USDT")
            print(f"   • Saldo Disponible:  {Style.BRIGHT}{bal['available_balance']:.2f} USDT")
            print(f"   • PnL No Realizado:  {Style.BRIGHT}{bal['unrealized_pnl']:.2f} USDT")
        else:
            print(f"{Fore.RED}{Style.BRIGHT}💰 SALDO REAL EN CUENTA BINANCE:")
            print(f"   {Fore.YELLOW}No disponible (Faltan API Keys en el archivo .env)")
        print(Fore.CYAN + "=" * 65)

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
                print(Fore.GREEN + f"[OK] Margen cambiado a ISOLATED para {self.symbol}.")
            except BinanceAPIException as e:
                # Código -4046: "No need to change margin type."
                if e.code == -4046 or "No need to change" in str(e):
                    pass
                else:
                    print(Fore.YELLOW + f"[!] Nota sobre margen: {e.message}")

            # Configurar Apalancamiento 10x
            self.client.futures_change_leverage(symbol=self.symbol, leverage=self.leverage)
            print(Fore.GREEN + f"[OK] Apalancamiento configurado a {self.leverage}x para {self.symbol}.")

        except Exception as e:
            print(Fore.RED + f"[!] Error al configurar cuenta de futuros: {e}")

    def close_existing_positions(self):
        """
        Cierra cualquier posición abierta previa al iniciar el bot y cancela órdenes pendientes.
        """
        print(Fore.YELLOW + "🔍 Verificando y cerrando posiciones abiertas al iniciar el bot...")
        if self.dry_run:
            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None
            print(Fore.GREEN + "[OK] Modo Simulación (DRY-RUN): Posición inicial restablecida a SIN POSICIÓN.")
            return

        if not self.client or not self.api_key or not self.api_secret:
            print(Fore.YELLOW + "[!] Sin API Keys configuradas. Omitiendo cierre de posiciones previas.")
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
                    print(Fore.RED + f"[⚠️ INICIO] Posición previa detectada en Binance: {pos_type} de {qty} {self.symbol}. Cerrando a MARKET...")
                    
                    close_order = self.client.futures_create_order(
                        symbol=self.symbol,
                        side=side_to_close,
                        type='MARKET',
                        quantity=qty,
                        reduceOnly=True
                    )
                    print(Fore.GREEN + f"[OK] Posición {pos_type} previa cerrada exitosamente a MARKET. Order ID: {close_order.get('orderId')}")
                    closed_any = True

            if not closed_any:
                print(Fore.GREEN + f"[OK] Sin posiciones abiertas previas para {self.symbol} en Binance Futuros.")

            self.current_position = None
            self.entry_price = 0.0
            self.position_qty = 0.0
            self.entry_time = None

        except Exception as e:
            print(Fore.RED + f"[!] Error cerrando posiciones abiertas al iniciar: {e}")

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
        Calcula precios exactos de Take Profit (+3% ROI) y Stop Loss (-3% ROI).
        Apalancamiento 10x:
        +3% ROI = +0.3% de variación de precio
        -3% ROI = -0.3% de variación de precio
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

        print(Fore.CYAN + "\n" + "=" * 65)
        print(Fore.MAGENTA + f"[🚀 SEÑAL ENCONTRADA] Entrada {side} detectada en {current_price}")
        print(f" -> Margen: {self.margin_usdt} USDT | Apalancamiento: {self.leverage}x | Posición Nocional: {notional_val} USDT")
        print(f" -> Cantidad: {qty} {self.symbol}")
        print(Fore.GREEN + f" -> Take Profit (+{self.tp_roi_pct}% ROI): {tp_price}")
        print(Fore.RED + f" -> Stop Loss (-{self.sl_roi_pct}% ROI): {sl_price}")

        if self.dry_run:
            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.position_start_time = int(time.time() * 1000)
            self.entry_time = datetime.now()
            print(Fore.GREEN + f"[SIMULACIÓN] Posición {side} abierta exitosamente a {current_price}")
            print(Fore.CYAN + f" -> Horario de Entrada: {self.entry_time.strftime('%H:%M:%S')} ({self.entry_time.strftime('%Y-%m-%d')})")
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
            print(Fore.GREEN + f"[OK] Orden MARKET de entrada ejecutada: ID {market_order.get('orderId')}")

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
                print(Fore.GREEN + f"[OK] Orden TAKE_PROFIT_MARKET colocada en {tp_price}")
            except Exception as e:
                print(Fore.RED + f"[!] Error colocando Take Profit algo: {e}")

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
                print(Fore.GREEN + f"[OK] Orden STOP_MARKET colocada en {sl_price}")
            except Exception as e:
                print(Fore.RED + f"[!] Error colocando Stop Loss algo: {e}")

            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            self.position_start_time = int(time.time() * 1000)
            self.entry_time = datetime.now()
            print(Fore.CYAN + f" -> Horario de Entrada: {self.entry_time.strftime('%H:%M:%S')} ({self.entry_time.strftime('%Y-%m-%d')})")
            return True

        except Exception as e:
            print(Fore.RED + f"[!] Error abriendo posición en Binance: {e}")
            return False

    def _record_trade_result(self, pnl):
        """Registra el resultado de una operación cerrada (ganada/perdida y PnL)."""
        if pnl > 0:
            self.winning_trades += 1
            self.money_won += pnl
        elif pnl < 0:
            self.losing_trades += 1
            self.money_lost += abs(pnl)

    @staticmethod
    def _fit_to_terminal(text, max_cols):
        """Trunca la línea respetando secuencias ANSI de color para que no supere max_cols y no haga salto de línea."""
        plain_text = re.sub(r'\033\[[0-9;]*m', '', text)
        if len(plain_text) < max_cols:
            return text

        result = []
        visible_count = 0
        i = 0
        n = len(text)
        max_vis = max(10, max_cols - 4)

        while i < n and visible_count < max_vis:
            if text[i] == '\033':
                match = re.match(r'\033\[[0-9;]*m', text[i:])
                if match:
                    code = match.group(0)
                    result.append(code)
                    i += len(code)
                    continue
            result.append(text[i])
            visible_count += 1
            i += 1

        result.append("..." + Style.RESET_ALL)
        return "".join(result)

    def show_trade_stats(self):
        """Muestra en una sola línea el resumen de estadísticas de operaciones, tiempo total de funcionamiento, dinero ganado/perdido y balance billetera."""
        if self.dry_run:
            wallet_bal_str = f"{self.simulated_balance:.2f} USDT"
        else:
            bal = self.get_account_balance()
            wallet_bal_str = f"{bal['wallet_balance']:.2f} USDT" if bal['has_keys'] else "N/A"

        uptime_hours = (time.time() - self.bot_start_time) / 3600.0

        stats_line = (
            f"{Fore.CYAN}{Style.BRIGHT}📊 RESUMEN: "
            f"{Fore.CYAN}Tiempo Total: {uptime_hours:.2f}h {Style.RESET_ALL}| "
            f"{Fore.GREEN}Ganadas: {self.winning_trades} {Style.RESET_ALL}| "
            f"{Fore.RED}Perdidas: {self.losing_trades} {Style.RESET_ALL}| "
            f"{Fore.GREEN}Dinero Ganado: +{self.money_won:.2f} USDT {Style.RESET_ALL}| "
            f"{Fore.RED}Dinero Perdido: -{self.money_lost:.2f} USDT {Style.RESET_ALL}| "
            f"{Fore.YELLOW}{Style.BRIGHT}Balance Billetera: {wallet_bal_str}{Style.RESET_ALL}"
        )
        print(stats_line)

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
            entry_str = self.entry_time.strftime('%H:%M:%S') if self.entry_time else "N/A"
            exit_str = exit_time.strftime('%H:%M:%S')
            dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

            if hit_tp:
                pnl = self.margin_usdt * (self.tp_roi_pct / 100.0)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                sys.stdout.write("\n")
                sys.stdout.flush()
                print(Fore.GREEN + f"[🎯 TAKE PROFIT ALCANZADO] Posición {pos} cerrada a {current_price}.")
                print(Fore.GREEN + f" -> PnL: +{pnl:.2f} USDT (+{self.tp_roi_pct}% ROI)")
                print(Fore.CYAN + f" -> Entrada: {entry_str} | Cierre: {exit_str} | Duración: {dur_mins:.2f} min ({dur_mins:.1f} minutos)")
                self.current_position = None
                self.entry_time = None
                self.show_trade_stats()
            elif hit_sl:
                pnl = -self.margin_usdt * (self.sl_roi_pct / 100.0)
                self.simulated_balance += pnl
                self._record_trade_result(pnl)
                sys.stdout.write("\n")
                sys.stdout.flush()
                print(Fore.RED + f"[🛑 STOP LOSS ALCANZADO] Posición {pos} cerrada a {current_price}.")
                print(Fore.RED + f" -> PnL: {pnl:.2f} USDT (-{self.sl_roi_pct}% ROI)")
                print(Fore.CYAN + f" -> Entrada: {entry_str} | Cierre: {exit_str} | Duración: {dur_mins:.2f} min ({dur_mins:.1f} minutos)")
                self.current_position = None
                self.entry_time = None
                self.show_trade_stats()

    def run(self):
        """Bucle principal de ejecución del bot."""
        print(Fore.CYAN + f"\n[Iniciando Monitoreo] Analizando {self.symbol} en velas de {self.timeframe}...\n")
        
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
                    entry_str = self.entry_time.strftime('%H:%M:%S') if self.entry_time else "N/A"
                    exit_str = exit_time.strftime('%H:%M:%S')
                    dur_mins = (exit_time - self.entry_time).total_seconds() / 60.0 if self.entry_time else 0.0

                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    print(Fore.YELLOW + f"[ℹ] Posición {self.current_position} cerrada en Binance. Cancelando órdenes pendientes huérfanas...")
                    print(Fore.CYAN + f" -> Entrada: {entry_str} | Cierre: {exit_str} | Duración: {dur_mins:.2f} min ({dur_mins:.1f} minutos)")
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

                    self._record_trade_result(pnl)
                    self.current_position = None
                    self.entry_time = None
                    self.show_trade_stats()

                # Consultar saldo actualizado
                if self.dry_run:
                    bal_str = f"Balance: {self.simulated_balance:.2f} USDT"
                else:
                    bal = self.get_account_balance()
                    bal_str = f"Balance: {bal['wallet_balance']:.2f} USDT" if bal['has_keys'] else "Balance: Req. API Keys"

                # Timestamp para registro y tiempo total de funcionamiento en horas
                now_str = datetime.now().strftime("%H:%M:%S")
                uptime_hours = (time.time() - self.bot_start_time) / 3600.0
                
                # Formato de consola
                status_color = Fore.YELLOW if active_pos else Fore.BLUE
                if active_pos:
                    dur_str = f" ({(datetime.now() - self.entry_time).total_seconds() / 60.0:.1f}m)" if self.entry_time else ""
                    pos_str = f"{active_pos} @ {entry:.2f}{dur_str}"
                else:
                    pos_str = "SIN POSICIÓN"
                sig_rsi_str = rsi_signal if rsi_signal else "Sin Div"
                sig_orc_str = "BULL" if oracle_signal == 'ORACLE_BULL' else ("BEAR" if oracle_signal == 'ORACLE_BEAR' else "NEUT")
                sig_comb_str = combined_signal if combined_signal else "ESPERANDO"

                cols = shutil.get_terminal_size(fallback=(160, 24)).columns

                if cols >= 150:
                    line_str = f"[{now_str} | Run: {uptime_hours:.2f}h] {self.symbol}: ${current_price:.2f} | VWAP: ${current_vwap:.2f} | RSI: {current_rsi:.1f} | Oracle: {oracle_val:.1f} ({sig_orc_str}) | Div: {sig_rsi_str} | {bal_str} | Señal: {sig_comb_str} | Estado: {status_color}{pos_str}{Style.RESET_ALL}"
                else:
                    line_str = f"[{now_str}|{uptime_hours:.2f}h] {self.symbol}:${current_price:.2f} | VWAP:${current_vwap:.2f} | RSI:{current_rsi:.1f} | Orc:{oracle_val:.1f}({sig_orc_str}) | Div:{sig_rsi_str} | {bal_str} | Señal:{sig_comb_str} | Est:{status_color}{pos_str}{Style.RESET_ALL}"

                line_str = self._fit_to_terminal(line_str, cols)

                sys.stdout.write(f"\r\033[K{line_str}")
                sys.stdout.flush()

                # 6. Lógica de entrada si no hay posición activa
                if active_pos is None and combined_signal:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    self.open_position(combined_signal, current_price)

                # Esperar 10 segundos antes de la siguiente verificación
                time.sleep(10)

            except KeyboardInterrupt:
                print(Fore.YELLOW + "\n[!] Bot detenido manualmente por el usuario. Exiting...")
                break
            except Exception as e:
                logging.error(f"Excepción no controlada en el bucle principal: {e}")
                time.sleep(10)


if __name__ == "__main__":
    bot = BinanceRsiDivergenceBot()
    bot.run()
