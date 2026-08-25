#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot de Trading Automático en Binance Futures (USDT-M)
Estrategia: Divergencias RSI en Velas de 1 Minuto
Modo: Aislado (Isolated) | Apalancamiento: 10x | Margen: 5 USDT
Take Profit: +10% ROI (sobre lo invertido)
Stop Loss: -50% ROI (sobre lo invertido)
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
        self.tp_roi_pct = float(os.getenv("TP_ROI_PCT", "10.0"))
        self.sl_roi_pct = float(os.getenv("SL_ROI_PCT", "50.0"))
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

        self._initialize_client()

    def _initialize_client(self):
        """Inicializa el cliente Binance API y obtiene precisión del símbolo."""
        print(Fore.CYAN + "=" * 65)
        print(Fore.CYAN + "      BOT DE TRADING FUTUROS BINANCE - DIVERGENCIA RSI (1m)      ")
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

    def calculate_tp_sl(self, side, entry_price):
        """
        Calcula precios exactos de Take Profit (+10% ROI) y Stop Loss (-50% ROI).
        Apalancamiento 10x:
        +10% ROI = +1.0% de variación de precio
        -50% ROI = -5.0% de variación de precio
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

        print(Fore.MAGENTA + f"\n[🚀 SEÑAL ENCONTRADA] Entrada {side} detectada en {current_price}")
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
            print(Fore.GREEN + f"[SIMULACIÓN] Posición {side} abierta exitosamente a {current_price}")
            return True

        # Ejecución Real en Binance Futures
        try:
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

            # 2. Orden de Take Profit (TAKE_PROFIT_MARKET)
            exit_side = 'SELL' if side == 'LONG' else 'BUY'
            self.client.futures_create_order(
                symbol=self.symbol,
                side=exit_side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=tp_price,
                closePosition=True
            )
            print(Fore.GREEN + f"[OK] Orden TAKE_PROFIT_MARKET colocada en {tp_price}")

            # 3. Orden de Stop Loss (STOP_MARKET)
            self.client.futures_create_order(
                symbol=self.symbol,
                side=exit_side,
                type='STOP_MARKET',
                stopPrice=sl_price,
                closePosition=True
            )
            print(Fore.GREEN + f"[OK] Orden STOP_MARKET colocada en {sl_price}")

            self.current_position = side
            self.entry_price = current_price
            self.position_qty = qty
            self.tp_price = tp_price
            self.sl_price = sl_price
            return True

        except Exception as e:
            print(Fore.RED + f"[!] Error abriendo posición en Binance: {e}")
            return False

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

        if hit_tp:
            pnl = self.margin_usdt * (self.tp_roi_pct / 100.0)
            self.simulated_balance += pnl
            print(Fore.GREEN + f"\n[🎯 TAKE PROFIT ALCANZADO] Posición {pos} cerrada a {current_price}.")
            print(Fore.GREEN + f" -> PnL: +{pnl:.2f} USDT (+{self.tp_roi_pct}% ROI) | Balance Simulado: {self.simulated_balance:.2f} USDT\n")
            self.current_position = None
        elif hit_sl:
            pnl = -self.margin_usdt * (self.sl_roi_pct / 100.0)
            self.simulated_balance += pnl
            print(Fore.RED + f"\n[🛑 STOP LOSS ALCANZADO] Posición {pos} cerrada a {current_price}.")
            print(Fore.RED + f" -> PnL: {pnl:.2f} USDT (-{self.sl_roi_pct}% ROI) | Balance Simulado: {self.simulated_balance:.2f} USDT\n")
            self.current_position = None

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
                
                # 2. Detectar divergencias RSI
                signal, df_rsi = self.detect_rsi_divergences(df)
                current_rsi = df_rsi['rsi'].iloc[-1] if 'rsi' in df_rsi else 0.0

                # 3. Consultar posición activa
                active_pos, entry, qty = self.get_active_position()
                
                # 4. En modo simulación, comprobar salidas TP/SL
                if self.dry_run and active_pos:
                    self.check_simulated_exit(current_price)
                    active_pos, entry, qty = self.get_active_position()

                # Consultar saldo real actualizado
                bal = self.get_account_balance()
                bal_str = f"Saldo Real: ${bal['available_balance']:<6.2f} USDT" if bal['has_keys'] else "Saldo Real: Req. API Keys"

                # Timestamp para registro
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Formato de consola
                status_color = Fore.YELLOW if active_pos else Fore.BLUE
                pos_str = f"{active_pos} @ {entry}" if active_pos else "SIN POSICIÓN"
                sig_str = signal if signal else "Sin Divergencia"

                print(f"[{now_str}] {self.symbol}: ${current_price:<9.2f} | RSI(14): {current_rsi:<5.2f} | {bal_str} | Señal: {sig_str:<14} | Estado: {status_color}{pos_str}")

                # 5. Lógica de entrada si no hay posición activa
                if active_pos is None:
                    if signal == 'BULL_DIV':
                        self.open_position('LONG', current_price)
                    elif signal == 'BEAR_DIV':
                        self.open_position('SHORT', current_price)

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
