import json
import time
import logging
import asyncio
import threading
import websockets
import traceback
from typing import Dict, List, Any, Optional, Set
from collections import deque
from datetime import datetime

logger = logging.getLogger(__name__)

class BingxWebSocketClient:
    """
    Client để kết nối WebSocket Bingx và nhận dữ liệu OHLCV.
    Phiên bản độc lập cho data server.
    """
    
    def __init__(self, max_data_points=1200):
        self.url = "wss://open-api-swap.bingx.com/swap-market"
        self.connected = False
        self.websocket = None
        self.subscriptions = set()
        self.kline_data = {}  # Dict để lưu trữ dữ liệu OHLCV
        self._lock = threading.Lock()  # Lock để đảm bảo thread safety
        self.max_data_points = max_data_points
        self.running = False
        self.thread = None
        
    def start(self):
        """Bắt đầu WebSocket client trong một thread riêng"""
        if self.thread is not None and self.thread.is_alive():
            logger.warning("WebSocket client is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._run_websocket)
        self.thread.daemon = True
        self.thread.start()
        logger.info("WebSocket client started")
        
    def stop(self):
        """Dừng WebSocket client"""
        self.running = False
        if self.thread is not None:
            logger.info("Waiting for WebSocket thread to stop...")
            self.thread.join(timeout=5)
            logger.info("WebSocket thread stopped")
            
    def is_connected(self):
        """Kiểm tra WebSocket có đang kết nối không"""
        return self.connected
        
    def _run_websocket(self):
        """Hàm chạy WebSocket trong thread"""
        asyncio.run(self._websocket_loop())
            
    async def _websocket_loop(self):
        """Loop chính của WebSocket"""
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while self.running:
            try:
                logger.info("Connecting to Bingx WebSocket...")
                
                async with websockets.connect(self.url) as websocket:
                    self.websocket = websocket
                    self.connected = True
                    reconnect_delay = 1  # Reset delay on successful connection
                    logger.info("Connected to Bingx WebSocket")
                    
                    # Resubscribe to all symbols
                    await self._resubscribe()
                    
                    # Process incoming messages
                    while self.running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            await self._process_message(message)
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            try:
                                pong_waiter = await websocket.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10)
                            except asyncio.TimeoutError:
                                logger.warning("Ping timeout, reconnecting...")
                                break
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("WebSocket connection closed, reconnecting...")
                            break
            
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                logger.error(traceback.format_exc())
                
            finally:
                self.connected = False
                self.websocket = None
                
                if self.running:
                    logger.info(f"Reconnecting in {reconnect_delay} seconds...")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def _resubscribe(self):
        """Đăng ký lại các symbols đã đăng ký trước đó"""
        if not self.subscriptions:
            return
            
        logger.info(f"Resubscribing to {len(self.subscriptions)} symbols...")
        
        # Tạo bản sao của subscriptions để tránh thay đổi trong quá trình lặp
        current_subs = list(self.subscriptions)
        
        for subscription in current_subs:
            symbol, interval = subscription.split("_")
            await self._subscribe(symbol, interval)
            await asyncio.sleep(0.2)  # Tránh rate limiting
            
        logger.info("Resubscription completed")
    
    async def _process_message(self, message):
        """Xử lý message từ WebSocket"""
        try:
            data = json.loads(message)
            
            # Xử lý heartbeat
            if "ping" in data:
                await self._send_pong(data["ping"])
                return
                
            # Kiểm tra lỗi
            if "code" in data and data["code"] != 0:
                logger.error(f"Error from WebSocket: {data}")
                return
                
            # Xử lý dữ liệu kline
            if "data" in data and "e" in data:
                if data["e"] == "kline":
                    await self._process_kline(data["data"])
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON received: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(traceback.format_exc())
    
    async def _process_kline(self, data):
        """Xử lý dữ liệu kline từ WebSocket"""
        try:
            if not data or not isinstance(data, dict):
                return
                
            symbol = data.get("s", "").split("-")[0]  # Extract symbol from "BTC-USDT"
            interval = data.get("i", "")
            
            if not symbol or not interval:
                return
                
            # Format dữ liệu
            kline = {
                "time": int(data.get("t", 0)),
                "open": float(data.get("o", 0)),
                "high": float(data.get("h", 0)),
                "low": float(data.get("l", 0)),
                "close": float(data.get("c", 0)),
                "volume": float(data.get("v", 0))
            }
            
            # Đảm bảo kline có dữ liệu hợp lệ
            if kline["time"] == 0:
                logger.warning(f"Invalid kline data received for {symbol}: {data}")
                return
                
            # Cập nhật dữ liệu kline
            with self._lock:
                if symbol not in self.kline_data:
                    self.kline_data[symbol] = {}
                    
                if interval not in self.kline_data[symbol]:
                    self.kline_data[symbol][interval] = deque(maxlen=self.max_data_points)
                    
                # Kiểm tra xem kline đã tồn tại chưa, nếu có thì cập nhật
                # Tìm kline với thời gian trùng khớp
                for i, existing_kline in enumerate(self.kline_data[symbol][interval]):
                    if existing_kline["time"] == kline["time"]:
                        # Cập nhật kline hiện tại
                        self.kline_data[symbol][interval][i] = kline
                        return
                        
                # Nếu không tìm thấy kline hiện tại, thêm mới
                self.kline_data[symbol][interval].append(kline)
                
                # Đảm bảo dữ liệu được sắp xếp theo thời gian
                klines_list = list(self.kline_data[symbol][interval])
                klines_list.sort(key=lambda x: x["time"])
                self.kline_data[symbol][interval] = deque(klines_list, maxlen=self.max_data_points)
                
        except Exception as e:
            logger.error(f"Error processing kline data: {e}")
            logger.error(traceback.format_exc())
    
    async def _send_pong(self, ping_value):
        """Gửi pong để trả lời ping từ server"""
        if not self.websocket or not self.connected:
            return
            
        try:
            pong_message = json.dumps({"pong": ping_value})
            await self.websocket.send(pong_message)
        except Exception as e:
            logger.error(f"Error sending pong: {e}")
    
    async def _subscribe(self, symbol, interval):
        """Subscribe vào một kênh kline"""
        if not self.websocket or not self.connected:
            logger.error("Cannot subscribe: WebSocket not connected")
            return False
            
        try:
            subscription_id = f"{symbol}_{interval}"
            
            # Kiểm tra xem đã subscribe chưa
            if subscription_id in self.subscriptions:
                logger.info(f"Already subscribed to {subscription_id}")
                return True
                
            # Tạo message subscribe
            message = {
                "id": str(int(time.time() * 1000)),
                "reqType": "sub",
                "dataType": f"kline_{interval}",
                "symbol": symbol
            }
            
            # Gửi message
            await self.websocket.send(json.dumps(message))
            
            # Đánh dấu là đã subscribe
            self.subscriptions.add(subscription_id)
            logger.info(f"Subscribed to {symbol} kline_{interval}")
            
            return True
        except Exception as e:
            logger.error(f"Error subscribing to {symbol} kline_{interval}: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def subscribe_kline(self, symbol, interval):
        """Subscribe vào một kênh kline (phiên bản đồng bộ)"""
        if not self.connected:
            logger.error("Cannot subscribe: WebSocket not connected")
            return False
            
        # Chuyển đổi để chạy hàm bất đồng bộ
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(self._subscribe(symbol, interval))
        loop.close()
        
        return result
    
    def get_klines(self, symbol, interval):
        """Lấy dữ liệu kline cho một symbol và interval"""
        with self._lock:
            if symbol not in self.kline_data or interval not in self.kline_data[symbol]:
                return []
                
            return list(self.kline_data[symbol][interval]) 