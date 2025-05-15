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
import codecs

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
                
                # Add ping_interval and ping_timeout parameters to handle automatic pings
                async with websockets.connect(
                    self.url,
                    ping_interval=None,   # Disable automatic ping to handle manually
                    ping_timeout=10,      # Wait 10 seconds for pong response
                    close_timeout=5,      # Wait 5 seconds for graceful close
                    max_size=10 * 1024 * 1024  # 10MB max message size for large responses
                ) as websocket:
                    self.websocket = websocket
                    self.connected = True
                    reconnect_delay = 1  # Reset delay on successful connection
                    logger.info("Connected to Bingx WebSocket")
                    
                    # Send initial ping to verify connection
                    await websocket.ping()
                    
                    # Resubscribe to all symbols
                    await self._resubscribe()
                    
                    # Keep track of last message time for custom keepalive
                    last_message_time = time.time()
                    last_ping_time = time.time()
                    ping_interval = 20  # Send ping every 20 seconds
                    
                    # Process incoming messages
                    while self.running:
                        try:
                            # Check if we need to send a manual ping
                            current_time = time.time()
                            
                            # Send periodic pings even if we're receiving messages
                            if current_time - last_ping_time > ping_interval:
                                logger.debug(f"Sending scheduled ping (interval: {ping_interval}s)")
                                try:
                                    await websocket.ping()
                                    last_ping_time = current_time
                                except Exception as e:
                                    logger.error(f"Error sending scheduled ping: {e}")
                                    break
                            
                            # Additional check for stalled connection
                            if current_time - last_message_time > 60:  # No message for 60 seconds
                                logger.warning(f"No messages for 60 seconds, reconnecting...")
                                break
                                
                            # Wait for next message with timeout
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            last_message_time = current_time  # Update last message time
                            await self._process_message(message)
                            
                        except asyncio.TimeoutError:
                            # No message received for 30 seconds, send ping to keep connection alive
                            logger.warning("No message received for 30 seconds, checking connection...")
                            try:
                                pong_waiter = await websocket.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10)
                                logger.debug("Ping successful, connection still alive")
                                last_ping_time = time.time()  # Update last ping time
                            except asyncio.TimeoutError:
                                logger.warning("Ping timeout, reconnecting...")
                                break
                            except Exception as e:
                                logger.error(f"Error during ping: {e}")
                                break
                        except websockets.exceptions.ConnectionClosed as e:
                            logger.warning(f"WebSocket connection closed: {e}")
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
            # Log raw message for debugging (only in debug mode)
            if isinstance(message, str) and len(message) < 50:
                logger.debug(f"Raw WebSocket message: '{message}'")
            
            # Check if message is a simple ping (not JSON)
            if isinstance(message, str) and message.strip().lower() in ["ping", "ping..."]:
                logger.info(f"Received simple text ping message: '{message}', sending pong")
                if self.websocket and self.connected:
                    await self.websocket.send("pong")
                return
            
            # Check if the message is binary data
            if isinstance(message, bytes):
                # Check for gzip compression (magic bytes 0x1f 0x8b)
                if len(message) > 2 and message[0] == 0x1f and message[1] == 0x8b:
                    import gzip
                    try:
                        # Decompress the message
                        message = gzip.decompress(message).decode('utf-8')
                    except Exception as e:
                        logger.error(f"Failed to decompress gzip data: {e}")
                        return
                else:
                    # Try different encodings
                    try:
                        message = message.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            message = message.decode('latin-1')
                        except Exception as e:
                            logger.error(f"Failed to decode binary message: {e}")
                            return
            
            # Skip empty messages
            if not message or (isinstance(message, str) and not message.strip()):
                logger.debug("Received empty message, skipping")
                return
                    
            # Now try to parse JSON
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                # Handle non-JSON messages better
                if isinstance(message, str):
                    text_msg = message.strip()
                    # Common non-JSON ping messages that Bingx might send
                    if text_msg.lower() in ["ping", "ping...", "keepalive", "heartbeat"]:
                        logger.info(f"Received non-JSON ping: '{text_msg}', sending pong")
                        if self.websocket and self.connected:
                            await self.websocket.send("pong")
                        return
                
                # Other invalid JSON - log truncated message
                truncated_msg = message[:100] + "..." if isinstance(message, str) and len(message) > 100 else message
                logger.error(f"Invalid JSON received: {truncated_msg}")
                return
            
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
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(traceback.format_exc())
    
    async def _process_kline(self, data):
        """Xử lý dữ liệu kline từ WebSocket"""
        try:
            # First log the raw data format
            log_data = str(data)[:200] + "..." if isinstance(data, str) and len(str(data)) > 200 else data
            logger.debug(f"Processing kline data: {log_data}")
            
            if not data:
                logger.warning("Received empty kline data")
                return
            
            # Handle different data types
            if isinstance(data, str):
                # Try to parse string as JSON
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logger.error(f"Cannot parse kline string data as JSON: {data[:100]}...")
                    return
            
            if not isinstance(data, dict):
                logger.error(f"Unexpected kline data type: {type(data)}")
                return
                
            # Extract symbol from the message
            symbol_raw = data.get("s", "")  # e.g., "BTC-USDT"
            if not symbol_raw:
                # Try alternative fields for symbol
                alt_fields = ["symbol", "Symbol", "pair", "Pair"]
                for field in alt_fields:
                    if field in data and data[field]:
                        symbol_raw = data[field]
                        break
                        
            if not symbol_raw:
                logger.warning(f"Missing symbol in kline data: {data}")
                return
                
            # Extract base symbol without -USDT
            symbol = symbol_raw.split("-")[0] if "-" in symbol_raw else symbol_raw
            
            # Extract interval/timeframe
            interval = data.get("i", "")
            if not interval:
                # Try alternative fields for interval
                alt_fields = ["interval", "timeframe", "Interval", "Timeframe"]
                for field in alt_fields:
                    if field in data and data[field]:
                        interval = data[field]
                        break
                        
            if not interval:
                logger.warning(f"Missing interval in kline data for {symbol}")
                return
            
            # Format kline data based on available fields
            try:
                # First try the standard format
                if all(k in data for k in ["t", "o", "h", "l", "c", "v"]):
                    # Standard format with single-letter keys
                    kline = {
                        "time": int(data.get("t", 0)),
                        "open": float(data.get("o", 0)),
                        "high": float(data.get("h", 0)),
                        "low": float(data.get("l", 0)),
                        "close": float(data.get("c", 0)),
                        "volume": float(data.get("v", 0))
                    }
                elif "k" in data and isinstance(data["k"], dict):
                    # Nested 'k' object containing kline data
                    k_data = data["k"]
                    kline = {
                        "time": int(k_data.get("t", 0)),
                        "open": float(k_data.get("o", 0)),
                        "high": float(k_data.get("h", 0)),
                        "low": float(k_data.get("l", 0)),
                        "close": float(k_data.get("c", 0)),
                        "volume": float(k_data.get("v", 0))
                    }
                else:
                    # Try with full field names
                    full_field_map = {
                        "time": ["time", "timestamp", "openTime", "open_time"],
                        "open": ["open", "Open", "openPrice", "open_price"],
                        "high": ["high", "High", "highPrice", "high_price"],
                        "low": ["low", "Low", "lowPrice", "low_price"],
                        "close": ["close", "Close", "closePrice", "close_price"],
                        "volume": ["volume", "Volume", "qty", "quantity", "amount"]
                    }
                    
                    kline = {}
                    for target_field, possible_fields in full_field_map.items():
                        for field in possible_fields:
                            if field in data and data[field] is not None:
                                if target_field == "time":
                                    kline[target_field] = int(data[field])
                                else:
                                    kline[target_field] = float(data[field])
                                break
                                
                    # Check if we found all required fields
                    if not all(field in kline for field in ["time", "open", "high", "low", "close", "volume"]):
                        missing = [f for f in ["time", "open", "high", "low", "close", "volume"] if f not in kline]
                        logger.warning(f"Missing required fields in kline data for {symbol}: {missing}")
                        return
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing kline values for {symbol}: {e}")
                return
                
            # Đảm bảo kline có dữ liệu hợp lệ
            if kline["time"] == 0:
                logger.warning(f"Invalid kline data received for {symbol}: timestamp is 0")
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