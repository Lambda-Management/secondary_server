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
import gzip
import io

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
        """Main WebSocket connection loop."""
        reconnect_delay = 1.0
        max_reconnect_delay = 60.0
        
        while self.running:
            try:
                logger.info(f"Connecting to WebSocket at {self.url}")
                
                # Connect to WebSocket API
                async with websockets.connect(self.url) as websocket:
                    self.websocket = websocket
                    self.connected = True
                    self.reconnecting = False
                    self.last_message_time = time.time()
                    logger.info("WebSocket connected successfully")
                    
                    # Resubscribe to existing channels
                    if self.subscriptions:
                        logger.info(f"Resubscribing to {len(self.subscriptions)} channels")
                        await self._resubscribe()
                    
                    # Reset reconnect delay on successful connection
                    reconnect_delay = 1.0
                    
                    while self.running:
                        try:
                            # Set timeout for receiving messages
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            
                            # Process the received message
                            await self._process_message(message)
                            
                        except asyncio.TimeoutError:
                            # Check if we haven't received any message for a while
                            time_since_last = time.time() - self.last_message_time
                            
                            if time_since_last > 30:
                                logger.warning(f"No message received for {time_since_last:.0f} seconds, checking connection...")
                                try:
                                    # Send ping to check connection
                                    await websocket.send("Ping")  # Use standard ping for Bingx
                                    logger.debug("Sent ping to check connection")
                                except Exception as e:
                                    logger.error(f"Error sending ping: {e}")
                                    break  # Connection likely broken, exit inner loop to reconnect
                            
                            # If no messages for too long, force reconnect
                            if time_since_last > 60:
                                logger.warning(f"No messages for {time_since_last:.0f} seconds, reconnecting...")
                                break  # Exit inner loop to reconnect
                                
                        except websockets.exceptions.ConnectionClosed as e:
                            logger.warning(f"WebSocket connection closed: {e}")
                            break
                            
                        except Exception as e:
                            logger.error(f"Error in websocket loop: {e}")
                            logger.error(traceback.format_exc())
                            # Continue and try to receive next message
                
                # If we exited the inner loop, connection is closed
                logger.warning("WebSocket connection closed, reconnecting...")
                self.connected = False
                self.websocket = None
                
            except Exception as e:
                self.connected = False
                self.websocket = None
                logger.error(f"WebSocket connection error: {e}")
                logger.error(traceback.format_exc())
                
            # Wait before reconnecting, with exponential backoff
            self.reconnecting = True
            logger.info(f"Reconnecting in {reconnect_delay:.1f} seconds...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
    
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
        """Process incoming websocket message."""
        try:
            # Kiểm tra xem dữ liệu có nén không
            try:
                # Thử giải nén với gzip nếu là binary data
                if isinstance(message, bytes):
                    try:
                        compressed_data = gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb')
                        decompressed_data = compressed_data.read()
                        message = decompressed_data.decode('utf-8')
                        logger.debug("Successfully decompressed gzip data")
                    except Exception as e:
                        logger.debug(f"Not gzipped data or decompression failed: {e}")
                        # Nếu không phải gzip, thử decode trực tiếp
                        message = message.decode('utf-8')
            except Exception as e:
                logger.debug(f"Error during decompression/decoding: {e}")
                # Nếu không thể decode, giả định là message đã là text
                pass

            # Xử lý ping từ server - rất quan trọng để duy trì kết nối
            if message == "Ping":
                logger.debug("Received Ping, sending Pong")
                await self.websocket.send("Pong")
                self.last_message_time = time.time()
                return

            # Trường hợp đặc biệt "ping..." trong một số phiên bản API
            if "ping" in message:
                try:
                    await self._send_pong(message)
                    self.last_message_time = time.time()
                    return
                except Exception as e:
                    logger.error(f"Error sending pong response: {e}")

            # Parse JSON message
            try:
                data = json.loads(message)
                
                # Kiểm tra lỗi từ server
                if 'code' in data and data['code'] != 0 and data['code'] != "":
                    logger.error(f"Error from WebSocket: {data}")
                    return

                # Xử lý dữ liệu kline
                if 'dataType' in data and '@kline_' in data.get('dataType', ''):
                    await self._process_kline(data)
                    self.last_message_time = time.time()
                    return

                # Xử lý các loại dữ liệu khác nếu cần
                logger.debug(f"Received other message type: {data}")
                self.last_message_time = time.time()
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message as JSON: {message[:200]}... Error: {e}")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(traceback.format_exc())
    
    async def _process_kline(self, data):
        """Process kline data from websocket."""
        try:
            # Log raw data format for debugging
            logger.debug(f"Processing kline data: {data}")
            
            # Extract symbol and interval from dataType
            data_type = data.get('dataType', '')
            if '@kline_' not in data_type:
                logger.warning(f"Invalid kline dataType: {data_type}")
                return
            
            parts = data_type.split('@kline_')
            if len(parts) != 2:
                logger.warning(f"Could not parse dataType: {data_type}")
                return
            
            symbol_with_suffix = parts[0]  # Example: "BTC-USDT"
            interval = parts[1]            # Example: "1m"
            
            # Extract base symbol (without -USDT)
            symbol = symbol_with_suffix.split('-')[0]
            
            # Extract kline data from response based on different formats
            kline = None
            
            # Format 1: Data in 'data' array with 'T' as timestamp
            if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                kline_item = data['data'][0]  # Use first item in the array
                if all(key in kline_item for key in ['c', 'o', 'h', 'l']) and 'T' in kline_item:
                    kline = {
                        'time': int(kline_item.get('T', 0)),
                        'open': float(kline_item.get('o', 0)),
                        'high': float(kline_item.get('h', 0)),
                        'low': float(kline_item.get('l', 0)),
                        'close': float(kline_item.get('c', 0)),
                        'volume': float(kline_item.get('v', 0))
                    }
                    logger.debug(f"Parsed kline data using Format 1 (data array with T timestamp)")
            
            # Format 2: Direct fields in main response
            elif all(key in data for key in ['o', 'h', 'l', 'c']):
                timestamp_field = 't' if 't' in data else ('T' if 'T' in data else 'time')
                if timestamp_field in data:
                    kline = {
                        'time': int(data.get(timestamp_field, 0)),
                        'open': float(data.get('o', 0)),
                        'high': float(data.get('h', 0)),
                        'low': float(data.get('l', 0)),
                        'close': float(data.get('c', 0)),
                        'volume': float(data.get('v', 0))
                    }
                    logger.debug(f"Parsed kline data using Format 2 (direct fields)")
            
            # Format 3: Standard format in 'data' object
            elif 'data' in data and isinstance(data['data'], dict):
                kline_data = data['data']
                # Try full field names
                if all(key in kline_data for key in ['time', 'open', 'high', 'low', 'close']):
                    kline = {
                        'time': int(kline_data.get('time', 0)),
                        'open': float(kline_data.get('open', 0)),
                        'high': float(kline_data.get('high', 0)),
                        'low': float(kline_data.get('low', 0)),
                        'close': float(kline_data.get('close', 0)),
                        'volume': float(kline_data.get('volume', 0))
                    }
                    logger.debug(f"Parsed kline data using Format 3 (standard format)")
                # Try short field names
                elif all(key in kline_data for key in ['t', 'o', 'h', 'l', 'c']):
                    kline = {
                        'time': int(kline_data.get('t', 0)),
                        'open': float(kline_data.get('o', 0)),
                        'high': float(kline_data.get('h', 0)),
                        'low': float(kline_data.get('l', 0)),
                        'close': float(kline_data.get('c', 0)),
                        'volume': float(kline_data.get('v', 0))
                    }
                    logger.debug(f"Parsed kline data using Format 3 (short field names)")
            
            # Format 4: Nested 'k' format (common in some websocket implementations)
            elif 'k' in data:
                k_data = data['k']
                timestamp_field = 't' if 't' in k_data else ('T' if 'T' in k_data else 'time')
                if timestamp_field in k_data:
                    kline = {
                        'time': int(k_data.get(timestamp_field, 0)),
                        'open': float(k_data.get('o', 0)),
                        'high': float(k_data.get('h', 0)),
                        'low': float(k_data.get('l', 0)),
                        'close': float(k_data.get('c', 0)),
                        'volume': float(k_data.get('v', 0))
                    }
                    logger.debug(f"Parsed kline data using Format 4 (nested k format)")
            
            # Log detailed info about the format if we couldn't parse it
            if kline is None:
                logger.warning(f"Could not find required kline fields in data: {data}")
                # Log detailed structure for debugging
                if 'data' in data:
                    logger.debug(f"Data field type: {type(data['data'])}")
                    if isinstance(data['data'], list) and len(data['data']) > 0:
                        logger.debug(f"First item in data array: {data['data'][0]}")
                        logger.debug(f"Keys in first item: {data['data'][0].keys()}")
                return
            
            # Validate the kline data
            if kline['time'] <= 0 or kline['open'] <= 0 or kline['close'] <= 0:
                logger.warning(f"Invalid kline data for {symbol}: {kline}")
                return

            # Update the internal data structure
            with self._lock:
                if symbol not in self.kline_data:
                    self.kline_data[symbol] = {}
                if interval not in self.kline_data[symbol]:
                    self.kline_data[symbol][interval] = deque(maxlen=self.max_data_points)
                
                klines = self.kline_data[symbol][interval]
                
                # Check if this candle already exists (update it) or is new (add it)
                existing_idx = None
                for i, existing_kline in enumerate(klines):
                    if existing_kline['time'] == kline['time']:
                        existing_idx = i
                        break
                    
                if existing_idx is not None:
                    # Update existing candle
                    klines[existing_idx] = kline
                    logger.debug(f"Updated existing candle for {symbol} at {datetime.fromtimestamp(kline['time']/1000)}")
                else:
                    # Add new candle and ensure the list stays sorted
                    klines.append(kline)
                    klines_list = list(klines)
                    klines_list.sort(key=lambda x: x['time'])
                    self.kline_data[symbol][interval] = deque(klines_list, maxlen=self.max_data_points)
                    logger.debug(f"Added new candle for {symbol} at {datetime.fromtimestamp(kline['time']/1000)}")
                
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
        """Subscribe to a kline websocket channel."""
        try:
            if not self.websocket or not self.is_connected():
                logger.warning("WebSocket not connected. Cannot subscribe.")
                return False

            # Format đúng cho dataType: symbol@kline_interval
            data_type = f"{symbol}@kline_{interval}"
            
            # Tạo subscription request đúng định dạng theo tài liệu Bingx
            subscription = {
                "id": str(int(time.time() * 1000)),  # Unique ID
                "reqType": "sub",
                "dataType": data_type
            }
            
            # Convert to JSON and send
            message = json.dumps(subscription)
            
            logger.info(f"Subscribing to {data_type}")
            await self.websocket.send(message)
            
            # Add to local subscriptions
            self.subscriptions.add(data_type)
            
            return True
        except Exception as e:
            logger.error(f"Error subscribing to {symbol} {interval}: {e}")
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