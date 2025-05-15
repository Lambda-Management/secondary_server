import os
import sys
import time
import json
import logging
import threading
import asyncio
import pickle
import traceback
import uvicorn
import requests
import numpy as np
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from collections import deque
from pydantic import BaseModel

# Thêm thư mục gốc vào sys.path để import các module cần thiết
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import WebSocket client và các hàm cần thiết từ dự án chính
# from websocket_client import BingxWebSocketClient
# from crawldata import get_ohclv
# from utils import convert_numpy_to_python

# Import các module nội bộ độc lập
from websocket_handler import BingxWebSocketClient
from rest_api import get_ohclv

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_server.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Khởi tạo FastAPI app
app = FastAPI(title="Data Service API", description="API for fetching OHLCV data from Bingx")

# Thêm CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cấu hình server
SERVER_ID = os.environ.get("SERVER_ID", "secondary")  # "primary" hoặc "secondary"
SERVER_PORT = int(os.environ.get("SERVER_PORT", 8080))
MAX_KLINE_CANDLES = 1200  # Số lượng nến tối đa lưu trong cache

# Đường dẫn thư mục cache
CACHE_DIR = os.path.join(os.path.dirname(__file__), "data_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Khởi tạo WebSocket client
ws_client = None
# Biến khóa để đảm bảo thread safety
kline_data_lock = threading.Lock()

# Model dữ liệu cho request
class SymbolRequest(BaseModel):
    symbols: List[str]
    timeframe: str
    limit: int = 1000

class HealthCheckResponse(BaseModel):
    status: str
    server_id: str
    uptime: float
    subscribed_symbols: int
    start_time: str

# Biến lưu thời điểm khởi động
start_time = datetime.now()

# Biến global để lưu trữ phân chia symbols
primary_symbols = set()
secondary_symbols = set()

def get_timeframe_ms(timeframe):
    """Convert timeframe string (e.g., '1m', '5m', '1h') to milliseconds."""
    unit = timeframe[-1]
    value = int(timeframe[:-1])
    
    if unit == 'm':
        return value * 60 * 1000  # minutes to milliseconds
    elif unit == 'h':
        return value * 60 * 60 * 1000  # hours to milliseconds
    elif unit == 'd':
        return value * 24 * 60 * 60 * 1000  # days to milliseconds
    elif unit == 'w':
        return value * 7 * 24 * 60 * 60 * 1000  # weeks to milliseconds
    else:
        logger.warning(f"Unknown timeframe unit: {unit}, defaulting to 5 minutes")
        return 5 * 60 * 1000  # default to 5 minutes

def save_klines_to_cache(symbol: str, timeframe: str, data: List[Dict[str, Any]]):
    """Lưu dữ liệu nến vào cache file."""
    try:
        cache_file = os.path.join(CACHE_DIR, f"{symbol}_{timeframe}_klines.pkl")
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
        logger.debug(f"Saved {len(data)} candles to cache for {symbol}_{timeframe}")
        return True
    except Exception as e:
        logger.error(f"Error saving klines to cache for {symbol}_{timeframe}: {e}")
        return False

def load_klines_from_cache(symbol: str, timeframe: str) -> List[Dict[str, Any]]:
    """Tải dữ liệu nến từ cache file."""
    try:
        cache_file = os.path.join(CACHE_DIR, f"{symbol}_{timeframe}_klines.pkl")
        if not os.path.exists(cache_file):
            return []
            
        # Kiểm tra thời gian cập nhật
        cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
        # Cache quá 1 ngày sẽ không được sử dụng
        if cache_age > timedelta(days=1):
            logger.warning(f"Cache for {symbol}_{timeframe} is too old ({cache_age.total_seconds() / 3600:.1f} hours), ignoring")
            return []
            
        # Chỉ đọc dữ liệu khi thực sự cần
        with open(cache_file, 'rb') as f:
            data = pickle.load(f)
            
        # Đảm bảo dữ liệu được sắp xếp từ cũ đến mới
        if len(data) > 1:
            is_sorted = all(data[i]['time'] <= data[i+1]['time'] for i in range(len(data)-1))
            if not is_sorted:
                logger.info(f"Resorting cached data for {symbol}_{timeframe} to ensure correct order")
                data.sort(key=lambda x: x['time'])
            
        logger.debug(f"Loaded {len(data)} candles from cache for {symbol}_{timeframe}")
        return data
    except Exception as e:
        logger.error(f"Error loading klines from cache for {symbol}_{timeframe}: {e}")
        return []

def fetch_historical_klines(symbol: str, timeframe: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch historical klines data from REST API for a symbol and timeframe.
    
    Args:
        symbol (str): The symbol to fetch data for (e.g., 'BTC')
        timeframe (str): The timeframe (e.g., '5m', '1h')
        limit (int): Number of candles to fetch
        
    Returns:
        List[Dict[str, Any]]: List of formatted candle data
    """
    try:
        logger.info(f"Fetching {limit} historical candles for {symbol} {timeframe} from REST API")
        
        # Get data from REST API
        rest_data = get_ohclv(symbol, timeframe, limit)
        
        if not rest_data:
            logger.error(f"Failed to get historical data for {symbol}")
            return []
            
        # Format data to match WebSocket format
        formatted_data = []
        
        # Kiểm tra định dạng dữ liệu trả về
        if rest_data and isinstance(rest_data, list):
            if len(rest_data) == 0:
                logger.error(f"No data returned for {symbol}")
                return []
                
            # Kiểm tra cấu trúc của dữ liệu
            sample_item = rest_data[0]
            
            # Định dạng API chuẩn: {'open': '0.7034', 'close': '0.7065', 'high': '0.7081', 'low': '0.7033', 'volume': '635494.00', 'time': 1702717200000}
            if isinstance(sample_item, dict) and 'open' in sample_item and 'time' in sample_item:
                logger.info(f"Processing API data for {symbol}")
                
                for candle in rest_data:
                    formatted_candle = {
                        'time': int(candle['time']),
                        'open': float(candle['open']),
                        'high': float(candle['high']),
                        'low': float(candle['low']),
                        'close': float(candle['close']),
                        'volume': float(candle['volume'])
                    }
                    formatted_data.append(formatted_candle)
                
                # Luôn sắp xếp dữ liệu từ cũ đến mới (thời gian tăng dần)
                formatted_data.sort(key=lambda x: x['time'])
                logger.info(f"Sorted {len(formatted_data)} candles for {symbol} from oldest to newest")
                
            # Định dạng cũ dạng mảng: [timestamp, open, high, low, close, volume]
            elif isinstance(sample_item, list) and len(sample_item) >= 6:
                logger.info(f"Processing legacy array format for {symbol}")
                for candle in rest_data:
                    formatted_candle = {
                        'time': int(candle[0]),     # timestamp
                        'open': float(candle[1]),   # open
                        'high': float(candle[2]),   # high
                        'low': float(candle[3]),    # low
                        'close': float(candle[4]),  # close
                        'volume': float(candle[5])  # volume
                    }
                    formatted_data.append(formatted_candle)
                
                # Luôn sắp xếp dữ liệu từ cũ đến mới (thời gian tăng dần)
                formatted_data.sort(key=lambda x: x['time'])
                logger.info(f"Sorted {len(formatted_data)} candles for {symbol} from oldest to newest")
                
            else:
                logger.error(f"Unknown data format for {symbol}: {sample_item}")
                return []
        else:
            logger.error(f"Unexpected data format from API for {symbol}: {type(rest_data)}")
            return []
            
        logger.info(f"Successfully retrieved {len(formatted_data)} historical candles for {symbol}")
        
        # Lưu vào cache
        save_klines_to_cache(symbol, timeframe, formatted_data)
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return []

def fix_data_gaps(symbol: str, timeframe: str, klines):
    """Check and fix data gaps by fetching missing candles.
    
    Args:
        symbol: Symbol to fix data gaps for
        timeframe: Timeframe of the data
        klines: The current klines data (deque or list)
        
    Returns:
        List or deque of candles with gaps filled
    """
    logger.debug(f"Checking for gaps in {symbol} data...")
    
    if not klines or len(klines) < 2:
        logger.warning(f"Not enough data to check for gaps in {symbol}")
        return klines
    
    # Sort data by time
    klines_list = list(klines) if isinstance(klines, deque) else klines
    klines_list.sort(key=lambda x: x['time'])
    
    # Get timeframe in milliseconds
    interval_ms = get_timeframe_ms(timeframe)
    
    # Check if we're missing the most recent candle
    current_time_ms = int(time.time() * 1000)
    expected_latest_candle_time = current_time_ms - (current_time_ms % interval_ms)
    
    # Account for potential delay in candle formation (wait for 10 seconds into the next candle)
    is_current_candle_expected = (current_time_ms % interval_ms) >= 10000
    
    # Only fetch the latest candle if we should have it by now
    if is_current_candle_expected:
        latest_candle_time = klines_list[-1]['time'] if klines_list else 0
        
        # Check if we're missing the most recent completed candle
        if latest_candle_time < expected_latest_candle_time - interval_ms:
            missing_count = (expected_latest_candle_time - latest_candle_time) // interval_ms
            logger.warning(f"{symbol} is missing the {missing_count} most recent candles. "
                          f"Latest: {datetime.fromtimestamp(latest_candle_time/1000)}, "
                          f"Expected: {datetime.fromtimestamp(expected_latest_candle_time/1000)}")
            
            # Fetch the most recent candles to fill in the gap at the end
            recent_candles_needed = min(missing_count + 5, 20)  # Add buffer, but don't request too many
            logger.info(f"Fetching {recent_candles_needed} recent candles for {symbol}...")
            recent_data = fetch_historical_klines(symbol, timeframe, recent_candles_needed)
            
            if recent_data:
                # Filter to only include candles we don't already have
                existing_times = {candle['time'] for candle in klines_list}
                new_candles = [c for c in recent_data if c['time'] not in existing_times]
                
                if new_candles:
                    logger.info(f"Adding {len(new_candles)} new recent candles to {symbol} data")
                    klines_list.extend(new_candles)
                    klines_list.sort(key=lambda x: x['time'])
                    logger.info(f"Updated {symbol} data. Now has candles from "
                               f"{datetime.fromtimestamp(klines_list[0]['time']/1000)} to "
                               f"{datetime.fromtimestamp(klines_list[-1]['time']/1000)}")
    
    # Check for internal gaps (more than 2 candles missing)
    large_gaps = []
    for i in range(1, len(klines_list)):
        time_diff = klines_list[i]['time'] - klines_list[i-1]['time']
        if time_diff > interval_ms * 3:  # Allow for small gaps (up to 2 missing candles)
            large_gaps.append({
                'start': klines_list[i-1]['time'], 
                'end': klines_list[i]['time'],
                'diff_minutes': time_diff / 1000 / 60,
                'index': i
            })
    
    if not large_gaps:
        logger.debug(f"No large gaps found in {symbol} data")
        return klines_list
    
    # Process gaps and fetch missing data
    logger.warning(f"Found {len(large_gaps)} large gaps in {symbol} data")
    for gap in large_gaps:
        logger.warning(f"Gap found: {gap['diff_minutes']:.1f} minutes between " 
                      f"{datetime.fromtimestamp(gap['start']/1000)} and "
                      f"{datetime.fromtimestamp(gap['end']/1000)}")
        
        # Calculate how many candles we should fetch to fill the gap
        candles_needed = int(gap['diff_minutes'] / (interval_ms / 1000 / 60)) + 5  # Add 5 extra candles as buffer
        
        # Fetch data from REST API for the gap period
        logger.info(f"Fetching {candles_needed} candles to fill gap in {symbol} data...")
        gap_data = fetch_historical_klines(symbol, timeframe, candles_needed)
        
        if gap_data:
            # Filter to only include candles within the gap
            gap_data = [c for c in gap_data if gap['start'] < c['time'] < gap['end']]
            
            if gap_data:
                logger.info(f"Found {len(gap_data)} candles to fill the gap in {symbol} data")
                
                # Insert the gap data into the original list
                before_gap = klines_list[:gap['index']]
                after_gap = klines_list[gap['index']:]
                klines_list = before_gap + gap_data + after_gap
                
                # Re-sort to ensure correct order
                klines_list.sort(key=lambda x: x['time'])
                logger.info(f"Gap filled. {symbol} now has {len(klines_list)} candles")
                
                # Lưu vào cache sau khi đã fix gap
                save_klines_to_cache(symbol, timeframe, klines_list)
            else:
                logger.warning(f"No suitable data found to fill gap in {symbol}")
    
    return klines_list

def determine_server_symbols(all_symbols: List[str]) -> tuple:
    """
    Xác định symbols nào thuộc về server nào dựa trên tên server.
    
    Args:
        all_symbols: Danh sách tất cả các symbols cần crawl
        
    Returns:
        tuple: (primary_symbols, secondary_symbols)
    """
    # Sắp xếp symbols để đảm bảo phân chia nhất quán
    sorted_symbols = sorted(all_symbols)
    
    # Chia đều symbols giữa hai server
    total = len(sorted_symbols)
    mid = total // 2
    
    primary = set(sorted_symbols[:mid])
    secondary = set(sorted_symbols[mid:])
    
    # Nếu số lượng symbols là lẻ, thêm symbol cuối cùng vào server primary
    if total % 2 != 0:
        primary.add(sorted_symbols[-1])
    
    return primary, secondary

def get_assigned_symbols(all_symbols: List[str]) -> Set[str]:
    """Lấy danh sách symbols được phân công cho server này."""
    global primary_symbols, secondary_symbols
    
    # Xác định phân chia symbols
    primary_symbols, secondary_symbols = determine_server_symbols(all_symbols)
    
    # Trả về symbols phù hợp với server ID
    if SERVER_ID == "primary":
        logger.info(f"Primary server assigned {len(primary_symbols)} symbols")
        return primary_symbols
    else:
        logger.info(f"Secondary server assigned {len(secondary_symbols)} symbols")
        return secondary_symbols

def start_websocket_client():
    """Khởi động WebSocket client."""
    global ws_client
    
    try:
        logger.info("Starting WebSocket client...")
        
        # Khởi tạo client với kích thước buffer phù hợp
        ws_client = BingxWebSocketClient(max_data_points=MAX_KLINE_CANDLES)
        
        # Bắt đầu client (chạy trong thread riêng)
        ws_client.start()
        
        # Đợi để kết nối thiết lập
        time.sleep(3)
        
        # Kiểm tra kết nối thành công
        if not ws_client.is_connected():
            logger.error("WebSocket client failed to connect after 3 seconds")
            return False
            
        logger.info("WebSocket client connected successfully")
        return True
    except Exception as e:
        logger.error(f"Error starting WebSocket client: {e}")
        logger.error(traceback.format_exc())
        return False

def subscribe_to_symbols(symbols: List[str], timeframe: str):
    """
    Đăng ký các symbols vào WebSocket.
    
    Args:
        symbols: Danh sách các symbols cần đăng ký
        timeframe: Timeframe cần đăng ký
    """
    global ws_client
    
    if not ws_client or not ws_client.is_connected():
        logger.error("WebSocket client is not connected")
        return False
    
    try:
        # Chia thành các batch để tránh quá tải
        batch_size = 10
        subscription_batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
        
        for i, batch in enumerate(subscription_batches):
            logger.info(f"Processing subscription batch {i+1}/{len(subscription_batches)}")
            
            for symbol in batch:
                full_symbol = f"{symbol}-USDT"
                if ws_client.subscribe_kline(full_symbol, timeframe):
                    logger.info(f"Subscribed to {full_symbol} {timeframe}")
                else:
                    logger.error(f"Failed to subscribe to {full_symbol} {timeframe}")
                
                time.sleep(0.2)  # Tránh rate limiting
            
            if i < len(subscription_batches) - 1:
                logger.info("Waiting 1 second between subscription batches...")
                time.sleep(1)
                
        return True
    except Exception as e:
        logger.error(f"Error subscribing to symbols: {e}")
        logger.error(traceback.format_exc())
        return False

def initialize_websocket_data(symbols: List[str], timeframe: str, limit: int = 1000):
    """
    Khởi tạo dữ liệu websocket từ dữ liệu lịch sử.
    
    Args:
        symbols: Danh sách các symbols cần khởi tạo
        timeframe: Timeframe cần lấy dữ liệu
        limit: Số lượng nến cần lấy
    """
    global ws_client
    
    try:
        logger.info(f"Initializing WebSocket data for {len(symbols)} symbols...")
        
        if not ws_client or not ws_client.is_connected():
            logger.error("WebSocket client is not connected")
            return False
            
        # Chỉ khởi tạo cho các symbols đã được gán cho server này
        assigned_symbols = get_assigned_symbols(symbols)
        symbols_to_initialize = [s for s in symbols if s in assigned_symbols]
        
        for symbol in symbols_to_initialize:
            try:
                # Kiểm tra xem đã có dữ liệu trong WebSocket chưa
                existing_data = []
                with ws_client._lock:
                    if symbol in ws_client.kline_data and timeframe in ws_client.kline_data[symbol]:
                        existing_data = list(ws_client.kline_data[symbol][timeframe])
                
                # Nếu đã có đủ dữ liệu, bỏ qua
                if existing_data and len(existing_data) >= limit:
                    logger.info(f"Symbol {symbol} already has enough data: {len(existing_data)}/{limit}")
                    continue
                    
                # Lấy dữ liệu lịch sử từ cache hoặc REST API
                klines = load_klines_from_cache(symbol, timeframe)
                
                # Nếu không có cache hoặc cache không đủ dữ liệu, lấy từ REST API
                if not klines or len(klines) < limit:
                    logger.info(f"Fetching historical data for {symbol}")
                    klines = fetch_historical_klines(symbol, timeframe, limit)
                
                # Kiểm tra và sửa gaps
                klines = fix_data_gaps(symbol, timeframe, klines)
                
                if klines:
                    # Lưu vào WebSocket client
                    with ws_client._lock:
                        if symbol not in ws_client.kline_data:
                            ws_client.kline_data[symbol] = {}
                        ws_client.kline_data[symbol][timeframe] = deque(klines, maxlen=MAX_KLINE_CANDLES)
                    logger.info(f"Initialized {len(klines)} candles for {symbol}")
                else:
                    logger.warning(f"Failed to initialize data for {symbol}")
                    
            except Exception as e:
                logger.error(f"Error initializing data for {symbol}: {e}")
                logger.error(traceback.format_exc())
                continue
                
        # Subscribe to WebSocket
        subscribe_to_symbols(list(assigned_symbols), timeframe)
        
        return True
    except Exception as e:
        logger.error(f"Error initializing WebSocket data: {e}")
        logger.error(traceback.format_exc())
        return False

@app.on_event("startup")
async def startup_event():
    """Khởi động các dịch vụ khi server bắt đầu."""
    # Khởi động WebSocket client
    if not start_websocket_client():
        logger.error("Failed to start WebSocket client")
        return
    
    # Remove the background sync thread since we're removing secondary-to-primary communication
    logger.info(f"Server started with ID: {SERVER_ID}")

@app.get("/api/health", response_model=HealthCheckResponse)
async def health_check():
    """Endpoint kiểm tra trạng thái của server."""
    global ws_client, start_time
    
    # Kiểm tra WebSocket
    if not ws_client or not ws_client.is_connected():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WebSocket client is not connected"
        )
    
    # Đếm số lượng symbols đã subscribe
    subscribed_count = len(ws_client.subscriptions)
    
    # Tính uptime
    uptime_seconds = (datetime.now() - start_time).total_seconds()
    
    return HealthCheckResponse(
        status="ok",
        server_id=SERVER_ID,
        uptime=uptime_seconds,
        subscribed_symbols=subscribed_count,
        start_time=start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

@app.post("/api/symbols")
async def update_symbols(request: SymbolRequest, background_tasks: BackgroundTasks):
    """
    Cập nhật danh sách symbols cần theo dõi và khởi tạo dữ liệu.
    
    Args:
        request: Thông tin về symbols cần theo dõi
    """
    try:
        symbols = request.symbols
        timeframe = request.timeframe
        limit = request.limit
        
        # Xác định symbols được gán cho server này
        assigned_symbols = get_assigned_symbols(symbols)
        
        # Khởi tạo dữ liệu trong background task
        background_tasks.add_task(initialize_websocket_data, symbols, timeframe, limit)
        
        return {
            "message": f"Processing {len(assigned_symbols)}/{len(symbols)} symbols in background",
            "assigned_symbols": list(assigned_symbols)
        }
    except Exception as e:
        logger.error(f"Error updating symbols: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.get("/api/klines/{symbol}")
async def get_symbol_klines(symbol: str, timeframe: str = "5m", limit: int = 1000):
    """
    Lấy dữ liệu OHLCV cho một symbol.
    
    Args:
        symbol: Symbol cần lấy dữ liệu
        timeframe: Timeframe cần lấy dữ liệu
        limit: Số lượng nến cần lấy
    """
    global ws_client
    
    try:
        # Kiểm tra xem symbol có thuộc server này không
        if SERVER_ID == "primary":
            is_assigned = symbol in primary_symbols
        else:
            is_assigned = symbol in secondary_symbols
            
        if not is_assigned:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Symbol {symbol} is not assigned to this server"
            )
        
        # Kiểm tra WebSocket client
        if not ws_client or not ws_client.is_connected():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="WebSocket client is not connected"
            )
            
        # Lấy dữ liệu từ WebSocket
        klines = ws_client.get_klines(symbol, timeframe)
        
        # Nếu không có dữ liệu từ WebSocket, thử lấy từ cache hoặc REST API
        if not klines or len(klines) == 0:
            # Thử lấy từ cache
            klines = load_klines_from_cache(symbol, timeframe)
            
            # Nếu không có cache, lấy từ REST API
            if not klines or len(klines) == 0:
                klines = fetch_historical_klines(symbol, timeframe, limit)
                
                if not klines or len(klines) == 0:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"No data found for {symbol} {timeframe}"
                    )
        
        # Fix gaps trong dữ liệu
        klines = fix_data_gaps(symbol, timeframe, klines)
        
        # Giới hạn số lượng nến trả về
        klines_list = list(klines)[-limit:] if len(klines) > limit else list(klines)
        
        # Đảm bảo đã sắp xếp theo thời gian
        klines_list.sort(key=lambda x: x['time'])
        
        # Chuyển đổi dữ liệu để gửi qua JSON
        result = convert_numpy_to_python(klines_list)
        
        return {"symbol": symbol, "timeframe": timeframe, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting klines for {symbol}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/api/batch_klines")
async def get_batch_klines(request: SymbolRequest):
    """
    Lấy dữ liệu OHLCV cho nhiều symbols cùng lúc.
    
    Args:
        request: Thông tin về symbols cần lấy dữ liệu
    """
    global ws_client
    
    try:
        symbols = request.symbols
        timeframe = request.timeframe
        limit = request.limit
        
        # Lọc các symbols thuộc về server này
        if SERVER_ID == "primary":
            assigned_symbols = [s for s in symbols if s in primary_symbols]
        else:
            assigned_symbols = [s for s in symbols if s in secondary_symbols]
            
        if not assigned_symbols:
            return {"message": "No assigned symbols for this server", "data": {}}
        
        # Kiểm tra WebSocket client
        if not ws_client or not ws_client.is_connected():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="WebSocket client is not connected"
            )
            
        result = {}
        
        for symbol in assigned_symbols:
            try:
                # Lấy dữ liệu từ WebSocket
                klines = ws_client.get_klines(symbol, timeframe)
                
                # Nếu không có dữ liệu từ WebSocket, thử lấy từ cache hoặc REST API
                if not klines or len(klines) == 0:
                    # Thử lấy từ cache
                    klines = load_klines_from_cache(symbol, timeframe)
                    
                    # Nếu không có cache, lấy từ REST API
                    if not klines or len(klines) == 0:
                        klines = fetch_historical_klines(symbol, timeframe, limit)
                
                if klines and len(klines) > 0:
                    # Fix gaps trong dữ liệu
                    klines = fix_data_gaps(symbol, timeframe, klines)
                    
                    # Giới hạn số lượng nến trả về
                    klines_list = list(klines)[-limit:] if len(klines) > limit else list(klines)
                    
                    # Đảm bảo đã sắp xếp theo thời gian
                    klines_list.sort(key=lambda x: x['time'])
                    
                    # Chuyển đổi dữ liệu để gửi qua JSON
                    result[symbol] = convert_numpy_to_python(klines_list)
                else:
                    logger.warning(f"No data found for {symbol} {timeframe}")
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                logger.error(traceback.format_exc())
                # Bỏ qua symbol lỗi nhưng vẫn tiếp tục xử lý các symbol khác
                continue
                
        return {"timeframe": timeframe, "data": result}
    except Exception as e:
        logger.error(f"Error getting batch klines: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Hàm chuyển đổi numpy/pandas types sang các kiểu Python cơ bản
def convert_numpy_to_python(obj):
    """Convert numpy/pandas types to standard Python types for JSON serialization."""
    if isinstance(obj, dict):
        return {k: convert_numpy_to_python(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_to_python(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_to_python(item) for item in obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return convert_numpy_to_python(obj.tolist())
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

if __name__ == "__main__":
    # Khởi động server
    uvicorn.run(
        "data_service:app", 
        host="0.0.0.0",  # Listen on all network interfaces
        port=SERVER_PORT,
        reload=False     # Set to False in production
    ) 