import time
import json
import logging
import requests
import traceback
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# URL API của Bingx
BASE_URL = "https://open-api.bingx.com"
KLINE_ENDPOINT = "/openApi/swap/v2/quote/klines"

def get_ohclv(symbol: str, timeframe: str = "5m", limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Lấy dữ liệu OHLCV từ Bingx REST API.
    
    Args:
        symbol: Symbol cần lấy dữ liệu (không bao gồm -USDT)
        timeframe: Timeframe cần lấy dữ liệu (1m, 5m, 15m, 1h, 4h, 1d, 1w)
        limit: Số lượng nến cần lấy (tối đa 1000)
        
    Returns:
        List[Dict[str, Any]]: Danh sách các nến OHLCV
    """
    try:
        # Map timeframe từ định dạng '5m' sang định dạng API '5min'
        interval_map = {
            "1m": "1m", 
            "3m": "3m", 
            "5m": "5m", 
            "15m": "15m", 
            "30m": "30m",
            "1h": "1h", 
            "4h": "4h", 
            "1d": "1d", 
            "1w": "1w", 
            "1M": "1M"
        }
        
        interval = interval_map.get(timeframe, "5m")
        
        # Thêm -USDT vào symbol nếu cần
        full_symbol = f"{symbol}-USDT" if "-" not in symbol else symbol
        
        # Chuẩn bị tham số
        params = {
            "symbol": full_symbol,
            "interval": interval,
            "limit": min(limit, 1000)  # API có giới hạn 1000 nến
        }
        
        # Gọi API
        logger.info(f"Fetching OHLCV data for {full_symbol} ({interval}), limit={params['limit']}")
        response = requests.get(f"{BASE_URL}{KLINE_ENDPOINT}", params=params, timeout=10)
        
        # Kiểm tra phản hồi
        if response.status_code != 200:
            logger.error(f"Error fetching OHLCV data for {full_symbol}: HTTP {response.status_code}")
            logger.error(f"Response: {response.text}")
            return []
            
        # Parse JSON
        data = response.json()
        
        # Log the raw response structure for debugging
        logger.debug(f"Raw API response structure: {type(data)}")
        
        # Kiểm tra lỗi từ API
        if "code" not in data or data["code"] != 0:
            error_msg = data.get("msg", "Unknown error")
            logger.error(f"API error for {full_symbol}: {error_msg}")
            return []
            
        # Lấy dữ liệu từ response
        klines_data = data.get("data", [])
        
        # Xử lý trường hợp data là dictionary đơn thay vì list
        if isinstance(klines_data, dict):
            logger.info(f"Received single candle for {full_symbol}, converting to list format")
            klines = [klines_data]  # Chuyển đổi thành list chứa 1 phần tử
        elif isinstance(klines_data, list):
            klines = klines_data
        elif isinstance(klines_data, str):
            # Thử parse nếu là JSON string
            try:
                parsed_data = json.loads(klines_data)
                if isinstance(parsed_data, dict):
                    klines = [parsed_data]
                elif isinstance(parsed_data, list):
                    klines = parsed_data
                else:
                    logger.error(f"Unexpected parsed data type: {type(parsed_data)}")
                    return []
            except json.JSONDecodeError:
                logger.error(f"Failed to parse data as JSON: {klines_data}")
                return []
        else:
            logger.error(f"Unexpected data format for {full_symbol}. Expected list or dict, got {type(klines_data)}")
            logger.error(f"Raw response data: {data}")
            return []
        
        # Format dữ liệu theo định dạng chuẩn
        formatted_klines = []
        
        for kline in klines:
            try:
                # Handle different possible formats
                if isinstance(kline, dict):
                    # Standard dictionary format
                    formatted_kline = {
                        "time": int(kline.get('time', 0)),
                        "open": float(kline.get('open', 0)),
                        "high": float(kline.get('high', 0)),
                        "low": float(kline.get('low', 0)),
                        "close": float(kline.get('close', 0)),
                        "volume": float(kline.get('volume', 0))
                    }
                elif isinstance(kline, list) and len(kline) >= 6:
                    # Array format [time, open, high, low, close, volume]
                    formatted_kline = {
                        "time": int(kline[0]),
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5])
                    }
                elif isinstance(kline, str):
                    # Sometimes data might be strings - try to parse as JSON
                    logger.warning(f"Received string instead of object/array: {kline}")
                    try:
                        parsed = json.loads(kline)
                        if isinstance(parsed, dict):
                            formatted_kline = {
                                "time": int(parsed.get('time', 0)),
                                "open": float(parsed.get('open', 0)),
                                "high": float(parsed.get('high', 0)),
                                "low": float(parsed.get('low', 0)),
                                "close": float(parsed.get('close', 0)),
                                "volume": float(parsed.get('volume', 0))
                            }
                        else:
                            logger.warning(f"Parsed string but got unexpected format: {parsed}")
                            continue
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse string as JSON: {kline}")
                        continue
                else:
                    logger.warning(f"Unexpected kline format: {type(kline)}")
                    continue
                
                # Validate timestamp
                if formatted_kline["time"] <= 0:
                    logger.warning(f"Invalid timestamp: {formatted_kline['time']}")
                    continue
                    
                formatted_klines.append(formatted_kline)
            except (IndexError, ValueError, TypeError) as e:
                logger.warning(f"Error parsing kline: {kline} - {e}")
                continue
                
        # Sắp xếp theo thời gian từ cũ đến mới
        formatted_klines.sort(key=lambda x: x["time"])
        
        logger.info(f"Successfully fetched {len(formatted_klines)} candles for {full_symbol}")
        return formatted_klines
        
    except Exception as e:
        logger.error(f"Error fetching OHLCV data for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return []

def check_symbol_exists(symbol: str) -> bool:
    """
    Kiểm tra xem symbol có tồn tại trên Bingx không.
    
    Args:
        symbol: Symbol cần kiểm tra (có thể có -USDT hoặc không)
    
    Returns:
        bool: True nếu symbol tồn tại, False nếu không
    """
    try:
        # Thêm -USDT vào symbol nếu cần
        full_symbol = f"{symbol}-USDT" if "-" not in symbol else symbol
        
        # Thử lấy dữ liệu nến để kiểm tra symbol tồn tại
        response = requests.get(
            f"{BASE_URL}{KLINE_ENDPOINT}", 
            params={"symbol": full_symbol, "interval": "5m", "limit": 1},
            timeout=5
        )
        
        # Kiểm tra phản hồi
        if response.status_code != 200:
            logger.warning(f"Symbol {full_symbol} check failed with HTTP {response.status_code}")
            return False
            
        # Parse JSON
        data = response.json()
        
        # Kiểm tra lỗi từ API
        if "code" not in data or data["code"] != 0:
            return False
            
        # Nếu có dữ liệu, symbol tồn tại
        return True
        
    except Exception as e:
        logger.error(f"Error checking if symbol {symbol} exists: {e}")
        return False 