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
        
        # Kiểm tra lỗi từ API
        if "code" not in data or data["code"] != 0:
            error_msg = data.get("msg", "Unknown error")
            logger.error(f"API error for {full_symbol}: {error_msg}")
            return []
            
        # Lấy dữ liệu từ response
        klines = data.get("data", [])
        
        # Format dữ liệu theo định dạng chuẩn
        formatted_klines = []
        
        for kline in klines:
            try:
                formatted_kline = {
                    "time": int(kline.get('time', 0)),  # timestamp
                    "open": float(kline.get('open', 0)),  # open
                    "high": float(kline.get('high', 0)),  # high
                    "low": float(kline.get('low', 0)),  # low
                    "close": float(kline.get('close', 0)),  # close
                    "volume": float(kline.get('volume', 0))  # volume
                }
                formatted_klines.append(formatted_kline)
            except (IndexError, ValueError) as e:
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