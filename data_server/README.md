# Data Server for Pair Trading Bot

This server provides a separate instance for handling WebSocket connections and OHLCV data crawling for the Pair Trading Bot.

## Purpose

The main purpose of this server is to overcome the Bingx WebSocket limit of 200 subscriptions by splitting the subscription load between multiple servers.

## Features

- REST API for fetching OHLCV data
- WebSocket connection to Bingx for real-time data
- Automatic load balancing of symbols between primary and secondary servers
- Data gap detection and filling
- Local caching of historical data

## Setup

1. Install dependencies:

```
pip install -r requirements.txt
```

2. Configure the server using environment variables:

```
SERVER_ID=secondary  # Use "primary" or "secondary"
SERVER_PORT=8080
MAIN_SERVER_URL=http://localhost:8000  # URL of the main server (only needed for secondary)
```

3. Run the server:

```
python data_service.py
```

## API Endpoints

- `GET /api/health` - Health check endpoint
- `POST /api/symbols` - Update the list of symbols to track
- `GET /api/klines/{symbol}` - Get OHLCV data for a specific symbol
- `POST /api/batch_klines` - Get OHLCV data for multiple symbols

## Load Balancing

The server automatically divides symbols between primary and secondary servers:

- Primary server handles the first half of alphabetically sorted symbols
- Secondary server handles the second half of alphabetically sorted symbols

## Synchronization

The secondary server automatically syncs with the primary server to get the latest list of symbols to track.

## Error Handling

- WebSocket connection failures are automatically detected
- Data gaps are automatically detected and filled
- Failed API requests are logged and retry mechanisms are in place
