# Data Server for Pair Trading Bot

Đây là một server độc lập để xử lý WebSocket connections và crawl dữ liệu OHLCV cho Pair Trading Bot. Server này được thiết kế để hoạt động độc lập, không phụ thuộc vào dự án chính.

## Mục đích

Server này giúp vượt qua giới hạn 200 subscriptions của Bingx WebSocket bằng cách chia tải giữa nhiều servers.

## Tính năng

- REST API để lấy dữ liệu OHLCV
- Kết nối WebSocket đến Bingx để nhận dữ liệu real-time
- Tự động cân bằng tải symbols giữa server chính và phụ
- Phát hiện và điền các khoảng trống dữ liệu
- Cache dữ liệu lịch sử trên ổ đĩa

## Cài đặt

1. Cài đặt các thư viện phụ thuộc:

```
pip install -r requirements.txt
```

2. Cấu hình server thông qua biến môi trường:

```
SERVER_ID=secondary  # Dùng "primary" hoặc "secondary"
SERVER_PORT=8080
MAIN_SERVER_URL=http://địa-chỉ-server-chính:8000  # URL của server chính (chỉ cần thiết cho server phụ)
```

3. Chạy server:

```
python data_service.py
```

## API Endpoints

- `GET /api/health` - Kiểm tra trạng thái server
- `POST /api/symbols` - Cập nhật danh sách symbols cần theo dõi
- `GET /api/klines/{symbol}` - Lấy dữ liệu OHLCV cho một symbol cụ thể
- `POST /api/batch_klines` - Lấy dữ liệu OHLCV cho nhiều symbols

## Cân bằng tải

Server tự động chia symbols giữa server chính và phụ:

- Server chính xử lý nửa đầu tiên của danh sách symbols (sắp xếp theo thứ tự bảng chữ cái)
- Server phụ xử lý nửa còn lại của danh sách symbols

## Đồng bộ hóa

Server phụ tự động đồng bộ với server chính để lấy danh sách symbols mới nhất cần theo dõi.

## Xử lý lỗi

- Tự động phát hiện và xử lý lỗi kết nối WebSocket
- Tự động phát hiện và điền các khoảng trống dữ liệu
- Cơ chế thử lại cho các yêu cầu API thất bại

Running as a Windows Service (Optional)
For a more robust setup, you can run the application as a Windows service using NSSM (Non-Sucking Service Manager):
Download NSSM from nssm.cc
Extract the ZIP file
Run the appropriate version (32-bit or 64-bit) of nssm.exe
Use these commands to install as a service:

nssm install DataService "C:\path\to\python.exe" "C:\path\to\data_server\data_service.py"
nssm set DataService AppDirectory "C:\path\to\project"
nssm set DataService AppEnvironment "SERVER_ID=secondary^SERVER_PORT=8080"
nssm start DataService


Configure Windows Firewall:
Open Windows Defender Firewall with Advanced Security
Click on "Inbound Rules" and select "New Rule..."
Choose "Port" and click "Next"
Select "TCP" and enter your port number (e.g., 8080) then click "Next"
Select "Allow the connection" and click "Next"
Check all network types and click "Next"
Name your rule (e.g., "Data Service API") and click "Finish"