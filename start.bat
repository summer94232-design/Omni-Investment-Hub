@echo off
echo 啟動 Docker 容器...
docker compose up -d

echo 等待服務就緒...
timeout /t 5 /nobreak

echo 啟動 Dashboard...
call venv\Scripts\activate.bat
python dashboard.py