@echo off
echo ============================================================
echo Starting Music Streaming Application with Spark Backend
echo ============================================================
echo.

echo [1/2] Starting Node.js Spark UI Proxy on port 4040...
start /B cmd /c "npm start > spark-proxy.log 2>&1"
timeout /t 2 /nobreak >nul

echo [2/2] Starting PySpark Backend on port 5000...
start /B cmd /c "python spark_backend.py > spark-backend.log 2>&1"
timeout /t 3 /nobreak >nul

echo.
echo ============================================================
echo All services started successfully!
echo ============================================================
echo.
echo Frontend:        http://localhost:4040 (Spark UI Proxy)
echo PySpark Backend: http://localhost:5000 (Flask API)
echo Real Spark UI:   http://localhost:4041 (PySpark Web UI)
echo.
echo Press any key to stop all services...
pause >nul

echo.
echo Stopping all services...
taskkill /F /IM node.exe >nul 2>&1
taskkill /F /IM python.exe >nul 2>&1

echo All services stopped.
pause
