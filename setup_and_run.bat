@echo off
echo Setting up PySpark environment...

REM Set Java Home (adjust this path if your Java installation is elsewhere)
set JAVA_HOME=C:\Program Files\Java\jdk-17
if not exist "%JAVA_HOME%" (
    echo Warning: JAVA_HOME not found at %JAVA_HOME%
    echo Trying to find Java installation...
    for /f "tokens=*" %%i in ('where java') do set JAVA_PATH=%%i
    if defined JAVA_PATH (
        for %%i in ("%JAVA_PATH%") do set JAVA_DIR=%%~dpi
        set JAVA_HOME=%JAVA_DIR:~0,-5%
        echo Found Java at %JAVA_HOME%
    )
)

REM Set Hadoop Home
set HADOOP_HOME=C:\hadoop
echo HADOOP_HOME set to %HADOOP_HOME%

REM Add Hadoop bin to PATH
set PATH=%HADOOP_HOME%\bin;%PATH%
echo Added %HADOOP_HOME%\bin to PATH

echo.
echo Starting Music Streaming Application with Spark Backend
echo ============================================================

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
echo Press Ctrl+C to stop all services...
pause >nul

echo.
echo Stopping all services...
taskkill /F /IM node.exe >nul 2>&1
taskkill /F /IM python.exe >nul 2>&1

echo All services stopped.
pause