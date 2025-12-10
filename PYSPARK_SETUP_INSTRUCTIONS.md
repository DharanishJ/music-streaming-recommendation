# Complete PySpark Setup Guide for Windows

This guide will help you set up PySpark correctly on Windows to avoid common issues like Python worker crashes.

## Current Status

Your PySpark installation is working at a basic level - you can create Spark sessions and access SparkContext. However, you're experiencing issues with DataFrame operations due to Python worker communication problems.

## Root Cause

The issue is caused by using the **Windows Store Python installation**, which has permission and communication restrictions that prevent PySpark from launching Python worker processes correctly.

## Solution Options

### Option 1: Install Standard Python (Recommended)

1. **Uninstall Windows Store Python** (optional but recommended)
   - Go to Settings → Apps → Installed apps
   - Find Python and uninstall it

2. **Install Python from python.org**
   - Download Python 3.12 from https://www.python.org/downloads/
   - During installation, make sure to check "Add Python to PATH"

3. **Reinstall dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Update environment variables** in your scripts to point to the new Python installation

### Option 2: Use Python Virtual Environment

Create a virtual environment with a standard Python installation:

```bash
# If you have standard Python installed
python -m venv pyspark_env
pyspark_env\Scripts\activate
pip install -r requirements.txt
```

### Option 3: Modify Your Scripts for Windows Store Python

If you must continue using Windows Store Python, you can try these additional configurations in your Spark session:

```python
spark = SparkSession.builder \
    .appName("YourApp") \
    .master("local[1]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()
```

## Verification Steps

After implementing any solution, verify your setup with this test:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[1]") \
    .getOrCreate()

# This should work without errors
df = spark.createDataFrame([("test", 1)], ["col1", "col2"])
df.show()  # This is where most issues occur

spark.stop()
print("✓ PySpark is working correctly!")
```

## Environment Variables

Ensure these environment variables are set correctly:

```batch
JAVA_HOME=C:\Program Files\Java\jdk-17
HADOOP_HOME=C:\hadoop
PYSPARK_PYTHON=python
PYSPARK_DRIVER_PYTHON=python
```

## Hadoop WinUtils

Make sure you have WinUtils properly installed:

1. Create directory: `C:\hadoop\bin`
2. Download these files to that directory:
   - `winutils.exe` from https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe
   - `hadoop.dll` from https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll

## Running Your Application

Once you've fixed the Python installation issue:

1. Start the backend:
   ```bash
   python spark_backend.py
   ```

2. Start the frontend proxy (in another terminal):
   ```bash
   npm start
   ```

3. Access the applications:
   - Frontend: http://localhost:4040
   - Backend API: http://localhost:5000
   - Spark UI: http://localhost:4041

## Additional Tips

1. **Run as Administrator**: Try running your command prompt as Administrator
2. **Firewall**: Ensure Windows Firewall isn't blocking the connections
3. **Antivirus**: Some antivirus software can interfere with Spark's temporary file creation

## Need More Help?

If you continue to experience issues:

1. Check the full logs in `spark-backend.log`
2. Try reducing parallelism: `.master("local[1]")` instead of `.master("local[*]")`
3. Consider using Docker for a consistent environment