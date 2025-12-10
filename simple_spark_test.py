import os
import sys

# Get the Python executable path
python_executable = sys.executable
print(f"Using Python executable: {python_executable}")

# Set environment variables
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk-17'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PYSPARK_PYTHON'] = python_executable
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable

# Add Hadoop bin to PATH
if 'PATH' in os.environ:
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']
else:
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin'

print("Environment variables set:")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
print(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME', 'Not set')}")
print(f"PATH contains hadoop: {'hadoop' in os.environ.get('PATH', '').lower()}")
print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")

try:
    from pyspark.sql import SparkSession
    print("✓ PySpark imported successfully")
    
    # Create Spark session with additional Windows-compatible configurations
    spark = SparkSession.builder \
        .appName("SimpleTest") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
        .config("spark.local.dir", "C:/temp") \
        .getOrCreate()
    
    print("✓ Spark session created successfully")
    print(f"✓ Spark version: {spark.version}")
    
    # Set log level to ERROR to reduce noise
    spark.sparkContext.setLogLevel("ERROR")
    
    # Test with simple data
    data = [("Hello", 1), ("World", 2)]
    df = spark.createDataFrame(data, ["word", "count"])
    df.show()
    
    spark.stop()
    print("✓ Spark session stopped successfully")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()