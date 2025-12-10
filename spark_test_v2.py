import os
import sys

# Print system information
print("System Information:")
print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")
print(f"Platform: {sys.platform}")

# Set environment variables for better Windows compatibility
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk-17'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Add Hadoop bin to PATH
hadoop_bin_path = os.path.join(os.environ['HADOOP_HOME'], 'bin')
if 'PATH' in os.environ:
    os.environ['PATH'] = hadoop_bin_path + ';' + os.environ['PATH']
else:
    os.environ['PATH'] = hadoop_bin_path

print("\nEnvironment Variables:")
for var in ['JAVA_HOME', 'HADOOP_HOME', 'PYSPARK_PYTHON']:
    print(f"{var}: {os.environ.get(var, 'Not set')}")

try:
    print("\nImporting PySpark...")
    from pyspark.sql import SparkSession
    print("✓ PySpark imported successfully")
    
    print("\nCreating Spark Session...")
    spark = SparkSession.builder \
        .appName("PySpark Test v2") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
        .config("spark.local.dir", "C:/temp") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    print("✓ Spark session created successfully")
    print(f"✓ Spark version: {spark.version}")
    
    # Set log level to ERROR to reduce noise
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\nTesting DataFrame operations...")
    
    # Create a simple DataFrame without showing it first
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    
    print("✓ DataFrame created successfully")
    
    # Try to perform an action that doesn't require showing data
    count = df.count()
    print(f"✓ DataFrame count: {count}")
    
    # Try aggregation
    avg_age = df.agg({"Age": "avg"}).collect()[0][0]
    print(f"✓ Average age: {avg_age}")
    
    spark.stop()
    print("\n✓ Spark session stopped successfully")
    print("\n🎉 PySpark is working correctly!")
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    print("\nFull traceback:")
    traceback.print_exc()