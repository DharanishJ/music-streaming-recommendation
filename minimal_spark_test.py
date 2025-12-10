import os
import sys

print("=== Minimal PySpark Test ===")
print(f"Python: {sys.version}")
print(f"Executable: {sys.executable}")

# Set environment variables
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk-17'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")

try:
    # Import PySpark
    from pyspark.sql import SparkSession
    print("✓ PySpark imported successfully")
    
    # Create minimal Spark session
    spark = SparkSession.builder \
        .appName("MinimalTest") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
        .getOrCreate()
    
    print(f"✓ Spark session created (version: {spark.version})")
    
    # Just check if we can access the SparkContext
    sc = spark.sparkContext
    print(f"✓ SparkContext accessible (app ID: {sc.applicationId})")
    
    spark.stop()
    print("✓ Spark session stopped")
    print("\n🎉 Minimal PySpark test PASSED!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()