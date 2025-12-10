from pyspark.sql import SparkSession
import os
import sys

# Set environment variables to avoid path issues
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def test_spark_setup():
    """
    Test if PySpark is properly configured
    """
    try:
        # Create a simple Spark session
        spark = SparkSession.builder \
            .appName("PySpark Setup Test") \
            .master("local[*]") \
            .getOrCreate()
        
        print("✓ Spark session created successfully!")
        print(f"✓ Spark version: {spark.version}")
        print(f"✓ Spark application ID: {spark.sparkContext.applicationId}")
        
        # Test with a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["Name", "Age"]
        
        df = spark.createDataFrame(data, columns)
        df.show()
        
        # Perform a simple operation
        avg_age = df.agg({"Age": "avg"}).collect()[0][0]
        print(f"✓ Average age: {avg_age}")
        
        # Stop the session
        spark.stop()
        print("✓ Spark session stopped successfully!")
        
        return True
        
    except Exception as e:
        print(f"✗ Error setting up PySpark: {str(e)}")
        return False

if __name__ == "__main__":
    print("Testing PySpark setup...")
    success = test_spark_setup()
    if success:
        print("\n🎉 PySpark is properly configured!")
    else:
        print("\n❌ PySpark setup has issues. Please check the error messages above.")