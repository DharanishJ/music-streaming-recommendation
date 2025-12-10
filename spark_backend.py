import os
import sys

# Set environment variables early, before importing PySpark
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

from pyspark.sql import SparkSession
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
import time
import random

# Set environment variables to avoid path issues
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

try:
    # Initialize Spark Session with Windows-compatible configurations
    spark = SparkSession.builder \
        .appName("MusicStreamingSparkBackend") \
        .master("local[1]") \
        .config("spark.ui.port", "4041") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
        .config("spark.local.dir", "C:/temp") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()
    
    print(f"✓ Spark Session created successfully!")
    print(f"✓ Spark UI available at: http://localhost:4041")
    print(f"✓ Application ID: {spark.sparkContext.applicationId}")
except Exception as e:
    print(f"⚠ Warning: Could not create Spark session: {e}")
    print("⚠ Falling back to simulated mode...")
    spark = None

# Store job results
job_results = []

@app.route('/')
def home():
    """Home endpoint with Spark logo"""
    if spark:
        spark_logo_html = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Apache Spark Backend - Running</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                }
                .container {
                    background: white;
                    padding: 40px;
                    border-radius: 20px;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.3);
                    text-align: center;
                    max-width: 600px;
                }
                .spark-logo {
                    width: 200px;
                    height: auto;
                    margin-bottom: 20px;
                }
                h1 {
                    color: #e25a1c;
                    margin: 20px 0;
                }
                .status {
                    background: #4CAF50;
                    color: white;
                    padding: 10px 20px;
                    border-radius: 25px;
                    display: inline-block;
                    margin: 20px 0;
                    font-weight: bold;
                }
                .info-box {
                    background: #f5f5f5;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 20px 0;
                    text-align: left;
                }
                .info-item {
                    padding: 8px 0;
                    border-bottom: 1px solid #ddd;
                }
                .info-item:last-child {
                    border-bottom: none;
                }
                .api-link {
                    color: #667eea;
                    text-decoration: none;
                    font-weight: bold;
                }
                .api-link:hover {
                    text-decoration: underline;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <!-- Apache Spark Logo (SVG) -->
                <svg class="spark-logo" viewBox="0 0 400 200" xmlns="http://www.w3.org/2000/svg">
                    <defs>
                        <linearGradient id="sparkGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                            <stop offset="0%" style="stop-color:#e25a1c;stop-opacity:1" />
                            <stop offset="100%" style="stop-color:#f79c42;stop-opacity:1" />
                        </linearGradient>
                    </defs>
                    <!-- Spark Icon -->
                    <path d="M 200 50 L 220 90 L 260 95 L 225 125 L 235 165 L 200 145 L 165 165 L 175 125 L 140 95 L 180 90 Z" 
                          fill="url(#sparkGradient)" stroke="#e25a1c" stroke-width="2"/>
                    <!-- Text: Apache Spark -->
                    <text x="200" y="190" font-family="Arial, sans-serif" font-size="24" font-weight="bold" 
                          fill="#e25a1c" text-anchor="middle">Apache Spark™</text>
                </svg>
                
                <h1>🎵 Music Streaming Backend</h1>
                <div class="status">✓ PySpark Backend Running</div>
                
                <div class="info-box">
                    <div class="info-item"><strong>Application ID:</strong> ''' + spark.sparkContext.applicationId + '''</div>
                    <div class="info-item"><strong>Spark Version:</strong> ''' + spark.version + '''</div>
                    <div class="info-item"><strong>Master:</strong> ''' + spark.sparkContext.master + '''</div>
                    <div class="info-item"><strong>Flask API:</strong> <a class="api-link" href="http://localhost:5000">http://localhost:5000</a></div>
                    <div class="info-item"><strong>Spark UI:</strong> <a class="api-link" href="http://localhost:4041">http://localhost:4041</a></div>
                </div>
                
                <p style="color: #666; margin-top: 20px;">
                    <strong>API Endpoints:</strong><br>
                    <a class="api-link" href="/api/spark/info">Spark Info</a> |
                    <a class="api-link" href="/api/spark/metrics">Metrics</a> |
                    <a class="api-link" href="/api/spark/jobs">Jobs</a>
                </p>
            </div>
        </body>
        </html>
        '''
    else:
        spark_logo_html = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>PySpark Backend - Initializing</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                }
                .container {
                    background: white;
                    padding: 40px;
                    border-radius: 20px;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.3);
                    text-align: center;
                    max-width: 600px;
                }
                .spark-logo {
                    width: 200px;
                    height: auto;
                    margin-bottom: 20px;
                }
                h1 {
                    color: #e25a1c;
                    margin: 20px 0;
                }
                .status {
                    background: #FF9800;
                    color: white;
                    padding: 10px 20px;
                    border-radius: 25px;
                    display: inline-block;
                    margin: 20px 0;
                    font-weight: bold;
                }
                .info-box {
                    background: #f5f5f5;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 20px 0;
                    text-align: left;
                }
                .info-item {
                    padding: 8px 0;
                    border-bottom: 1px solid #ddd;
                }
                .info-item:last-child {
                    border-bottom: none;
                }
                .api-link {
                    color: #667eea;
                    text-decoration: none;
                    font-weight: bold;
                }
                .api-link:hover {
                    text-decoration: underline;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <!-- Apache Spark Logo (SVG) -->
                <svg class="spark-logo" viewBox="0 0 400 200" xmlns="http://www.w3.org/2000/svg">
                    <defs>
                        <linearGradient id="sparkGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                            <stop offset="0%" style="stop-color:#e25a1c;stop-opacity:1" />
                            <stop offset="100%" style="stop-color:#f79c42;stop-opacity:1" />
                        </linearGradient>
                    </defs>
                    <!-- Spark Icon -->
                    <path d="M 200 50 L 220 90 L 260 95 L 225 125 L 235 165 L 200 145 L 165 165 L 175 125 L 140 95 L 180 90 Z" 
                          fill="url(#sparkGradient)" stroke="#e25a1c" stroke-width="2"/>
                    <!-- Text: Apache Spark -->
                    <text x="200" y="190" font-family="Arial, sans-serif" font-size="24" font-weight="bold" 
                          fill="#e25a1c" text-anchor="middle">Apache Spark™</text>
                </svg>
                        
                <h1>🎵 Music Streaming Backend</h1>
                <div class="status">⚠ PySpark Initializing...</div>
                        
                <div class="info-box">
                    <div class="info-item"><strong>Status:</strong> Spark session not available</div>
                    <div class="info-item"><strong>Mode:</strong> Simulated backend</div>
                    <div class="info-item"><strong>Flask API:</strong> <a class="api-link" href="http://localhost:5000">http://localhost:5000</a></div>
                </div>
                        
                <p style="color: #666; margin-top: 20px;">
                    <strong>API Endpoints:</strong><br>
                    <a class="api-link" href="/api/spark/info">Spark Info</a> |
                    <a class="api-link" href="/api/spark/metrics">Metrics</a> |
                    <a class="api-link" href="/api/spark/jobs">Jobs</a>
                </p>
            </div>
        </body>
        </html>
        '''
    return render_template_string(spark_logo_html)

@app.route('/api/spark/info')
def spark_info():
    """Get Spark cluster information"""
    if spark:
        sc = spark.sparkContext
        return jsonify({
            "application_id": sc.applicationId,
            "application_name": sc.appName,
            "master": sc.master,
            "spark_version": spark.version,
            "default_parallelism": sc.defaultParallelism,
            "spark_ui_url": "http://localhost:4041"
        })
    else:
        return jsonify({
            "application_id": "app-sim-20251209-0000",
            "application_name": "Music Streaming Backend (Simulated)",
            "master": "local[*]",
            "spark_version": "3.5.0 (Simulated)",
            "default_parallelism": 4,
            "spark_ui_url": "http://localhost:4040"
        })

@app.route('/api/spark/job/sample-data', methods=['POST'])
def run_sample_job():
    """Run a sample Spark job with data processing"""
    try:
        if spark:
            # Create sample data
            data = [
                ("Track1", "Artist1", 150, "Pop"),
                ("Track2", "Artist2", 200, "Rock"),
                ("Track3", "Artist1", 180, "Pop"),
                ("Track4", "Artist3", 220, "Jazz"),
                ("Track5", "Artist2", 190, "Rock"),
                ("Track6", "Artist4", 160, "Electronic"),
                ("Track7", "Artist3", 210, "Jazz"),
                ("Track8", "Artist5", 170, "Pop"),
                ("Track9", "Artist4", 195, "Electronic"),
                ("Track10", "Artist5", 205, "Rock")
            ]
            
            # Create DataFrame
            df = spark.createDataFrame(data, ["track_name", "artist", "duration", "genre"])
            
            # Perform aggregations
            genre_stats = df.groupBy("genre").agg(
                {"duration": "avg", "track_name": "count"}
            ).collect()
            
            artist_stats = df.groupBy("artist").agg(
                {"duration": "avg", "track_name": "count"}
            ).collect()
            
            result = {
                "status": "success",
                "job_name": "Sample Music Data Analysis",
                "total_records": df.count(),
                "genre_statistics": [
                    {
                        "genre": row["genre"],
                        "avg_duration": round(row["avg(duration)"], 2),
                        "track_count": row["count(track_name)"]
                    }
                    for row in genre_stats
                ],
                "artist_statistics": [
                    {
                        "artist": row["artist"],
                        "avg_duration": round(row["avg(duration)"], 2),
                        "track_count": row["count(track_name)"]
                    }
                    for row in artist_stats
                ],
                "timestamp": time.time()
            }
        else:
            # Simulated mode
            result = {
                "status": "success",
                "job_name": "Sample Music Data Analysis (Simulated)",
                "total_records": 10,
                "genre_statistics": [
                    {"genre": "Pop", "avg_duration": 172.5, "track_count": 3},
                    {"genre": "Rock", "avg_duration": 198.33, "track_count": 3},
                    {"genre": "Jazz", "avg_duration": 215.0, "track_count": 2},
                    {"genre": "Electronic", "avg_duration": 177.5, "track_count": 2}
                ],
                "artist_statistics": [
                    {"artist": "Artist1", "avg_duration": 165.0, "track_count": 2},
                    {"artist": "Artist2", "avg_duration": 195.0, "track_count": 2},
                    {"artist": "Artist3", "avg_duration": 215.0, "track_count": 2},
                    {"artist": "Artist4", "avg_duration": 177.5, "track_count": 2},
                    {"artist": "Artist5", "avg_duration": 187.5, "track_count": 2}
                ],
                "timestamp": time.time()
            }
        
        job_results.append(result)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/spark/job/word-count', methods=['POST'])
def run_word_count():
    """Run a classic word count Spark job"""
    try:
        if spark:
            # Sample text data
            text_data = [
                "Apache Spark is a unified analytics engine",
                "Spark provides high-level APIs in Python Java and Scala",
                "Spark runs on Hadoop YARN Kubernetes and standalone mode",
                "PySpark is the Python API for Apache Spark"
            ]
            
            # Create RDD and perform word count
            rdd = spark.sparkContext.parallelize(text_data)
            word_counts = rdd.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                            .reduceByKey(lambda a, b: a + b) \
                            .collect()
            
            result = {
                "status": "success",
                "job_name": "Word Count Analysis",
                "total_words": len(word_counts),
                "word_frequencies": [
                    {"word": word, "count": count}
                    for word, count in sorted(word_counts, key=lambda x: x[1], reverse=True)
                ],
                "timestamp": time.time()
            }
        else:
            # Simulated mode
            word_counts = {
                "Spark": 4, "Apache": 2, "is": 2, "Python": 2, "and": 2,
                "a": 1, "unified": 1, "analytics": 1, "engine": 1,
                "provides": 1, "high-level": 1, "APIs": 1, "in": 1,
                "Java": 1, "Scala": 1, "runs": 1, "on": 1,
                "Hadoop": 1, "YARN": 1, "Kubernetes": 1, "standalone": 1,
                "mode": 1, "PySpark": 1, "the": 1, "API": 1, "for": 1
            }
            
            result = {
                "status": "success",
                "job_name": "Word Count Analysis (Simulated)",
                "total_words": len(word_counts),
                "word_frequencies": [
                    {"word": word, "count": count}
                    for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
                ],
                "timestamp": time.time()
            }
        
        job_results.append(result)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/spark/job/recommendation', methods=['POST'])
def run_recommendation_job():
    """Simulate a music recommendation job"""
    try:
        if spark:
            # Simulate user listening data
            user_data = []
            genres = ["Pop", "Rock", "Jazz", "Electronic", "Classical", "Hip-Hop"]
            
            for i in range(100):
                user_data.append({
                    "user_id": f"user_{i % 20}",
                    "track_id": f"track_{random.randint(1, 50)}",
                    "genre": random.choice(genres),
                    "play_count": random.randint(1, 100),
                    "duration": random.randint(120, 300)
                })
            
            df = spark.createDataFrame(user_data)
            
            # Calculate user preferences
            user_prefs = df.groupBy("user_id", "genre").agg(
                {"play_count": "sum", "track_id": "count"}
            ).collect()
            
            # Get top genres per user
            recommendations = {}
            for row in user_prefs:
                user_id = row["user_id"]
                if user_id not in recommendations:
                    recommendations[user_id] = []
                recommendations[user_id].append({
                    "genre": row["genre"],
                    "total_plays": row["sum(play_count)"],
                    "unique_tracks": row["count(track_id)"]
                })
            
            # Sort recommendations by play count
            for user_id in recommendations:
                recommendations[user_id] = sorted(
                    recommendations[user_id],
                    key=lambda x: x["total_plays"],
                    reverse=True
                )[:3]  # Top 3 genres per user
            
            result = {
                "status": "success",
                "job_name": "Music Recommendation Engine",
                "total_users": len(recommendations),
                "total_interactions": len(user_data),
                "sample_recommendations": dict(list(recommendations.items())[:5]),
                "timestamp": time.time()
            }
        else:
            # Simulated mode
            sample_recommendations = {
                "user_1": [
                    {"genre": "Rock", "total_plays": 450, "unique_tracks": 12},
                    {"genre": "Pop", "total_plays": 320, "unique_tracks": 8},
                    {"genre": "Electronic", "total_plays": 280, "unique_tracks": 6}
                ],
                "user_5": [
                    {"genre": "Jazz", "total_plays": 520, "unique_tracks": 15},
                    {"genre": "Classical", "total_plays": 380, "unique_tracks": 10},
                    {"genre": "Rock", "total_plays": 290, "unique_tracks": 7}
                ],
                "user_10": [
                    {"genre": "Hip-Hop", "total_plays": 410, "unique_tracks": 11},
                    {"genre": "Pop", "total_plays": 350, "unique_tracks": 9},
                    {"genre": "Electronic", "total_plays": 270, "unique_tracks": 5}
                ]
            }
            
            result = {
                "status": "success",
                "job_name": "Music Recommendation Engine (Simulated)",
                "total_users": 3,
                "total_interactions": 100,
                "sample_recommendations": sample_recommendations,
                "timestamp": time.time()
            }
        
        job_results.append(result)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/spark/jobs')
def get_jobs():
    """Get all completed jobs"""
    return jsonify({
        "total_jobs": len(job_results),
        "jobs": job_results[-10:]  # Return last 10 jobs
    })

@app.route('/api/spark/metrics')
def get_metrics():
    """Get Spark metrics"""
    if spark:
        sc = spark.sparkContext
        status_tracker = sc.statusTracker()
        
        active_jobs = status_tracker.getActiveJobIds()
        active_stages = status_tracker.getActiveStageIds()
        
        return jsonify({
            "active_jobs": len(active_jobs),
            "active_stages": len(active_stages),
            "default_parallelism": sc.defaultParallelism,
            "application_id": sc.applicationId,
            "completed_jobs": len(job_results)
        })
    else:
        return jsonify({
            "active_jobs": 0,
            "active_stages": 0,
            "default_parallelism": 4,
            "application_id": "app-sim-20251209-0000",
            "completed_jobs": len(job_results)
        })

@app.route('/api/spark/stop', methods=['POST'])
def stop_spark():
    """Stop Spark session"""
    try:
        if spark:
            spark.stop()
            return jsonify({
                "status": "success",
                "message": "Spark session stopped"
            })
        else:
            return jsonify({
                "status": "success",
                "message": "No Spark session to stop (simulated mode)"
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🎵 Music Streaming PySpark Backend Server")
    print("="*60)
    if spark:
        print(f"✓ Flask API: http://localhost:5000")
        print(f"✓ Spark UI: http://localhost:4041")
        print(f"✓ Status: Running with Real PySpark {spark.version}")
    else:
        print(f"⚠ Flask API: http://localhost:5000")
        print(f"⚠ Status: Running in Simulated Mode (PySpark not available)")
    print("="*60 + "\n")
    
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
