# Music Streaming Application with PySpark Backend

This is a music streaming application backend built with PySpark and Flask. It demonstrates how to use PySpark for data processing in a web application context.

## Prerequisites

1. **Java 17** - Required for PySpark
2. **Python 3.12** - Application runtime
3. **Node.js** - For the frontend UI proxy

## Setup Instructions

### 1. Install Java 17
Make sure you have Java 17 installed and `JAVA_HOME` is set to your Java installation directory.

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup Hadoop WinUtils (Windows only)
```bash
# Create Hadoop directory
mkdir C:\hadoop\bin

# Download winutils.exe and hadoop.dll to C:\hadoop\bin
# You can download them from: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin
```

### 4. Run the Application
You can start the application in two ways:

#### Option 1: Using the batch file (Windows)
```bash
setup_and_run.bat
```

#### Option 2: Manual start
```bash
# Terminal 1: Start the Spark UI proxy
npm start

# Terminal 2: Start the PySpark backend
python spark_backend.py
```

## Accessing the Application

Once running, you can access:

- **Frontend UI**: http://localhost:4040
- **PySpark Backend API**: http://localhost:5000
- **Real Spark UI**: http://localhost:4041

## API Endpoints

- `GET /` - Main application page
- `GET /api/spark/info` - Spark cluster information
- `POST /api/spark/job/sample-data` - Run sample music data analysis
- `POST /api/spark/job/word-count` - Run word count analysis
- `POST /api/spark/job/recommendation` - Run music recommendation engine
- `GET /api/spark/jobs` - Get all completed jobs
- `GET /api/spark/metrics` - Get Spark metrics
- `POST /api/spark/stop` - Stop Spark session

## Troubleshooting

### Common Issues on Windows

1. **WinUtils Missing**: Make sure `winutils.exe` is in `C:\hadoop\bin`
2. **Java Path Issues**: Ensure `JAVA_HOME` is set correctly
3. **Permission Errors**: Run the command prompt as Administrator if needed

### PySpark Connection Issues

If you encounter Python worker crashes:
1. Make sure you're not using Windows Store Python
2. Try using a standard Python installation from python.org
3. Check that all environment variables are set correctly

## Project Structure

```
├── spark-ui/              # Static Spark UI files
├── spark_backend.py       # Main Flask application with PySpark
├── spark-proxy.js         # Node.js proxy for Spark UI
├── setup_and_run.bat      # Windows batch file to start everything
├── requirements.txt       # Python dependencies
├── package.json           # Node.js dependencies
└── README.md              # This file
```

## Development

To test PySpark setup independently:
```bash
python minimal_spark_test.py
```

This will verify that PySpark can create a session without running the full application.