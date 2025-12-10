const express = require('express');
const path = require('path');
const app = express();
const PORT = 4040;

// Serve static files
app.use(express.static(path.join(__dirname, 'spark-ui')));

// API endpoints that Spark Web UI uses
app.get('/api/v1/applications', (req, res) => {
  res.json([
    {
      "id": "app-20230515123456-0000",
      "name": "Sample Spark Application",
      "cores": 4,
      "status": "RUNNING",
      "startTime": "2023-05-15T12:34:56.789GMT",
      "attempts": [
        {
          "startTime": "2023-05-15T12:34:56.789GMT",
          "endTime": "Unknown",
          "lastUpdated": "2023-05-15T12:45:30.123GMT",
          "duration": 633343,
          "sparkUser": "user",
          "completed": false,
          "appSparkVersion": "3.4.0"
        }
      ]
    },
    {
      "id": "app-20230515123000-0001",
      "name": "Data Processing Job",
      "cores": 2,
      "status": "COMPLETED",
      "startTime": "2023-05-15T12:30:00.000GMT",
      "attempts": [
        {
          "startTime": "2023-05-15T12:30:00.000GMT",
          "endTime": "2023-05-15T12:34:56.000GMT",
          "lastUpdated": "2023-05-15T12:34:56.000GMT",
          "duration": 296000,
          "sparkUser": "user",
          "completed": true,
          "appSparkVersion": "3.4.0"
        }
      ]
    },
    {
      "id": "app-20230515122500-0002",
      "name": "Machine Learning Model Training",
      "cores": 4,
      "status": "COMPLETED",
      "startTime": "2023-05-15T12:25:00.000GMT",
      "attempts": [
        {
          "startTime": "2023-05-15T12:25:00.000GMT",
          "endTime": "2023-05-15T12:30:12.000GMT",
          "lastUpdated": "2023-05-15T12:30:12.000GMT",
          "duration": 312000,
          "sparkUser": "user",
          "completed": true,
          "appSparkVersion": "3.4.0"
        }
      ]
    }
  ]);
});

app.get('/api/v1/applications/:appId', (req, res) => {
  const appId = req.params.appId;
  
  // Mock data for different applications
  const apps = {
    "app-20230515123456-0000": {
      "id": "app-20230515123456-0000",
      "name": "Sample Spark Application",
      "cores": 4,
      "status": "RUNNING",
      "startTime": "2023-05-15T12:34:56.789GMT",
      "attempts": [
        {
          "startTime": "2023-05-15T12:34:56.789GMT",
          "endTime": "Unknown",
          "lastUpdated": "2023-05-15T12:45:30.123GMT",
          "duration": 633343,
          "sparkUser": "user",
          "completed": false,
          "appSparkVersion": "3.4.0"
        }
      ]
    },
    "app-20230515123000-0001": {
      "id": "app-20230515123000-0001",
      "name": "Data Processing Job",
      "cores": 2,
      "status": "COMPLETED",
      "startTime": "2023-05-15T12:30:00.000GMT",
      "attempts": [
        {
          "startTime": "2023-05-15T12:30:00.000GMT",
          "endTime": "2023-05-15T12:34:56.000GMT",
          "lastUpdated": "2023-05-15T12:34:56.000GMT",
          "duration": 296000,
          "sparkUser": "user",
          "completed": true,
          "appSparkVersion": "3.4.0"
        }
      ]
    },
    "app-20230515122500-0002": {
      "id": "app-20230515122500-0002",
      "name": "Machine Learning Model Training",
      "cores": 4,
      "status": "COMPLETED",
      "startTime": "2023-05-15T12:25:00.000GMT",
      "attempts": [
        {
          "startTime": "2023-05-15T12:25:00.000GMT",
          "endTime": "2023-05-15T12:30:12.000GMT",
          "lastUpdated": "2023-05-15T12:30:12.000GMT",
          "duration": 312000,
          "sparkUser": "user",
          "completed": true,
          "appSparkVersion": "3.4.0"
        }
      ]
    }
  };
  
  if (apps[appId]) {
    res.json(apps[appId]);
  } else {
    res.status(404).json({ error: "Application not found" });
  }
});

// Additional API endpoints for workers and cluster info
app.get('/api/v1/applications/:appId/executors', (req, res) => {
  res.json([
    {
      "id": "driver",
      "hostPort": "localhost:50000",
      "isActive": true,
      "cores": 4,
      "memory": 1024,
      "diskUsed": 0,
      "totalCores": 4,
      "maxTasks": 4,
      "activeTasks": 2,
      "failedTasks": 0,
      "completedTasks": 10,
      "totalTasks": 12,
      "totalDuration": 120000,
      "totalGCTime": 5000,
      "totalInputBytes": 1048576,
      "totalShuffleRead": 2097152,
      "totalShuffleWrite": 1048576
    },
    {
      "id": "1",
      "hostPort": "localhost:50001",
      "isActive": true,
      "cores": 2,
      "memory": 512,
      "diskUsed": 0,
      "totalCores": 2,
      "maxTasks": 2,
      "activeTasks": 1,
      "failedTasks": 0,
      "completedTasks": 5,
      "totalTasks": 6,
      "totalDuration": 60000,
      "totalGCTime": 2000,
      "totalInputBytes": 524288,
      "totalShuffleRead": 1048576,
      "totalShuffleWrite": 524288
    }
  ]);
});

app.get('/api/v1/workers', (req, res) => {
  res.json([
    {
      "id": "worker-202305151234-local-4",
      "host": "localhost",
      "port": 50000,
      "webUiPort": 50001,
      "cores": 4,
      "memory": 15360,
      "state": "ALIVE",
      "lastHeartbeat": 1684151130000
    }
  ]);
});

app.get('/api/v1/environment', (req, res) => {
  res.json({
    "runtime": {
      "Java Version": "11.0.12",
      "Scala Version": "2.12.15",
      "Spark Version": "3.4.0"
    },
    "sparkProperties": {
      "spark.master": "local[*]",
      "spark.app.name": "Sample Spark Application",
      "spark.driver.memory": "2g",
      "spark.executor.memory": "1g"
    },
    "systemProperties": {
      "java.version": "11.0.12",
      "os.name": "Windows 10"
    }
  });
});

app.get('/', (req, res) => {
  res.sendFile(path.resolve(__dirname, 'spark-ui', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Spark UI Proxy listening at http://localhost:${PORT}`);
});