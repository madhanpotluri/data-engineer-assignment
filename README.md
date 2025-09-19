# IoT Data Ingestion Pipeline

This repository is a demo of a  data engineering solution for IoT temperature data ingestion, designed to handle real-time data processing with a focus on scalability and production readiness.

## Overview

The project addresses the core challenge of ingesting IoT sensor data into a relational database (PostgreSQL) while implementing industry-standard data engineering practices. The solution provides a complete end-to-end pipeline from data generation to analytics-ready insights.


## Data Architecture

### Data Flow

```

IoT Sensors → Landing Zone → ELT Pipeline (Apache Airflow & Spark) → PostgreSQL → Dashboard
```

### Three-Layered (Medallion) Data Processing

```
Raw Data Layer → Clean Data Layer → Analytics Data Layer
```

The solution implements a medallion architecture with three distinct layers:

1. **Raw Data Layer (Bronze)**
   - Stores data exactly as received from IoT sensors
   - Preserves original format and timestamps
   - Includes data lineage and source file tracking

2. **Cleaned Data Layer (Silver)**
   - Validates and cleans raw data
   - Standardizes formats and data types
   - Applies data quality rules and scoring
   - Removes duplicates and handles missing values

3. **Analytics Data Layer (Gold)**
   - Aggregated metrics and business insights
   - Device performance analytics
   - Location-based temperature analysis
   - Time-series patterns and trends




## Technologies Used

Airflow, Spark, PostgreSQL, Docker, Streamlit, Python


## Explanation to Whys

### Why Apache Airflow instead of cron jobs?

**Used:** Apache Airflow for workflow orchestration

Cron jobs work for simple tasks, but fail with complex data workflows. Airflow handles dependencies automatically, retries failed tasks, provides web UI monitoring, and scales better than managing multiple cron jobs.

### Why Spark instead of normal Python processing?

**Used:** Apache Spark for distributed data processing

For small datasets, regular Python works fine. But when you're dealing with IoT data that can generate millions of records, Spark processes data in chunks (handles datasets larger than RAM), processes multiple files in parallel, automatically redistributes work if a node crashes, scales horizontally without rewriting code, and has dynamic schema inference - if new sensor parameters or columns get added, the pipeline adapts automatically without code changes.

### Why separate Spark Master and Worker nodes?

**Used:** Spark Master + Worker nodes for distributed computing

Master node acts like a traffic controller - receives jobs, breaks them into tasks, and distributes to workers. Worker nodes do the actual processing. Separate nodes let you add more workers as data grows, instead of being limited by one machine's CPU and memory.

### How to scale with more worker nodes in production?

**Used:** Docker containers for easy scaling

Just spin up new Spark worker containers and register with master (no code changes). Configure memory/CPU per worker, Spark auto-distributes work based on capacity, set up auto-scaling in cloud, and partition data across more nodes for better performance.

### Why ELT over ETL for IoT data?

**Used:** ELT (Extract, Load, Transform) approach for big data processing

Traditional ETL transforms data before loading, but IoT data is massive and comes in various formats. ELT loads raw data first, then transforms it in the database. This approach gives you faster ingestion (no transformation delays), preserves original data for debugging, handles schema changes easily, scales better with big data, and lets you reprocess data with different transformations without re-ingesting.

### Why the three-layer architecture?

**Used:** Medallion architecture (Bronze-Silver-Gold)

Raw layer preserves original data for debugging/compliance, cleaned layer standardizes format and applies data quality rules, analytics layer pre-aggregates data for fast queries. This gives flexibility - you can always go back to raw data, but also have clean, fast data for analytics.



### Technical Stack

**Control Plane:**
- **Airflow Web Server** - Pipeline orchestration and monitoring UI
- **Airflow Scheduler** - Automated pipeline execution and scheduling
- **Spark Master** - Distributed processing coordination

**Data Plane:**
- **Spark Workers** - Distributed data processing
- **PostgreSQL** - Primary data warehouse
- **Streamlit Dashboard** - Data Ingestion Insights 

## Project Structure

```
data-engineer-assignment/
├── dags/                          # Airflow DAG definitions
├── scripts/                       # Core processing scripts
│   ├── iot_simulator.py          # IoT data generation
│   ├── iot_three_layered_pipeline.py # Three-layered processing
│   ├── checkpoint_processor.py   # Checkpoint management
│   └── dashboard.py              # Streamlit dashboard
├── data/                         # Data storage
├── docker/                       # Container configurations
└── docker-compose.yml           # Service orchestration
```

## Key Features

### Data Ingestion
- **Real-time Simulation**: This script is created to genarte IoT temperature data every 5 seconds
- **Format Standardization**: Converts data to match target schema
- **Checkpoint Management**: Prevents duplicate processing
- **Error Handling**: Robust error recovery and logging

### Data Processing
- **Three-Layer Pipeline**: Bronze → Silver → Gold transformation
- **Data Quality Scoring**: Automated quality assessment
- **Anomaly Detection**: Statistical outlier identification
- **Metadata Tracking**: Complete processing audit trail

### Monitoring & Analytics
- **Real-time Dashboard**: Live data ingestion insights
- **Pipeline Monitoring**: Execution status and performance metrics
- **Anomaly Detection**: Interactive threshold configuration
- **Data Export**: CSV download capabilities

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- PostgreSQL client (optional)

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/madhanpotluri/data-engineer-assignment.git
   cd data-engineer-assignment
   ```

2. **Start the services**
   ```bash
   docker-compose up -d
   ```

3. **Access the applications**
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Dashboard: http://localhost:8501
   - PostgreSQL: localhost:5433

4. **Run the pipelines**
   - Navigate to Airflow UI
   - Trigger `iot-simulation-only` for data generation
   - Trigger `iot-three-layered-processing` for data processing

## Pipeline Details

### IoT Data Simulation
- **Frequency**: Every 5 seconds
- **Data Format**: CSV with standardized schema
- **Location**: `/data/landing_zone/`
- **Schema**: `id, room_id/id, noted_date, temp, out/in, room`

### Data Processing Pipeline
- **Trigger**: Manual or scheduled (every 5 minutes)
- **Checkpoint**: Prevents reprocessing of existing files
- **Processing**: Three-layer transformation
- **Output**: PostgreSQL tables with metadata

### Dashboard Features
- **Overview**: High-level metrics and KPIs
- **Pipeline Runs**: Execution history and performance
- **Raw Data**: Source data inspection
- **Processed Data**: Cleaned data analysis
- **Anomaly Detection**: Statistical outlier analysis
- **Data Quality**: Pipeline health monitoring

## Configuration

### Environment Variables
```bash
DB_HOST=localhost
DB_PORT=5433
DB_NAME=iot_data
DB_USER=postgres
DB_PASSWORD=postgres123
```

### Airflow Configuration
- **DAGs**: Located in `/dags/` directory
- **Logs**: Stored in `/opt/airflow/logs/`
- **Pools**: Configured for resource management

## Data Schema

### Raw Data Table (`rawdata_iot_sensors`)
```sql
id SERIAL PRIMARY KEY,
room_id/id TEXT,
noted_date TIMESTAMP,
temp DOUBLE PRECISION,
out/in TEXT,
room TEXT,
source_file TEXT,
processing_timestamp TIMESTAMP,
created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

### Cleaned Data Table (`cleaneddata_iot_sensors`)
```sql
id SERIAL PRIMARY KEY,
room_id/id TEXT,
noted_date TIMESTAMP,
temp DOUBLE PRECISION,
out/in TEXT,
room TEXT,
source_file TEXT,
processing_timestamp TIMESTAMP,
cleaneddata_layer_version TEXT,
cleaning_timestamp TIMESTAMP,
data_quality_score DOUBLE PRECISION,
created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

## Monitoring & Troubleshooting

### Common Issues
1. **Database Connection**: Ensure PostgreSQL is running on port 5433
2. **File Permissions**: Check volume mounts in docker-compose.yml
3. **Airflow DAGs**: Verify DAG files are in correct directory
4. **Memory Issues**: Adjust Spark worker memory settings

### Logs
- **Airflow**: Available in Airflow UI or `/opt/airflow/logs/`
- **Dashboard**: Check container logs with `docker logs iot-dashboard`
- **PostgreSQL**: Query logs in database

## Development

### Adding New Features
1. **New DAGs**: Add to `/dags/` directory
2. **Processing Logic**: Modify scripts in `/scripts/`
3. **Dashboard**: Update `/scripts/dashboard.py`
4. **Database Schema**: Update migration scripts

### Testing
- **Unit Tests**: Run individual script tests
- **Integration Tests**: Test full pipeline execution
- **Data Validation**: Verify data quality and completeness

## Production Considerations

### Scalability
- **Horizontal Scaling**: Add more Spark workers
- **Database Optimization**: Implement partitioning and indexing
- **Caching**: Add Redis for frequently accessed data

### Security
- **Authentication**: Implement proper user management
- **Encryption**: Add data encryption at rest and in transit
- **Access Control**: Configure proper database permissions

### Monitoring
- **Alerting**: Set up failure notifications
- **Metrics**: Implement comprehensive monitoring
- **Logging**: Centralized log management

## License

This project is created for educational and demonstration purposes as part of a data engineering assignment.

