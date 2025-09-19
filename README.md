Vm

This repo is created for the data engineer assignment.

 The initial purpose is to address the task of iot-data ingestion to the rds final table such as mysql or postgres.
 
 In addition, we will be covering the broader data engineering architecture and practices, so that the solution can be scalable, production ready. 

 The folder structure.


 The Data Ingestion Architecture.
  - three layered architecture (rawdata -> cleaned -> analytics ready or final table).

Technical architecture.
    - Iot data --> data pipelines (orchestrated in airflow) --> postgres.

    (airflow-webserver for UI, airflow-schedular, spark-master ) --- control plane 
    
    spark-worker - dashboard for insights --- data plane.