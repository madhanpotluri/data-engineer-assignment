#!/usr/bin/env python3
"""
Enhanced Medallion Pipeline with Checkpoint Support
Processes batch files from checkpoint processor
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with PostgreSQL support"""
    return SparkSession.builder \
        .appName("IoT-Medallion-Pipeline-Checkpoint") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

def create_dynamic_table_schema(spark, df, table_name, db_url, db_properties):
    """Create PostgreSQL table based on DataFrame schema with dynamic column addition"""
    try:
        logger.warning(f"🔧 Starting create_dynamic_table_schema for table: {table_name}")
        # Get DataFrame schema
        schema = df.schema
        
        # Map Spark types to PostgreSQL types
        type_mapping = {
            "StringType": "TEXT",
            "IntegerType": "INTEGER", 
            "LongType": "BIGINT",
            "DoubleType": "DOUBLE PRECISION",
            "FloatType": "REAL",
            "BooleanType": "BOOLEAN",
            "TimestampType": "TIMESTAMP",
            "DateType": "DATE"
        }
        
        # Check if table exists using direct PostgreSQL connection
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="postgres",
                port=5432,
                database="iot_data", 
                user="postgres",
                password="postgres123"
            )
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                # Get existing columns
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                existing_columns = [row[0] for row in cursor.fetchall()]
                logger.info(f"📋 Table {table_name} exists with {len(existing_columns)} columns")
            else:
                existing_columns = []
                logger.info(f"📋 Table {table_name} does not exist, will create")
            
            cursor.close()
            conn.close()
                
        except Exception as e:
            logger.warning(f"⚠️ Error checking table existence: {e}")
            existing_columns = []
        
        # Build ALTER TABLE statements for new columns
        new_columns = []
        for field in schema.fields:
            if field.name not in existing_columns and field.name != "created_timestamp":
                spark_type = str(field.dataType)
                pg_type = type_mapping.get(spark_type, "TEXT")
                new_columns.append(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{field.name}" {pg_type}')
        
        # If table doesn't exist, create it
        logger.warning(f"🔍 Table existence check: existing_columns = {len(existing_columns)} columns")
        logger.warning(f"🔍 Existing columns: {existing_columns}")
        if not existing_columns:
            columns = []
            for field in schema.fields:
                spark_type = str(field.dataType)
                pg_type = type_mapping.get(spark_type, "TEXT")
                columns.append(f'"{field.name}" {pg_type}')
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)},
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Execute via direct JDBC connection
            import psycopg2
            conn = psycopg2.connect(
                host="postgres",
                port=5432,
                database="iot_data", 
                user="postgres",
                password="postgres123"
            )
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Table created: {table_name}")
        else:
            # Check if table has existing data
            try:
                count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
                count_df = spark.read \
                    .format("jdbc") \
                    .option("url", db_url) \
                    .option("dbtable", f"({count_query}) as count_table") \
                    .option("user", db_properties["user"]) \
                    .option("password", db_properties["password"]) \
                    .load()
                
                existing_row_count = count_df.collect()[0][0] if count_df.count() > 0 else 0
                logger.info(f"📊 Table {table_name} has {existing_row_count} existing rows")
            except Exception as e:
                logger.warning(f"⚠️ Could not check existing row count: {e}")
                existing_row_count = 0
            # Add new columns if any
            if new_columns:
                import psycopg2
                conn = psycopg2.connect(
                    host="postgres",
                    port=5432,
                    database="iot_data",
                    user="postgres", 
                    password="postgres123"
                )
                cursor = conn.cursor()
                for alter_sql in new_columns:
                    try:
                        cursor.execute(alter_sql)
                        logger.info(f"✅ Added column: {alter_sql}")
                    except Exception as col_e:
                        logger.warning(f"⚠️  Column may already exist: {col_e}")
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"✅ Table schema updated: {table_name}")
            else:
                logger.info(f"✅ Table schema verified: {table_name}")
        
    except Exception as e:
        logger.warning(f"⚠️  Table creation/update failed: {e}")
        logger.warning(f"⚠️  Exception type: {type(e).__name__}")
        import traceback
        logger.warning(f"⚠️  Traceback: {traceback.format_exc()}")

def get_base_schema(spark, landing_zone_path, sample_size=20):
    """Get base schema from first N files for schema inference"""
    try:
        # Get list of files in landing zone
        files = [f for f in os.listdir(landing_zone_path) if f.endswith('.csv')]
        files.sort()  # Sort to ensure consistent order
        
        # Take first sample_size files
        sample_files = files[:sample_size]
        logger.warning(f"🔍 Using {len(sample_files)} files for schema inference: {sample_files[:5]}...")
        
        if not sample_files:
            logger.warning("⚠️ No files found for schema inference")
            return None
        
        # Read sample files to infer schema
        sample_paths = [os.path.join(landing_zone_path, f) for f in sample_files]
        df_sample = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", "\"") \
            .csv(sample_paths)
        
        base_schema = df_sample.schema
        logger.warning(f"✅ Base schema inferred with {len(base_schema.fields)} columns")
        return base_schema
        
    except Exception as e:
        logger.warning(f"⚠️ Schema inference failed: {e}")
        return None

def process_with_schema_evolution(spark, df, base_schema, table_name, db_url, db_properties, batch_file_path=None):
    """Process DataFrame with schema evolution - Databricks style"""
    try:
        # Get current DataFrame schema
        current_schema = df.schema
        current_columns = set([field.name for field in current_schema.fields])
        
        if base_schema:
            base_columns = set([field.name for field in base_schema.fields])
            
            # Find new columns (not in base schema)
            new_columns = current_columns - base_columns
            existing_columns = current_columns & base_columns
            
            logger.warning(f"🔍 Schema Analysis:")
            logger.warning(f"   - Existing columns: {len(existing_columns)}")
            logger.warning(f"   - New columns: {len(new_columns)}")
            
            if new_columns:
                logger.warning(f"   - New columns found: {list(new_columns)}")
                
                # Create rescue column with new data
                rescue_data = {}
                for col in new_columns:
                    rescue_data[col] = df[col]
                
                # Create rescue column as JSON
                from pyspark.sql.functions import to_json, struct, lit
                rescue_col = struct(*[df[col].alias(col) for col in new_columns])
                df_with_rescue = df.withColumn("rescue_column", to_json(rescue_col))
                
                # Select only existing columns + rescue column
                select_columns = [df[col] for col in existing_columns] + [df_with_rescue["rescue_column"]]
                df_processed = df_with_rescue.select(*select_columns)
                
            else:
                # No new columns, add empty rescue column
                from pyspark.sql.functions import lit
                df_processed = df.withColumn("rescue_column", lit(None).cast("string"))
        else:
            # No base schema, treat all columns as existing
            from pyspark.sql.functions import lit
            df_processed = df.withColumn("rescue_column", lit(None).cast("string"))
        
        # Add metadata columns
        from pyspark.sql.functions import current_timestamp, lit
        df_processed = df_processed \
            .withColumn("rawdata_layer_version", lit("1.0")) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Add batch_file column if batch_file_path is provided
        if batch_file_path:
            df_processed = df_processed.withColumn("batch_file", lit(os.path.basename(batch_file_path)))
        else:
            df_processed = df_processed.withColumn("batch_file", lit("unknown"))
        
        return df_processed
        
    except Exception as e:
        logger.error(f"❌ Schema evolution processing failed: {e}")
        raise e

def rawdata_layer_processing(spark, batch_file_path, db_url, db_properties):
    """RawData Layer: Store data as-is (Bronze Layer - append-only)"""
    logger.info("📥 Processing RawData Layer (Bronze Layer - APPEND ONLY)...")
    
    try:
        # Read current batch file
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", "\"") \
            .csv(batch_file_path)
        
        # Add simple metadata columns (no schema evolution for now)
        from pyspark.sql.functions import current_timestamp, lit
        df_processed = df \
            .withColumn("rawdata_layer_version", lit("1.0")) \
                      .withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("batch_file", lit(os.path.basename(batch_file_path)))
        
        # Create table if it doesn't exist (simple approach)
        logger.info(f"🔍 Calling create_simple_table for rawdata_iot_sensors")
        create_simple_table(spark, df_processed, "rawdata_iot_sensors", db_url, db_properties)
        
        # Write to PostgreSQL using INSERT INTO (Bronze Layer - append-only)
        logger.info(f"📝 Writing {df_processed.count()} records to rawdata_iot_sensors table using INSERT INTO (Bronze Layer)")
        logger.info(f"🔍 Calling write_dataframe_to_postgresql_insert for rawdata_iot_sensors")
        write_dataframe_to_postgresql_insert(df_processed, "rawdata_iot_sensors", db_url, db_properties)
        
        # Verify the write was successful
        try:
            verify_query = "SELECT COUNT(*) as total_count FROM rawdata_iot_sensors"
            verify_df = spark.read \
            .format("jdbc") \
            .option("url", db_url) \
                .option("dbtable", f"({verify_query}) as verify_table") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
                .load()
            
            total_count = verify_df.collect()[0][0] if verify_df.count() > 0 else 0
            logger.warning(f"✅ Verification: rawdata_iot_sensors now has {total_count} total records")
        except Exception as e:
            logger.warning(f"⚠️ Could not verify record count: {e}")
        
        logger.info(f"✅ RawData Layer completed: {df_processed.count()} records")
        return df_processed
        
    except Exception as e:
        logger.error(f"❌ RawData Layer failed: {e}")
        raise e

def create_simple_table(spark, df, table_name, db_url, db_properties):
    """Create table with simple schema (no schema evolution)"""
    try:
        import psycopg2
        logger.info(f"🔍 Creating simple table: {table_name}")
        
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="iot_data", 
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0]
        logger.info(f"🔍 Table {table_name} exists: {table_exists}")
        
        if not table_exists:
            # Create table with simple schema
            schema = df.schema
            columns = []
            
            for field in schema.fields:
                # Map Spark types to PostgreSQL types
                type_mapping = {
                    "StringType": "TEXT",
                    "IntegerType": "INTEGER",
                    "LongType": "BIGINT",
                    "DoubleType": "DOUBLE PRECISION",
                    "FloatType": "REAL",
                    "BooleanType": "BOOLEAN",
                    "TimestampType": "TIMESTAMP",
                    "DateType": "DATE"
                }
                spark_type = str(field.dataType)
                pg_type = type_mapping.get(spark_type, "TEXT")
                columns.append(f'"{field.name}" {pg_type}')
            
            # Check if created_timestamp already exists in columns
            has_created_timestamp = any('created_timestamp' in col.lower() for col in columns)
            
            if has_created_timestamp:
                create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)}
                )
                """
            else:
                create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)},
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            
            logger.info(f"🔍 CREATE SQL: {create_sql}")
            cursor.execute(create_sql)
            conn.commit()
            logger.info(f"✅ Created simple table: {table_name}")
        else:
            logger.info(f"✅ Table already exists: {table_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        # Don't fail the entire pipeline

def create_table_with_schema_evolution(spark, df, table_name, db_url, db_properties):
    """Create table with schema evolution support"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="iot_data", 
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            # Create table with base schema + rescue column
            schema = df.schema
            columns = []
            
            for field in schema.fields:
                if field.name == "rescue_column":
                    columns.append(f'"{field.name}" TEXT')
                elif field.name in ["rawdata_layer_version", "ingestion_timestamp", "batch_file"]:
                    columns.append(f'"{field.name}" TEXT')
                else:
                    # Map Spark types to PostgreSQL types
                    type_mapping = {
                        "StringType": "TEXT",
                        "IntegerType": "INTEGER",
                        "LongType": "BIGINT",
                        "DoubleType": "DOUBLE PRECISION",
                        "FloatType": "REAL",
                        "BooleanType": "BOOLEAN",
                        "TimestampType": "TIMESTAMP",
                        "DateType": "DATE"
                    }
                    spark_type = str(field.dataType)
                    pg_type = type_mapping.get(spark_type, "TEXT")
                    columns.append(f'"{field.name}" {pg_type}')
            
            create_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)},
                created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            cursor.execute(create_sql)
            conn.commit()
            logger.warning(f"✅ Created table with schema evolution: {table_name}")
        else:
            logger.warning(f"✅ Table already exists: {table_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.warning(f"⚠️ Table creation failed: {e}")
        # Don't fail the entire pipeline

def write_dataframe_to_postgresql_insert(df, table_name, db_url, db_properties):
    """Write DataFrame to PostgreSQL using INSERT INTO (Bronze Layer - append-only)"""
    try:
        import psycopg2
        logger.info(f"🔍 Starting INSERT operation for {table_name}")
        
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="iot_data", 
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Get DataFrame schema
        schema = df.schema
        columns = [field.name for field in schema.fields]
        logger.info(f"🔍 DataFrame columns: {columns}")
        
        # Check current record count before insert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_before = cursor.fetchone()[0]
        logger.info(f"🔍 Record count before insert: {count_before}")
        
        # Create INSERT statement
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'"{col}"' for col in columns])
        insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        logger.info(f"🔍 INSERT SQL: {insert_sql}")
        
        # Collect data from DataFrame
        data = df.collect()
        logger.info(f"🔍 Collected {len(data)} rows from DataFrame")
        
        # Insert data row by row
        inserted_count = 0
        for i, row in enumerate(data):
            try:
                values = []
                for j, field in enumerate(schema.fields):
                    value = row[j]
                    if value is None:
                        values.append(None)
                    elif str(field.dataType) == "TimestampType":
                        values.append(value)
                    else:
                        values.append(str(value))
                
                cursor.execute(insert_sql, values)
                inserted_count += 1
                
                if i < 3:  # Log first 3 rows for debugging
                    logger.info(f"🔍 Inserted row {i+1}: {values[:3]}...")
                    
            except Exception as row_error:
                logger.error(f"❌ Error inserting row {i+1}: {row_error}")
                logger.error(f"❌ Row data: {row}")
                raise row_error
        
        conn.commit()
        logger.info(f"✅ Successfully inserted {inserted_count} records into {table_name} (Bronze Layer - APPEND)")
        
        # Check record count after insert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cursor.fetchone()[0]
        logger.info(f"🔍 Record count after insert: {count_after}")
        logger.info(f"🔍 Records added: {count_after - count_before}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to write to PostgreSQL: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise e

def write_dataframe_to_postgresql_overwrite(df, table_name, db_url, db_properties):
    """Write DataFrame to PostgreSQL using OVERWRITE (Silver/Gold Layer - recreate each time)"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="iot_data", 
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Clear existing data (OVERWRITE approach)
        cursor.execute(f"DELETE FROM {table_name}")
        logger.warning(f"🗑️ Cleared existing data from {table_name} (Silver/Gold Layer - OVERWRITE)")
        
        # Get DataFrame schema
        schema = df.schema
        columns = [field.name for field in schema.fields]
        
        # Create INSERT statement
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'"{col}"' for col in columns])
        insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Collect data from DataFrame
        data = df.collect()
        
        # Insert data row by row
        for row in data:
            values = []
            for i, field in enumerate(schema.fields):
                value = row[i]
                if value is None:
                    values.append(None)
                elif str(field.dataType) == "TimestampType":
                    values.append(value)
                else:
                    values.append(str(value))
            
            cursor.execute(insert_sql, values)
        
        conn.commit()
        logger.warning(f"✅ Successfully overwrote {len(data)} records in {table_name} (Silver/Gold Layer - OVERWRITE)")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to write to PostgreSQL: {e}")
        raise e

def write_dataframe_to_postgresql_upsert(df, table_name, db_url, db_properties):
    """Write DataFrame to PostgreSQL using UPSERT (Silver Layer - deduplication and latest record)"""
    try:
        import psycopg2
        logger.info(f"🔄 Starting UPSERT operation for {table_name} (Silver Layer)")
        
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="iot_data", 
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Get DataFrame schema
        schema = df.schema
        columns = [field.name for field in schema.fields]
        logger.info(f"🔍 DataFrame columns: {columns}")
        
        # Check current record count before upsert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_before = cursor.fetchone()[0]
        logger.info(f"🔍 Record count before upsert: {count_before}")
        
        # Create UPSERT statement using ON CONFLICT (PostgreSQL 9.5+)
        # For IoT data, we'll use (id, room_id/id, noted_date) as unique key
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'"{col}"' for col in columns])
        
        # Create the conflict target (unique key columns)
        conflict_columns = '"id", "room_id/id", "noted_date"'
        
        # Create the update clause for conflicting records
        update_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in ['id', 'room_id/id', 'noted_date']])
        
        upsert_sql = f"""
        INSERT INTO {table_name} ({column_names}) 
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns}) 
        DO UPDATE SET {update_clause}
        """
        
        logger.info(f"🔍 UPSERT SQL: {upsert_sql}")
        
        # Collect data from DataFrame
        data = df.collect()
        logger.info(f"🔍 Collected {len(data)} rows from DataFrame")
        
        # Upsert data row by row
        upserted_count = 0
        for i, row in enumerate(data):
            try:
                values = []
                for j, field in enumerate(schema.fields):
                    value = row[j]
                    if value is None:
                        values.append(None)
                    elif str(field.dataType) == "TimestampType":
                        values.append(value)
                    else:
                        values.append(str(value))
                
                cursor.execute(upsert_sql, values)
                upserted_count += 1
                
                if i < 3:  # Log first 3 rows for debugging
                    logger.info(f"🔍 Upserted row {i+1}: {values[:3]}...")
                    
            except Exception as row_error:
                logger.error(f"❌ Error upserting row {i+1}: {row_error}")
                logger.error(f"❌ Row data: {row}")
                raise row_error
        
        conn.commit()
        logger.info(f"✅ Successfully upserted {upserted_count} records into {table_name} (Silver Layer - UPSERT)")
        
        # Check record count after upsert
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cursor.fetchone()[0]
        logger.info(f"🔍 Record count after upsert: {count_after}")
        logger.info(f"🔍 Net records added: {count_after - count_before}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to upsert to PostgreSQL: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise e

def create_silver_table_with_constraints(spark, df, table_name, db_url, db_properties):
    """Create Silver table with unique constraints for upsert operations"""
    try:
        import psycopg2
        logger.info(f"🔍 Creating Silver table with constraints: {table_name}")
        
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="iot_data", 
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        
        table_exists = cursor.fetchone()[0]
        logger.info(f"🔍 Table {table_name} exists: {table_exists}")
        
        if not table_exists:
            # Create table with schema and unique constraint
            schema = df.schema
            columns = []
            
            for field in schema.fields:
                # Map Spark types to PostgreSQL types
                type_mapping = {
                    "StringType": "TEXT",
                    "IntegerType": "INTEGER",
                    "LongType": "BIGINT",
                    "DoubleType": "DOUBLE PRECISION",
                    "FloatType": "REAL",
                    "BooleanType": "BOOLEAN",
                    "TimestampType": "TIMESTAMP",
                    "DateType": "DATE"
                }
                spark_type = str(field.dataType)
                pg_type = type_mapping.get(spark_type, "TEXT")
                columns.append(f'"{field.name}" {pg_type}')
            
            # Check if created_timestamp already exists in columns
            has_created_timestamp = any('created_timestamp' in col.lower() for col in columns)
            
            if has_created_timestamp:
                create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)},
                    CONSTRAINT {table_name}_unique_key UNIQUE ("id", "room_id/id", "noted_date")
                )
                """
            else:
                create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)},
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT {table_name}_unique_key UNIQUE ("id", "room_id/id", "noted_date")
                )
                """
            
            logger.info(f"🔍 CREATE SQL with constraints: {create_sql}")
            cursor.execute(create_sql)
            conn.commit()
            logger.info(f"✅ Created Silver table with constraints: {table_name}")
        else:
            logger.info(f"✅ Table already exists: {table_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Silver table creation failed: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        # Don't fail the entire pipeline

def cleaneddata_layer_processing(spark, df_rawdata, db_url, db_properties, batch_file_path=None):
    """Silver Layer: Clean, validate, deduplicate, upsert, and detect anomalies"""
    logger.info("🧹 Processing Silver Layer (CleanedData) with deduplication, anomaly detection, and missing value treatment...")
    
    try:
        # Data cleaning and validation with enhanced logic
        from pyspark.sql.functions import col, row_number, window, max as spark_max, to_timestamp, when, isnan, isnull, lit, abs as spark_abs
        from pyspark.sql.window import Window
        
        # Step 1: Handle missing values
        logger.info("🔧 Step 1: Missing value treatment...")
        df_missing_treated = df_rawdata
        
        # Count missing values before treatment
        missing_temp = df_rawdata.filter(col("temp").isNull()).count()
        missing_location = df_rawdata.filter(col("room_id/id").isNull()).count()
        missing_timestamp = df_rawdata.filter(col("noted_date").isNull()).count()
        
        logger.info(f"🔍 Missing values detected: temp={missing_temp}, location={missing_location}, timestamp={missing_timestamp}")
        
        # Handle missing temperatures (impute with median by location)
        df_missing_treated = df_missing_treated.withColumn(
            "temp", 
            when(col("temp").isNull(), 
                 when(col("out/in") == "In", 22)  # Default indoor temperature
                 .otherwise(20))  # Default outdoor temperature
            .otherwise(col("temp"))
        )
        
        # Handle missing locations (impute with "Unknown")
        df_missing_treated = df_missing_treated.withColumn(
            "room_id/id",
            when(col("room_id/id").isNull(), lit("Unknown"))
            .otherwise(col("room_id/id"))
        )
        
        # Handle missing timestamps (impute with current timestamp)
        from pyspark.sql.functions import current_timestamp
        df_missing_treated = df_missing_treated.withColumn(
            "noted_date",
            when(col("noted_date").isNull(), current_timestamp())
            .otherwise(col("noted_date"))
        )
        
        # Step 2: Anomaly detection
        logger.info("🔍 Step 2: Anomaly detection...")
        
        # Define anomaly detection rules
        df_with_anomaly_flags = df_missing_treated.withColumn(
            "is_extreme_temp",
            (col("temp") < -50) | (col("temp") > 100)
        ).withColumn(
            "is_negative_indoor",
            (col("temp") < 0) & (col("out/in") == "In")
        ).withColumn(
            "is_impossible_value",
            (col("temp") == 999) | (col("temp") == -999) | (col("temp") == 1000)
        ).withColumn(
            "is_outlier_indoor",
            (col("temp") > 40) & (col("out/in") == "In")
        ).withColumn(
            "is_outlier_outdoor",
            (col("temp") < -30) & (col("out/in") == "Out")
        )
        
        # Create overall anomaly flag
        df_with_anomaly_flags = df_with_anomaly_flags.withColumn(
            "is_anomaly",
            col("is_extreme_temp") | col("is_negative_indoor") | col("is_impossible_value") | 
            col("is_outlier_indoor") | col("is_outlier_outdoor")
        )
        
        # Count anomalies
        anomaly_count = df_with_anomaly_flags.filter(col("is_anomaly") == True).count()
        logger.info(f"🚨 Anomalies detected: {anomaly_count}")
        
        # Step 3: Data quality scoring with anomaly consideration
        df_with_quality = df_with_anomaly_flags.withColumn(
            "data_quality_score",
            when(col("is_anomaly") == True, 0.0)  # Anomalies get 0 quality score
            .when(col("temp").between(15, 35), 1.0)  # Normal range
            .when(col("temp").between(10, 40), 0.8)  # Acceptable range
            .otherwise(0.5)  # Other values
        )
        
        # Step 4: Filter out extreme anomalies but keep treatable ones
        df_cleaned = df_with_quality.filter(
            col("temp").isNotNull() & 
            (col("temp") >= -50) &  # Allow some negative temperatures for outdoor
            (col("temp") <= 100) &
            col("room_id/id").isNotNull() &
            col("noted_date").isNotNull() &
            ~col("is_impossible_value")  # Remove impossible values
        )
        
        # Add enhanced cleaning metadata
        from pyspark.sql.functions import current_timestamp, lit, when, col
        df_cleaneddata = df_cleaned.withColumn("cleaneddata_layer_version", lit("2.0")) \
                                  .withColumn("cleaning_timestamp", current_timestamp()) \
                                  .withColumn("missing_values_treated", 
                                            when(col("temp") == 22, lit("temp_imputed_indoor"))
                                            .when(col("temp") == 20, lit("temp_imputed_outdoor"))
                                            .when(col("room_id/id") == "Unknown", lit("location_imputed"))
                                            .otherwise(lit("no_imputation"))) \
                                  .withColumn("anomaly_type",
                                            when(col("is_extreme_temp"), lit("extreme_temperature"))
                                            .when(col("is_negative_indoor"), lit("negative_indoor"))
                                            .when(col("is_outlier_indoor"), lit("outlier_indoor"))
                                            .when(col("is_outlier_outdoor"), lit("outlier_outdoor"))
                                            .otherwise(lit("normal")))
        
        # Handle corrupt records and count rejections
        if "_corrupt_record" in df_rawdata.columns:
            df_rejected = df_rawdata.filter(col("_corrupt_record").isNotNull()) \
                                  .withColumn("rejection_reason", lit("corrupt_record"))
            corrupt_count = df_rejected.count()
        else:
            corrupt_count = 0
        
        # Count different types of rejections
        impossible_values_count = df_with_anomaly_flags.filter(col("is_impossible_value") == True).count()
        extreme_anomalies_count = df_with_anomaly_flags.filter(col("is_extreme_temp") == True).count()
        
        total_rejected = corrupt_count + impossible_values_count + extreme_anomalies_count
        logger.info(f"📊 Rejection summary: corrupt={corrupt_count}, impossible_values={impossible_values_count}, extreme_anomalies={extreme_anomalies_count}")
        
        # Add processing metadata
        from pyspark.sql.functions import current_timestamp, lit
        df_processed_cleaned = df_cleaneddata \
            .withColumn("cleaneddata_layer_version", lit("2.0")) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("batch_file", lit(os.path.basename(batch_file_path)))
        
        # Create table if it doesn't exist (with unique constraint for upsert)
        create_silver_table_with_constraints(spark, df_processed_cleaned, "cleaneddata_iot_sensors", db_url, db_properties)
        
        # Implement Silver Layer deduplication and upsert
        logger.info("🔄 Implementing Silver Layer deduplication and upsert...")
        write_dataframe_to_postgresql_upsert(df_processed_cleaned, "cleaneddata_iot_sensors", db_url, db_properties)
        
        # Verify the write was successful
        try:
            verify_query = "SELECT COUNT(*) as total_count FROM cleaneddata_iot_sensors"
            verify_df = spark.read \
            .format("jdbc") \
            .option("url", db_url) \
                .option("dbtable", f"({verify_query}) as verify_table") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
                .load()
            
            total_count = verify_df.collect()[0][0] if verify_df.count() > 0 else 0
            logger.info(f"✅ Verification: cleaneddata_iot_sensors now has {total_count} total records")
        except Exception as e:
            logger.warning(f"⚠️ Could not verify record count: {e}")
        
        # Final statistics
        final_count = df_cleaneddata.count()
        anomaly_count_final = df_cleaneddata.filter(col("is_anomaly") == True).count()
        missing_treated_count = df_cleaneddata.filter(col("missing_values_treated") != "no_imputation").count()
        
        logger.info(f"✅ Enhanced Silver Layer completed:")
        logger.info(f"   📊 Records processed: {final_count}")
        logger.info(f"   🚨 Anomalies detected: {anomaly_count_final}")
        logger.info(f"   🔧 Missing values treated: {missing_treated_count}")
        logger.info(f"   ❌ Records rejected: {total_rejected}")
        
        return df_cleaneddata, total_rejected
        
    except Exception as e:
        logger.error(f"❌ CleanedData Layer failed: {e}")
        raise e

def analyticsdata_layer_processing(spark, df_cleaneddata, db_url, db_properties):
    """AnalyticsData Layer: Business analytics and aggregations"""
    logger.info("📊 Processing AnalyticsData Layer...")
    
    try:
        # Import necessary functions
        from pyspark.sql.functions import count, avg, min, max, stddev, hour, to_timestamp, col, lit, current_timestamp
        from datetime import datetime
        
        # BUG FIX: Read ALL cleaned data from database instead of just current batch
        logger.info("🔍 Reading all cleaned data from database for comprehensive analytics...")
        try:
            # Read all cleaned data from the database table
            df_all_cleaned_data = spark.read \
                .format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", "cleaneddata_iot_sensors") \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .load()
            
            total_records_in_db = df_all_cleaned_data.count()
            current_batch_records = df_cleaneddata.count()
            logger.info(f"📊 Analytics will process {total_records_in_db} total records (including {current_batch_records} from current batch)")
            
            # Use all cleaned data for analytics
            analytics_source_data = df_all_cleaned_data
            
        except Exception as e:
            logger.warning(f"⚠️ Could not read from cleaneddata_iot_sensors table: {e}")
            logger.warning("⚠️ Falling back to current batch data only")
            analytics_source_data = df_cleaneddata
        
        # Device metrics - now using ALL cleaned data
        df_device_metrics = analytics_source_data.groupBy("room_id/id") \
            .agg(
                count("*").alias("reading_count"),
                avg("temp").alias("avg_temperature"),
                min("temp").alias("min_temperature"),
                max("temp").alias("max_temperature"),
                stddev("temp").alias("temp_stddev")
            ) \
            .withColumn("analyticsdata_layer_version", lit("2.0")) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("data_source", lit("all_cleaned_data"))
        
        # Location metrics - now using ALL cleaned data
        df_location_metrics = analytics_source_data.groupBy("out/in") \
            .agg(
                count("*").alias("reading_count"),
                avg("temp").alias("avg_temperature"),
                min("temp").alias("min_temperature"),
                max("temp").alias("max_temperature")
            ) \
            .withColumn("analyticsdata_layer_version", lit("2.0")) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("data_source", lit("all_cleaned_data"))
        
        # Time-based metrics - now using ALL cleaned data
        df_time_metrics = analytics_source_data.withColumn("hour", hour(to_timestamp(col("noted_date"), "yyyy-MM-dd HH:mm:ss"))) \
            .groupBy("hour") \
            .agg(
                count("*").alias("reading_count"),
                avg("temp").alias("avg_temperature")
            ) \
            .withColumn("analyticsdata_layer_version", lit("2.0")) \
            .withColumn("created_timestamp", current_timestamp()) \
            .withColumn("data_source", lit("all_cleaned_data"))
        
        # Quality summary - now using ALL cleaned data
        total_records = analytics_source_data.count()
        quality_score = analytics_source_data.agg(avg("data_quality_score")).collect()[0][0]
        
        df_quality_summary = spark.createDataFrame([{
            "total_records": total_records,
            "avg_quality_score": quality_score,
            "high_quality_records": analytics_source_data.filter(col("data_quality_score") >= 0.8).count(),
            "medium_quality_records": analytics_source_data.filter((col("data_quality_score") >= 0.5) & (col("data_quality_score") < 0.8)).count(),
            "low_quality_records": analytics_source_data.filter(col("data_quality_score") < 0.5).count(),
            "analyticsdata_layer_version": "2.0",
            "created_timestamp": datetime.now(),
            "data_source": "all_cleaned_data"
        }])
        
        # Create tables and write data
        tables_data = [
            (df_device_metrics, "analyticsdata_device_metrics"),
            (df_location_metrics, "analyticsdata_location_metrics"),
            (df_time_metrics, "analyticsdata_time_metrics"),
            (df_quality_summary, "analyticsdata_quality_summary")
        ]
        
        for df, table_name in tables_data:
            # Add simple metadata columns (no schema evolution for now)
            from pyspark.sql.functions import current_timestamp, lit
            df_processed_analytics = df \
                .withColumn("analyticsdata_layer_version", lit("1.0")) \
                .withColumn("created_timestamp", current_timestamp())
            
            # Create table if it doesn't exist (simple approach)
            create_simple_table(spark, df_processed_analytics, table_name, db_url, db_properties)
            
            # Write to PostgreSQL using OVERWRITE (Gold Layer - recreate each time with comprehensive data)
            record_count = df_processed_analytics.count()
            logger.info(f"📝 Writing {record_count} comprehensive analytics records to {table_name} table using OVERWRITE (Gold Layer)")
            write_dataframe_to_postgresql_overwrite(df_processed_analytics, table_name, db_url, db_properties)
        
        logger.info("✅ AnalyticsData Layer completed with comprehensive analytics from all historical data")
        return df_device_metrics, df_location_metrics, df_time_metrics, df_quality_summary
        
    except Exception as e:
        logger.error(f"❌ AnalyticsData Layer failed: {e}")
        raise e

def generate_pipeline_metadata(spark, batch_file_path, db_url, db_properties, 
                             rawdata_count, cleaneddata_count, rejected_count, 
                             processing_start_time, processing_end_time):
    """Generate comprehensive pipeline metadata for audit and reprocessing"""
    try:
        processing_duration = (processing_end_time - processing_start_time).total_seconds()
        success_rate = (cleaneddata_count / rawdata_count * 100) if rawdata_count > 0 else 0
        rejection_rate = (rejected_count / rawdata_count * 100) if rawdata_count > 0 else 0
        
        # Calculate data quality score
        quality_score = 0.8 if success_rate >= 90 else 0.6 if success_rate >= 70 else 0.4
        
        # Determine pipeline status
        pipeline_status = "SUCCESS" if rejected_count == 0 else "PARTIAL_SUCCESS"
        
        # Create metadata DataFrame
        metadata_data = [{
            "pipeline_run_id": f"run_{int(processing_start_time.timestamp())}",
            "input_file_name": os.path.basename(batch_file_path),
            "input_file_path": batch_file_path,
            "processing_start_time": processing_start_time,
            "processing_end_time": processing_end_time,
            "processing_duration_seconds": processing_duration,
            "rawdata_records_ingested": rawdata_count,
            "cleaneddata_records_processed": cleaneddata_count,
            "records_rejected": rejected_count,
            "success_rate_percent": success_rate,
            "rejection_rate_percent": rejection_rate,
            "pipeline_status": pipeline_status,
            "data_quality_score": quality_score,
            "created_timestamp": datetime.now()
        }]
        
        df_metadata = spark.createDataFrame(metadata_data)
        
        # Create metadata table
        create_dynamic_table_schema(spark, df_metadata, "pipeline_metadata", db_url, db_properties)
        
        # Write metadata
        df_metadata.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "pipeline_metadata") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .mode("append") \
            .save()
        
        logger.info("✅ Pipeline metadata generated and stored")
        
    except Exception as e:
        logger.error(f"❌ Metadata generation failed: {e}")

def run_checkpoint_pipeline(batch_file_path, output_db="postgres"):
    """Run the three-layer architecture pipeline with checkpoint support"""
    logger.info("🚀 Starting Checkpoint-based Medallion Pipeline...")
    
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info("✅ Spark session created")
        
        # Database configuration
        db_url = "jdbc:postgresql://postgres:5432/iot_data"
        db_properties = {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "postgres123"
        }
        
        # Track processing start time
        processing_start_time = datetime.now()
        
        # RawData Layer: Raw data ingestion
        df_rawdata = rawdata_layer_processing(spark, batch_file_path, db_url, db_properties)
        rawdata_count = df_rawdata.count()
        
        # CleanedData Layer: Cleaned data
        df_cleaneddata, rejected_count = cleaneddata_layer_processing(spark, df_rawdata, db_url, db_properties, batch_file_path)
        cleaneddata_count = df_cleaneddata.count()
        
        # AnalyticsData Layer: Business analytics
        df_device_analytics, df_location_analytics, df_time_analytics, df_quality_summary = analyticsdata_layer_processing(spark, df_cleaneddata, db_url, db_properties)
        
        # Track processing end time
        processing_end_time = datetime.now()
        
        # Generate comprehensive pipeline metadata
        generate_pipeline_metadata(spark, batch_file_path, db_url, db_properties, 
                                 rawdata_count, cleaneddata_count, rejected_count, 
                                 processing_start_time, processing_end_time)
        
        logger.info("🎉 Checkpoint-based Medallion Pipeline completed successfully!")
        logger.info("📊 Data layers created:")
        logger.info("  📥 RawData: rawdata_iot_sensors (raw as-is)")
        logger.info("  🧹 CleanedData: cleaneddata_iot_sensors (cleaned & validated)")
        logger.info("  📊 AnalyticsData: analyticsdata_device_metrics, analyticsdata_location_metrics, analyticsdata_time_metrics, analyticsdata_quality_summary")
        logger.info("  📋 Pipeline Metadata: pipeline_metadata (audit & reprocessing info)")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise e
    finally:
        if spark:
            spark.stop()

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Checkpoint-based Medallion Pipeline")
    parser.add_argument("--batch-file", required=True, help="Path to batch CSV file")
    parser.add_argument("--output-db", default="postgres", help="Output database")
    
    args = parser.parse_args()
    
    run_checkpoint_pipeline(args.batch_file, args.output_db)

if __name__ == "__main__":
    main()
