#!/usr/bin/env python3
"""
IoT Data Pipeline Dashboard
Data Ingestion Insights - Real-time monitoring and anomaly detection
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from datetime import datetime, timedelta
import os
import numpy as np

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5433"),
        database=os.getenv("DB_NAME", "iot_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres123")
    )

# Load data functions
def load_raw_data():
    conn = get_db_connection()
    query = """
    SELECT 
        "room_id/id" as device_id, 
        noted_date as reading_time, 
        temp as temperature, 
        "out/in" as location_type, 
        processing_timestamp,
        room,
        source_file
    FROM rawdata_iot_sensors 
    ORDER BY noted_date DESC 
    LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_cleaned_data():
    conn = get_db_connection()
    query = """
    SELECT 
        "room_id/id" as device_id,
        noted_date as reading_time, 
        temp as temperature, 
        "out/in" as location_type, 
        processing_timestamp,
        data_quality_score,
        room
    FROM cleaneddata_iot_sensors 
    ORDER BY noted_date DESC 
    LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_analytics_data():
    conn = get_db_connection()
    
    # Device metrics
    try:
    device_query = "SELECT * FROM analyticsdata_device_metrics"
    device_df = pd.read_sql(device_query, conn)
    except:
        device_df = pd.DataFrame()
    
    # Location metrics
    try:
    location_query = "SELECT * FROM analyticsdata_location_metrics"
    location_df = pd.read_sql(location_query, conn)
    except:
        location_df = pd.DataFrame()
    
    # Time series metrics
    try:
        timeseries_query = "SELECT * FROM analyticsdata_timeseries_metrics"
        timeseries_df = pd.read_sql(timeseries_query, conn)
    except:
        timeseries_df = pd.DataFrame()
    
    conn.close()
    return device_df, location_df, timeseries_df

def load_pipeline_metadata():
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            pipeline_run_id,
            input_file_path as batch_file_path,
            processing_start_time,
            processing_end_time,
            rawdata_records_ingested as records_processed,
            pipeline_status as success_status,
            '' as error_message
        FROM pipeline_metadata 
        ORDER BY processing_start_time DESC
        LIMIT 50
        """
        df = pd.read_sql(query, conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def detect_anomalies(df, threshold_std=2):
    """Detect temperature anomalies using statistical methods"""
    if df.empty or 'temperature' not in df.columns:
        return pd.DataFrame()
    
    # Convert to numeric and remove NaN values
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df_clean = df.dropna(subset=['temperature'])
    
    if df_clean.empty:
        return pd.DataFrame()
    
    # Calculate statistics
    mean_temp = df_clean['temperature'].mean()
    std_temp = df_clean['temperature'].std()
    
    # Define anomaly thresholds
    upper_threshold = mean_temp + (threshold_std * std_temp)
    lower_threshold = mean_temp - (threshold_std * std_temp)
    
    # Identify anomalies
    anomalies = df_clean[
        (df_clean['temperature'] > upper_threshold) | 
        (df_clean['temperature'] < lower_threshold)
    ].copy()
    
    if not anomalies.empty:
        # Add severity classification
        anomalies['severity'] = anomalies['temperature'].apply(
            lambda x: 'High' if abs(x - mean_temp) > (threshold_std * 1.5 * std_temp) else 'Medium'
        )
        anomalies['deviation'] = abs(anomalies['temperature'] - mean_temp)
    
    return anomalies, mean_temp, std_temp, upper_threshold, lower_threshold

def load_data_summary():
    """Load comprehensive data summary from all three tables"""
    conn = get_db_connection()
    try:
        # Raw data summary
        raw_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT "room_id/id") as unique_devices,
            COUNT(DISTINCT room) as unique_rooms,
            COUNT(DISTINCT "out/in") as location_types,
            MIN(processing_timestamp) as earliest_data,
            MAX(processing_timestamp) as latest_data,
            AVG(temp) as avg_temperature,
            MIN(temp) as min_temperature,
            MAX(temp) as max_temperature
        FROM rawdata_iot_sensors
        """
        raw_df = pd.read_sql(raw_query, conn)
        
        # Cleaned data summary
        cleaned_query = """
        SELECT 
            COUNT(*) as total_records,
            AVG(data_quality_score) as avg_quality_score,
            MIN(data_quality_score) as min_quality_score,
            MAX(data_quality_score) as max_quality_score
        FROM cleaneddata_iot_sensors
        """
        cleaned_df = pd.read_sql(cleaned_query, conn)
        
        # Pipeline metadata summary
        metadata_query = """
        SELECT 
            COUNT(*) as total_runs,
            COUNT(CASE WHEN pipeline_status = 'SUCCESS' THEN 1 END) as successful_runs,
            AVG(processing_duration_seconds) as avg_processing_time,
            SUM(rawdata_records_ingested) as total_records_processed,
            MIN(created_timestamp) as first_run,
            MAX(created_timestamp) as last_run
        FROM pipeline_metadata
        """
        metadata_df = pd.read_sql(metadata_query, conn)
    
    conn.close()
        return raw_df, cleaned_df, metadata_df
    except Exception as e:
        print(f"Database connection error: {e}")
        conn.close()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def load_sample_data():
    """Load sample data from all three tables"""
    conn = get_db_connection()
    try:
        # Sample raw data
        raw_sample_query = """
        SELECT 
            "room_id/id" as device_id,
            noted_date as reading_time,
            temp as temperature,
            "out/in" as location_type,
            room,
            processing_timestamp
        FROM rawdata_iot_sensors 
        ORDER BY processing_timestamp DESC 
        LIMIT 10
        """
        raw_sample = pd.read_sql(raw_sample_query, conn)
        
        # Sample cleaned data
        cleaned_sample_query = """
        SELECT 
            "room_id/id" as device_id,
            noted_date as reading_time,
            temp as temperature,
            "out/in" as location_type,
            data_quality_score,
            processing_timestamp
        FROM cleaneddata_iot_sensors 
        ORDER BY processing_timestamp DESC 
        LIMIT 10
        """
        cleaned_sample = pd.read_sql(cleaned_sample_query, conn)
        
        # Sample analytics data (if exists)
        analytics_sample_query = """
        SELECT 
            device_id,
            avg_temperature,
            record_count,
            data_quality_score
        FROM analyticsdata_device_metrics 
        ORDER BY record_count DESC 
        LIMIT 10
        """
        try:
            analytics_sample = pd.read_sql(analytics_sample_query, conn)
        except:
            analytics_sample = pd.DataFrame()
        
        conn.close()
        return raw_sample, cleaned_sample, analytics_sample
    except Exception as e:
        print(f"Database connection error: {e}")
        conn.close()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def load_deduplication_insights():
    """Load deduplication insights from Silver layer"""
    conn = get_db_connection()
    try:
        # Get duplicate detection insights
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT CONCAT("id", "room_id/id", "noted_date")) as unique_records,
            COUNT(*) - COUNT(DISTINCT CONCAT("id", "room_id/id", "noted_date")) as potential_duplicates,
            AVG(data_quality_score) as avg_quality_score,
            MAX(cleaning_timestamp) as latest_cleaning,
            COUNT(DISTINCT batch_file) as batches_processed
        FROM cleaneddata_iot_sensors
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        return pd.DataFrame()

def load_duplicate_scenarios():
    """Load information about duplicate scenarios detected"""
    conn = get_db_connection()
    try:
        # Get records with same key but different timestamps (potential duplicates)
        query = """
        SELECT 
            "id",
            "room_id/id" as device_id,
            "noted_date" as reading_time,
            temp as temperature,
            "out/in" as location_type,
            cleaning_timestamp,
            batch_file,
            data_quality_score,
            ROW_NUMBER() OVER (PARTITION BY "id", "room_id/id", "noted_date" ORDER BY cleaning_timestamp DESC) as record_rank
        FROM cleaneddata_iot_sensors
        ORDER BY "id", "room_id/id", "noted_date", cleaning_timestamp DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        return pd.DataFrame()

def load_anomaly_insights():
    """Load anomaly detection insights from Silver layer"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) as anomaly_count,
            SUM(CASE WHEN is_extreme_temp = true THEN 1 ELSE 0 END) as extreme_temp_count,
            SUM(CASE WHEN is_negative_indoor = true THEN 1 ELSE 0 END) as negative_indoor_count,
            SUM(CASE WHEN is_outlier_indoor = true THEN 1 ELSE 0 END) as outlier_indoor_count,
            SUM(CASE WHEN is_outlier_outdoor = true THEN 1 ELSE 0 END) as outlier_outdoor_count,
            AVG(CASE WHEN is_anomaly = false THEN data_quality_score END) as avg_quality_normal,
            AVG(CASE WHEN is_anomaly = true THEN data_quality_score END) as avg_quality_anomaly
        FROM cleaneddata_iot_sensors
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        return pd.DataFrame()

def load_missing_value_insights():
    """Load missing value treatment insights from Silver layer"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN missing_values_treated = 'temp_imputed_indoor' THEN 1 ELSE 0 END) as temp_imputed_indoor,
            SUM(CASE WHEN missing_values_treated = 'temp_imputed_outdoor' THEN 1 ELSE 0 END) as temp_imputed_outdoor,
            SUM(CASE WHEN missing_values_treated = 'location_imputed' THEN 1 ELSE 0 END) as location_imputed,
            SUM(CASE WHEN missing_values_treated = 'no_imputation' THEN 1 ELSE 0 END) as no_imputation,
            COUNT(DISTINCT missing_values_treated) as imputation_types
        FROM cleaneddata_iot_sensors
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        return pd.DataFrame()

def load_anomaly_details():
    """Load detailed anomaly information"""
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            "id",
            "room_id/id" as device_id,
            "noted_date" as reading_time,
            temp as temperature,
            "out/in" as location_type,
            is_anomaly,
            is_extreme_temp,
            is_negative_indoor,
            is_outlier_indoor,
            is_outlier_outdoor,
            anomaly_type,
            data_quality_score,
            cleaning_timestamp,
            batch_file
        FROM cleaneddata_iot_sensors
        WHERE is_anomaly = true
        ORDER BY cleaning_timestamp DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        return pd.DataFrame()

def main():
    # Page configuration
    st.set_page_config(
        page_title="Data Ingestion Insights",
        layout="wide"
    )
    
    # Title and header
    st.title("Data Ingestion Insights")
    st.markdown("Real-time monitoring of IoT data pipeline performance and anomaly detection")
    
    # Load data
    try:
        raw_df = load_raw_data()
        cleaned_df = load_cleaned_data()
        device_df, location_df, timeseries_df = load_analytics_data()
        metadata_df = load_pipeline_metadata()
    except Exception as e:
        st.error(f"Database connection error: {e}")
        st.stop()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a view:",
        ["Overview", "Data Summary", "Data Quality", "Deduplication Insights", "Anomaly & Missing Value Insights"]
    )
    
    if page == "Overview":
        st.header("Pipeline Overview")
        
        # Enhanced Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.subheader("Data Ingestion")
            if not raw_df.empty:
                st.metric("Total Records", f"{len(raw_df):,}")
                unique_sources = raw_df['source_file'].nunique() if 'source_file' in raw_df.columns else 0
                st.metric("Data Sources", f"{unique_sources}")
                if 'processing_timestamp' in raw_df.columns:
                    # Convert to datetime if it's not already
                    raw_df['processing_timestamp'] = pd.to_datetime(raw_df['processing_timestamp'], errors='coerce')
                    latest_data = raw_df['processing_timestamp'].max()
                    st.metric("Latest Data", latest_data.strftime("%H:%M:%S") if pd.notna(latest_data) else "N/A")
            else:
                st.metric("Total Records", "No data available")
                st.metric("Data Sources", "No data available")
                st.metric("Latest Data", "No data available")
        
        with col2:
            st.subheader("Data Quality")
            if not cleaned_df.empty:
                st.metric("Cleaned Records", f"{len(cleaned_df):,}")
                if 'data_quality_score' in cleaned_df.columns:
                    # Convert to numeric, handling any string values
                    quality_scores = pd.to_numeric(cleaned_df['data_quality_score'], errors='coerce')
                    avg_quality = quality_scores.mean()
                    st.metric("Avg Quality Score", f"{avg_quality:.2f}" if pd.notna(avg_quality) else "N/A")
                if 'location_type' in cleaned_df.columns:
                    unique_locations = cleaned_df['location_type'].nunique()
                    st.metric("Location Types", f"{unique_locations}")
            else:
                st.metric("Cleaned Records", "No data available")
                st.metric("Avg Quality Score", "No data available")
                st.metric("Location Types", "No data available")
        
        with col3:
            st.subheader("Pipeline Performance")
            if not metadata_df.empty:
                total_runs = len(metadata_df)
                successful_runs = (metadata_df['success_status'] == 'SUCCESS').sum()
                success_rate = (successful_runs / total_runs) * 100 if total_runs > 0 else 0
                st.metric("Total Runs", f"{total_runs}")
                st.metric("Success Rate", f"{success_rate:.1f}%")
                if 'processing_duration_seconds' in metadata_df.columns:
                    avg_duration = metadata_df['processing_duration_seconds'].mean()
                    st.metric("Avg Duration", f"{avg_duration:.1f}s")
            else:
                st.metric("Total Runs", "No data available")
                st.metric("Success Rate", "No data available")
                st.metric("Avg Duration", "No data available")
        
        with col4:
            st.subheader("System Status")
            if not raw_df.empty and not cleaned_df.empty:
                st.success("System Active")
                st.caption("Data flowing through all layers")
            elif not raw_df.empty:
                st.warning("Partial Data")
                st.caption("Raw data only, processing pending")
            else:
                st.error("No Data")
                st.caption("System idle or error")
        
        if not metadata_df.empty:
            latest_run = metadata_df.iloc[0]['success_status'] if len(metadata_df) > 0 else 'Unknown'
            if latest_run == 'SUCCESS':
                st.success("Last Run: Success")
            else:
                st.error("Last Run: Failed")
        else:
            st.info("No pipeline runs")
        
        # Charts
        if not raw_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Recent Data Flow")
                if 'reading_time' in raw_df.columns:
                    # Convert to datetime with error handling
                    raw_df['reading_time'] = pd.to_datetime(raw_df['reading_time'], errors='coerce')
                    # Only proceed if we have valid datetime values
                    if not raw_df['reading_time'].isna().all():
                        daily_counts = raw_df.groupby(raw_df['reading_time'].dt.date).size().reset_index()
                        daily_counts.columns = ['Date', 'Records']
                        fig = px.line(daily_counts, x='Date', y='Records', title="Records per Day")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("Unable to parse datetime values for chart")
                else:
                    st.info("No reading_time column available for chart")
            
            with col2:
                st.subheader("Data Sources")
                if 'source_file' in raw_df.columns:
                    source_counts = raw_df['source_file'].value_counts().head(10)
                    fig = px.bar(x=source_counts.index, y=source_counts.values, title="Top Data Sources")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for visualization")
    
    elif page == "Data Summary":
        st.header("Data Summary - Three Tables Overview")
        
        # Load summary data
        raw_summary, cleaned_summary, metadata_summary = load_data_summary()
        raw_sample, cleaned_sample, analytics_sample = load_sample_data()
        
        if raw_summary.empty and cleaned_summary.empty and metadata_summary.empty:
            st.info("No data available in any tables")
            return
        
        # Summary metrics in columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("Raw Data Layer")
            if not raw_summary.empty:
                raw_data = raw_summary.iloc[0]
                st.metric("Total Records", f"{int(raw_data['total_records']):,}")
                st.metric("Unique Devices", int(raw_data['unique_devices']))
                st.metric("Unique Rooms", int(raw_data['unique_rooms']))
                st.metric("Location Types", int(raw_data['location_types']))
                st.metric("Avg Temperature", f"{raw_data['avg_temperature']:.1f}°C")
                st.metric("Temp Range", f"{raw_data['min_temperature']:.1f}°C - {raw_data['max_temperature']:.1f}°C")
            else:
                st.info("No raw data available")
        
        with col2:
            st.subheader("Cleaned Data Layer")
            if not cleaned_summary.empty:
                cleaned_data = cleaned_summary.iloc[0]
                st.metric("Total Records", f"{int(cleaned_data['total_records']):,}")
                st.metric("Avg Quality Score", f"{cleaned_data['avg_quality_score']:.2f}")
                st.metric("Min Quality Score", f"{cleaned_data['min_quality_score']:.2f}")
                st.metric("Max Quality Score", f"{cleaned_data['max_quality_score']:.2f}")
            else:
                st.info("No cleaned data available")
        
        with col3:
            st.subheader("Pipeline Metadata")
            if not metadata_summary.empty:
                meta_data = metadata_summary.iloc[0]
                st.metric("Total Runs", int(meta_data['total_runs']))
                st.metric("Successful Runs", int(meta_data['successful_runs']))
                success_rate = (meta_data['successful_runs'] / meta_data['total_runs'] * 100) if meta_data['total_runs'] > 0 else 0
                st.metric("Success Rate", f"{success_rate:.1f}%")
                st.metric("Avg Processing Time", f"{meta_data['avg_processing_time']:.2f}s")
                st.metric("Total Records Processed", f"{int(meta_data['total_records_processed']):,}")
            else:
                st.info("No pipeline metadata available")
        
        # Sample data tables
        st.subheader("Sample Data from All Tables")
        
        tab1, tab2, tab3 = st.tabs(["Raw Data Sample", "Cleaned Data Sample", "Analytics Data Sample"])
        
        with tab1:
            if not raw_sample.empty:
                st.write("**Latest 10 Raw Data Records:**")
                st.dataframe(raw_sample, use_container_width=True)
            else:
                st.info("No raw data sample available")
        
        with tab2:
            if not cleaned_sample.empty:
                st.write("**Latest 10 Cleaned Data Records:**")
                st.dataframe(cleaned_sample, use_container_width=True)
            else:
                st.info("No cleaned data sample available")
        
        with tab3:
            if not analytics_sample.empty:
                st.write("**Top 10 Analytics Records by Record Count:**")
                st.dataframe(analytics_sample, use_container_width=True)
            else:
                st.info("No analytics data sample available")
        
        # Data flow visualization
        st.subheader("Data Flow Status")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.success("IoT Simulation")
            st.caption("Generating data every 5 seconds")
        
        with col2:
            st.success("Landing Zone")
            st.caption("Raw CSV files stored")
        
        with col3:
            st.success("Processing Pipeline")
            st.caption("Three-layered data processing")
        
        with col4:
            st.success("Database Storage")
            st.caption("PostgreSQL with all layers")
    
    elif page == "Data Quality":
        st.header("Data Quality Metrics")
        
        if cleaned_df.empty:
            st.info("No cleaned data available")
            return
        
        # Data quality metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'data_quality_score' in cleaned_df.columns:
                quality_scores = pd.to_numeric(cleaned_df['data_quality_score'], errors='coerce')
                avg_quality = quality_scores.mean()
                st.metric("Average Quality Score", f"{avg_quality:.2f}" if pd.notna(avg_quality) else "N/A")
            else:
                st.metric("Average Quality Score", "N/A")
        
        with col2:
            if 'reading_time' in cleaned_df.columns:
                cleaned_df['reading_time'] = pd.to_datetime(cleaned_df['reading_time'], errors='coerce')
                latest_data = cleaned_df['reading_time'].max()
                st.metric("Latest Processing", latest_data.strftime("%Y-%m-%d %H:%M") if pd.notna(latest_data) else "N/A")
            else:
                st.metric("Latest Processing", "N/A")
        
        with col3:
            if 'reading_time' in cleaned_df.columns:
                daily_counts = cleaned_df.groupby(cleaned_df['reading_time'].dt.date).size()
                st.metric("Daily Records", f"{daily_counts.iloc[-1] if len(daily_counts) > 0 else 0}")
            else:
                st.metric("Daily Records", "N/A")
        
        # Quality trends
        if 'reading_time' in cleaned_df.columns and 'data_quality_score' in cleaned_df.columns:
            st.subheader("Quality Score Trends")
            # Convert quality scores to numeric first
            cleaned_df['data_quality_score'] = pd.to_numeric(cleaned_df['data_quality_score'], errors='coerce')
            daily_quality = cleaned_df.groupby(cleaned_df['reading_time'].dt.date)['data_quality_score'].mean().reset_index()
            daily_quality.columns = ['Date', 'Quality Score']
            fig = px.line(daily_quality, x='Date', y='Quality Score', title="Average Quality Score Over Time")
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Deduplication Insights":
        st.header("Silver Layer Deduplication Insights")
        st.markdown("**Understanding how the Silver layer handles duplicates and maintains data quality**")
        
        # Load deduplication data
        dedup_insights = load_deduplication_insights()
        duplicate_scenarios = load_duplicate_scenarios()
        
        if dedup_insights.empty:
            st.info("No deduplication data available")
            return
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_records = int(dedup_insights.iloc[0]['total_records'])
            st.metric("Total Records", f"{total_records:,}")
        
        with col2:
            unique_records = int(dedup_insights.iloc[0]['unique_records'])
            st.metric("Unique Records", f"{unique_records:,}")
        
        with col3:
            potential_duplicates = int(dedup_insights.iloc[0]['potential_duplicates'])
            st.metric("Potential Duplicates", f"{potential_duplicates:,}")
        
        with col4:
            dedup_rate = (potential_duplicates / total_records * 100) if total_records > 0 else 0
            st.metric("Deduplication Rate", f"{dedup_rate:.1f}%")
        
        st.markdown("---")
        
        # Deduplication explanation
        st.subheader("How Silver Layer Deduplication Works")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **🔑 Unique Key Strategy:**
            - **Primary Key**: `(id, room_id/id, noted_date)`
            - **Purpose**: Identifies unique sensor readings
            - **Behavior**: Latest record wins (UPSERT)
            
            **🔄 Duplicate Scenarios Handled:**
            - **Network Retries**: Same data, different timestamps
            - **Data Corrections**: Updated temperature readings
            - **Reprocessing**: Same data, new batch files
            - **Sensor Faults**: Corrected readings
            """)
        
        with col2:
            st.markdown("""
            **⚡ UPSERT Operations:**
            - **INSERT**: New unique records
            - **UPDATE**: Existing records with latest data
            - **Efficiency**: Only processes changed records
            
            **Quality Metrics:**
            - **Data Quality Score**: 0.0 - 1.0 scale
            - **High Quality**: ≥ 0.8 (normal temperature range)
            - **Medium Quality**: 0.5 - 0.8 (acceptable range)
            - **Low Quality**: < 0.5 (anomalous readings)
            """)
        
        # Duplicate scenarios table
        if not duplicate_scenarios.empty:
            st.subheader("Duplicate Scenarios Detected")
            
            # Filter for records that might be duplicates (rank > 1)
            potential_dups = duplicate_scenarios[duplicate_scenarios['record_rank'] > 1]
            
            if not potential_dups.empty:
                st.dataframe(
                    potential_dups[['device_id', 'reading_time', 'temperature', 'location_type', 'batch_file', 'data_quality_score']].head(10),
                    use_container_width=True
                )
                
                # Show duplicate patterns
                st.subheader("Duplicate Patterns")
                pattern_counts = potential_dups.groupby(['device_id', 'reading_time']).size().reset_index(name='duplicate_count')
                pattern_counts = pattern_counts[pattern_counts['duplicate_count'] > 1].sort_values('duplicate_count', ascending=False)
                
                if not pattern_counts.empty:
                    st.dataframe(pattern_counts.head(10), use_container_width=True)
                else:
                    st.info("No duplicate patterns detected")
            else:
                st.success("No duplicate records detected - Silver layer is working perfectly!")
        
        # Quality distribution
        st.subheader("Data Quality Distribution")
        if not duplicate_scenarios.empty:
            quality_dist = duplicate_scenarios['data_quality_score'].value_counts().sort_index()
        
        fig = px.bar(
                x=quality_dist.index, 
                y=quality_dist.values,
                title="Data Quality Score Distribution",
                labels={'x': 'Quality Score', 'y': 'Number of Records'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
        # Batch processing insights
        if not duplicate_scenarios.empty:
            st.subheader("Batch Processing Insights")
            # Convert quality scores to numeric first
            duplicate_scenarios['data_quality_score'] = pd.to_numeric(duplicate_scenarios['data_quality_score'], errors='coerce')
            batch_insights = duplicate_scenarios.groupby('batch_file').agg({
                'device_id': 'count',
                'data_quality_score': 'mean'
            }).rename(columns={'device_id': 'record_count', 'data_quality_score': 'avg_quality'})
            
            st.dataframe(batch_insights.head(10), use_container_width=True)
    
    elif page == "Anomaly & Missing Value Insights":
        st.header("Silver Layer: Anomaly Detection & Missing Value Treatment")
        st.markdown("**Comprehensive insights into data quality issues and their treatment**")
        
        # Load insights data
        anomaly_insights = load_anomaly_insights()
        missing_insights = load_missing_value_insights()
        anomaly_details = load_anomaly_details()
        
        if anomaly_insights.empty and missing_insights.empty:
            st.info("No anomaly or missing value data available")
            return
        
        # Anomaly Detection Metrics
        st.subheader("🚨 Anomaly Detection Metrics")
        
        if not anomaly_insights.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_records = int(anomaly_insights.iloc[0]['total_records'])
                st.metric("Total Records", f"{total_records:,}")
            
            with col2:
                anomaly_count = int(anomaly_insights.iloc[0]['anomaly_count'])
                st.metric("Anomalies Detected", f"{anomaly_count:,}")
            
            with col3:
                anomaly_rate = (anomaly_count / total_records * 100) if total_records > 0 else 0
                st.metric("Anomaly Rate", f"{anomaly_rate:.1f}%")
            
            with col4:
                avg_quality_normal = float(anomaly_insights.iloc[0]['avg_quality_normal']) if anomaly_insights.iloc[0]['avg_quality_normal'] else 0
                st.metric("Avg Quality (Normal)", f"{avg_quality_normal:.2f}")
        
        # Anomaly Types Breakdown
        if not anomaly_insights.empty:
            st.subheader("Anomaly Types Breakdown")
            
            col1, col2 = st.columns(2)
            
            with col1:
                extreme_temp = int(anomaly_insights.iloc[0]['extreme_temp_count'])
                negative_indoor = int(anomaly_insights.iloc[0]['negative_indoor_count'])
                
                anomaly_types_data = {
                    'Anomaly Type': ['Extreme Temperature', 'Negative Indoor', 'Outlier Indoor', 'Outlier Outdoor'],
                    'Count': [
                        extreme_temp,
                        negative_indoor,
                        int(anomaly_insights.iloc[0]['outlier_indoor_count']),
                        int(anomaly_insights.iloc[0]['outlier_outdoor_count'])
                    ]
                }
                
                anomaly_df = pd.DataFrame(anomaly_types_data)
                fig = px.bar(anomaly_df, x='Anomaly Type', y='Count', title="Anomaly Types Distribution")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Quality score comparison
                normal_quality = float(anomaly_insights.iloc[0]['avg_quality_normal']) if anomaly_insights.iloc[0]['avg_quality_normal'] else 0
                anomaly_quality = float(anomaly_insights.iloc[0]['avg_quality_anomaly']) if anomaly_insights.iloc[0]['avg_quality_anomaly'] else 0
                
                quality_comparison = {
                    'Data Type': ['Normal Records', 'Anomalous Records'],
                    'Avg Quality Score': [normal_quality, anomaly_quality]
                }
                
                quality_df = pd.DataFrame(quality_comparison)
                fig = px.bar(quality_df, x='Data Type', y='Avg Quality Score', title="Quality Score Comparison")
                st.plotly_chart(fig, use_container_width=True)
        
        # Missing Value Treatment Metrics
        st.subheader("🔧 Missing Value Treatment Metrics")
        
        if not missing_insights.empty:
            col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            temp_indoor = int(missing_insights.iloc[0]['temp_imputed_indoor'])
            st.metric("Temp Imputed (Indoor)", f"{temp_indoor:,}")
        
        with col2:
            temp_outdoor = int(missing_insights.iloc[0]['temp_imputed_outdoor'])
            st.metric("Temp Imputed (Outdoor)", f"{temp_outdoor:,}")
        
        with col3:
            location_imputed = int(missing_insights.iloc[0]['location_imputed'])
            st.metric("Location Imputed", f"{location_imputed:,}")
        
        with col4:
            no_imputation = int(missing_insights.iloc[0]['no_imputation'])
            st.metric("No Imputation Needed", f"{no_imputation:,}")
        
        # Missing Value Treatment Visualization
        if not missing_insights.empty:
            st.subheader("Missing Value Treatment Distribution")
            
            imputation_data = {
                'Treatment Type': [
                    'Temperature (Indoor)', 'Temperature (Outdoor)', 
                    'Location Data', 'No Imputation'
                ],
                'Count': [
                    int(missing_insights.iloc[0]['temp_imputed_indoor']),
                    int(missing_insights.iloc[0]['temp_imputed_outdoor']),
                    int(missing_insights.iloc[0]['location_imputed']),
                    int(missing_insights.iloc[0]['no_imputation'])
                ]
            }
            
            imputation_df = pd.DataFrame(imputation_data)
            fig = px.pie(imputation_df, values='Count', names='Treatment Type', title="Missing Value Treatment Distribution")
        st.plotly_chart(fig, use_container_width=True)
    
        # Detailed Anomaly Table
        if not anomaly_details.empty:
            st.subheader("Detailed Anomaly Records")
            
            # Filter columns for display
            display_columns = ['device_id', 'reading_time', 'temperature', 'location_type', 'anomaly_type', 'data_quality_score', 'batch_file']
            available_columns = [col for col in display_columns if col in anomaly_details.columns]
            
            st.dataframe(
                anomaly_details[available_columns].head(20),
                use_container_width=True
            )
            
            # Download button for anomaly data
            csv = anomaly_details[available_columns].to_csv(index=False)
            st.download_button(
                label="Download Anomaly Data as CSV",
                data=csv,
                file_name=f"anomaly_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        # Data Quality Insights
        st.subheader("📋 Data Quality Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Anomaly Detection Rules:**
            - **Extreme Temperature**: < -50°C or > 100°C
            - **Negative Indoor**: < 0°C for indoor sensors
            - **Outlier Indoor**: > 40°C for indoor sensors
            - **Outlier Outdoor**: < -30°C for outdoor sensors
            - **Impossible Values**: 999, -999, 1000
            """)
        
        with col2:
            st.markdown("""
            **🔧 Missing Value Treatment:**
            - **Temperature**: 22°C (indoor), 20°C (outdoor)
            - **Location**: "Unknown" for missing room data
            - **Timestamp**: Current timestamp for missing dates
            - **Quality Score**: 0.0 for anomalies, 1.0 for normal data
            """)
    
    # Removed duplicate Anomaly Detection page - using "Anomaly & Missing Value Insights" instead
    elif page == "Anomaly Detection":
        st.info("This page has been consolidated. Please use 'Anomaly & Missing Value Insights' for comprehensive anomaly detection and data quality analysis.")
        return
    
    # Pipeline execution history
    if not metadata_df.empty:
        st.sidebar.markdown("---")
        st.sidebar.subheader("Pipeline Execution History")
        
        total_runs = len(metadata_df)
        successful_runs = (metadata_df['success_status'] == 'SUCCESS').sum()
        success_rate = (successful_runs / total_runs) * 100 if total_runs > 0 else 0
        
        avg_processing_time = 0
        if 'processing_start_time' in metadata_df.columns and 'processing_end_time' in metadata_df.columns:
            metadata_df['processing_start_time'] = pd.to_datetime(metadata_df['processing_start_time'], errors='coerce')
            metadata_df['processing_end_time'] = pd.to_datetime(metadata_df['processing_end_time'], errors='coerce')
            processing_times = (metadata_df['processing_end_time'] - metadata_df['processing_start_time']).dt.total_seconds()
            avg_processing_time = processing_times.mean() if not processing_times.empty else 0
        
        records_per_run = metadata_df['records_processed'].mean() if 'records_processed' in metadata_df.columns else 0
        if 'processing_start_time' in metadata_df.columns:
            metadata_df['processing_start_time'] = pd.to_datetime(metadata_df['processing_start_time'], errors='coerce')
            latest_run = metadata_df['processing_start_time'].max()
        else:
            latest_run = "N/A"
        
        st.sidebar.metric("Total Pipeline Runs", f"{total_runs} completed runs")
        st.sidebar.metric("Success Rate", f"{success_rate:.0f}% ({successful_runs} runs successful)")
        st.sidebar.metric("Average Processing Time", f"{avg_processing_time:.2f} seconds")
        st.sidebar.metric("Records Processed", f"{records_per_run:.0f} record per run")
        st.sidebar.metric("Latest Run", latest_run.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(latest_run) else "N/A")

if __name__ == "__main__":
    main()