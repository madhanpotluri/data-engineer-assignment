#!/usr/bin/env python3
"""
IoT Data Pipeline Dashboard
Simple web dashboard to visualize PostgreSQL data
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from datetime import datetime, timedelta
import os

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
# @st.cache_data  # Temporarily disabled for fresh start
def load_raw_data():
    conn = get_db_connection()
    query = """
    SELECT 
        "room_id/id" as device_id, 
        noted_date as reading_time, 
        temp as temperature, 
        "out/in" as location_type, 
        processing_timestamp
    FROM rawdata_iot_sensors 
    ORDER BY noted_date DESC 
    LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# @st.cache_data  # Temporarily disabled for fresh start
def load_cleaned_data():
    conn = get_db_connection()
    query = """
    SELECT 
        "room_id/id" as device_id, 
        noted_date as reading_time, 
        temp as temperature, 
        "out/in" as location_type, 
        processing_timestamp,
        data_quality_score
    FROM cleaneddata_iot_sensors 
    ORDER BY noted_date DESC 
    LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# @st.cache_data  # Temporarily disabled for fresh start
def load_analytics_data():
    conn = get_db_connection()
    
    # Device metrics
    device_query = "SELECT * FROM analyticsdata_device_metrics"
    device_df = pd.read_sql(device_query, conn)
    
    # Location metrics
    location_query = "SELECT * FROM analyticsdata_location_metrics"
    location_df = pd.read_sql(location_query, conn)
    
    # Time metrics
    time_query = "SELECT * FROM analyticsdata_time_metrics ORDER BY hour"
    time_df = pd.read_sql(time_query, conn)
    
    # Quality summary
    quality_query = "SELECT * FROM analyticsdata_quality_summary"
    quality_df = pd.read_sql(quality_query, conn)
    
    conn.close()
    return device_df, location_df, time_df, quality_df

# @st.cache_data  # Temporarily disabled for fresh start
def load_pipeline_metadata():
    conn = get_db_connection()
    
    # Check if pipeline_metadata table exists
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'pipeline_metadata'
    );
    """
    
    table_exists = pd.read_sql(check_table_query, conn).iloc[0, 0]
    
    if table_exists:
        # Load pipeline metadata
        metadata_query = """
        SELECT 
            pipeline_run_id,
            input_file_name,
            processing_start_time,
            processing_end_time,
            processing_duration_seconds,
            rawdata_records_ingested,
            cleaneddata_records_processed,
            records_rejected,
            success_rate_percent,
            rejection_rate_percent,
            pipeline_status,
            data_quality_score,
            created_timestamp
        FROM pipeline_metadata 
        ORDER BY processing_start_time DESC
        """
        metadata_df = pd.read_sql(metadata_query, conn)
    else:
        # Return empty DataFrame if table doesn't exist
        metadata_df = pd.DataFrame()
    
    conn.close()
    return metadata_df

def main():
    st.set_page_config(
        page_title="Data Ingestion Insights",
        layout="wide"
    )
    
    st.title("Data Ingestion Insights")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["Overview", "Pipeline Runs", "Raw Data", "Processed Data", "Anomaly Detection", "Data Quality"]
    )
    
    if page == "Overview":
        st.header("Overview")
        
        # Load data
        device_df, location_df, time_df, quality_df = load_analytics_data()
        
        # Load real data for metrics
        raw_df = load_raw_data()
        cleaned_df = load_cleaned_data()
        metadata_df = load_pipeline_metadata()
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_records = len(cleaned_df) if not cleaned_df.empty else 0
            st.metric("Total Records", f"{total_records:,}", "Processed" if total_records > 0 else "No Data")
        
        with col2:
            unique_devices = raw_df['device_id'].nunique() if not raw_df.empty else 0
            st.metric("Data Sources", f"{unique_devices} Device(s)", "Active" if unique_devices > 0 else "None")
        
        with col3:
            unique_locations = raw_df['location_type'].nunique() if not raw_df.empty else 0
            st.metric("Location Types", f"{unique_locations}", "In/Out" if unique_locations > 0 else "None")
        
        with col4:
            pipeline_runs = len(metadata_df) if not metadata_df.empty else 0
            status = "Active" if pipeline_runs > 0 else "Inactive"
            st.metric("Pipeline Status", f"{pipeline_runs} Runs", status)
        
        # Temperature distribution
        st.subheader("Temperature Distribution")
        
        if not cleaned_df.empty:
            fig = px.histogram(
                cleaned_df, 
                x='temperature', 
                nbins=50,
                title="Temperature Reading Distribution",
                labels={'temperature': 'Temperature (°C)', 'count': 'Frequency'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Location comparison
            st.subheader("Indoor vs Outdoor Temperatures")
            location_avg = cleaned_df.groupby('location_type')['temperature'].mean().reset_index()
            
            fig = px.bar(
                location_avg,
                x='location_type',
                y='temperature',
                title="Average Temperature by Location",
                labels={'temperature': 'Average Temperature (°C)', 'location_type': 'Location Type'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available. Start the IoT simulation and processing pipelines to see temperature distributions.")
    
    elif page == "Pipeline Runs":
        st.header("Pipeline Runs")
        st.markdown("**Data ingestion pipeline execution history and metadata**")
        
        # Load pipeline metadata
        metadata_df = load_pipeline_metadata()
        
        if metadata_df.empty:
            st.warning("No pipeline metadata found. Run the pipeline to generate tracking data.")
        else:
            # Summary metrics
            st.subheader("Pipeline Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Runs", len(metadata_df))
            
            with col2:
                avg_duration = metadata_df['processing_duration_seconds'].mean()
                st.metric("Avg Duration", f"{avg_duration:.1f}s")
            
            with col3:
                total_processed = metadata_df['cleaneddata_records_processed'].sum()
                st.metric("Total Processed", f"{total_processed:,}")
            
            with col4:
                avg_success_rate = metadata_df['success_rate_percent'].mean()
                st.metric("Avg Success Rate", f"{avg_success_rate:.1f}%")
            
            # Pipeline runs table
            st.subheader("Pipeline Execution History")
            
            # Format the dataframe for better display
            display_df = metadata_df.copy()
            display_df['processing_start_time'] = pd.to_datetime(display_df['processing_start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            display_df['processing_end_time'] = pd.to_datetime(display_df['processing_end_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            display_df['processing_duration_seconds'] = display_df['processing_duration_seconds'].round(2)
            display_df['success_rate_percent'] = display_df['success_rate_percent'].round(2)
            display_df['rejection_rate_percent'] = display_df['rejection_rate_percent'].round(2)
            display_df['data_quality_score'] = display_df['data_quality_score'].round(3)
            
            # Rename columns for better display
            display_df = display_df.rename(columns={
                'pipeline_run_id': 'Run ID',
                'input_file_name': 'Input File',
                'processing_start_time': 'Start Time',
                'processing_end_time': 'End Time',
                'processing_duration_seconds': 'Duration (s)',
                'rawdata_records_ingested': 'Raw Records',
                'cleaneddata_records_processed': 'Processed Records',
                'records_rejected': 'Rejected Records',
                'success_rate_percent': 'Success Rate (%)',
                'rejection_rate_percent': 'Rejection Rate (%)',
                'pipeline_status': 'Status',
                'data_quality_score': 'Quality Score'
            })
            
            st.dataframe(display_df, use_container_width=True)
            
            # Pipeline performance charts
            st.subheader("Pipeline Performance Trends")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Success rate over time
                fig = px.line(
                    metadata_df,
                    x='processing_start_time',
                    y='success_rate_percent',
                    title="Success Rate Over Time",
                    labels={'success_rate_percent': 'Success Rate (%)', 'processing_start_time': 'Run Time'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Processing duration over time
                fig = px.line(
                    metadata_df,
                    x='processing_start_time',
                    y='processing_duration_seconds',
                    title="Processing Duration Over Time",
                    labels={'processing_duration_seconds': 'Duration (seconds)', 'processing_start_time': 'Run Time'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Data volume trends
            st.subheader("Data Volume Trends")
            fig = px.bar(
                metadata_df,
                x='processing_start_time',
                y=['rawdata_records_ingested', 'cleaneddata_records_processed', 'records_rejected'],
                title="Data Volume by Pipeline Run",
                labels={'value': 'Number of Records', 'processing_start_time': 'Run Time'},
                barmode='group'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Raw Data":
        st.header("Raw Data")
        st.markdown("**Data as ingested from IoT devices**")
        
        raw_df = load_raw_data()
        
        # Data info
        st.subheader("Data Summary")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Records", len(raw_df))
            st.metric("Date Range", f"{str(raw_df['reading_time'].min())} to {str(raw_df['reading_time'].max())}")
        
        with col2:
            st.metric("Temperature Range", f"{raw_df['temperature'].min():.1f}°C to {raw_df['temperature'].max():.1f}°C")
            st.metric("Average Temperature", f"{raw_df['temperature'].mean():.1f}°C")
        
        # Data table
        st.subheader("Sample Data")
        st.dataframe(raw_df.head(20))
        
        # Time series
        st.subheader("Temperature Over Time")
        fig = px.line(
            raw_df.head(100), 
            x='reading_time', 
            y='temperature',
            title="Temperature Readings Over Time (Last 100 records)",
            labels={'reading_time': 'Time', 'temperature': 'Temperature (°C)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Processed Data":
        st.header("Processed Data")
        st.markdown("**Data after cleaning and validation**")
        
        cleaned_df = load_cleaned_data()
        
        # Data quality metrics
        st.subheader("Data Quality Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Valid Records", len(cleaned_df))
        
        with col2:
            null_count = cleaned_df.isnull().sum().sum()
            st.metric("Null Values", null_count)
        
        with col3:
            st.metric("Data Completeness", f"{((len(cleaned_df) - null_count) / len(cleaned_df) * 100):.1f}%")
        
        # Temperature analysis
        st.subheader("Temperature Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.box(
                cleaned_df, 
                x='location_type', 
                y='temperature',
                title="Temperature Distribution by Location",
                labels={'temperature': 'Temperature (°C)', 'location_type': 'Location Type'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(
                cleaned_df.head(500), 
                x='reading_time', 
                y='temperature',
                color='location_type',
                title="Temperature Scatter Plot",
                labels={'reading_time': 'Time', 'temperature': 'Temperature (°C)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Anomaly Detection":
        st.header("Anomaly Detection")
        st.markdown("**Detect unusual patterns and outliers in IoT temperature data**")
        
        cleaned_df = load_cleaned_data()
        
        if not cleaned_df.empty:
            # Statistical Analysis
            st.subheader("Statistical Analysis")
            
            temp_mean = cleaned_df['temperature'].mean()
            temp_std = cleaned_df['temperature'].std()
            temp_min = cleaned_df['temperature'].min()
            temp_max = cleaned_df['temperature'].max()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Mean Temperature", f"{temp_mean:.1f}°C")
            with col2:
                st.metric("Standard Deviation", f"{temp_std:.1f}°C")
            with col3:
                st.metric("Min Temperature", f"{temp_min:.1f}°C")
            with col4:
                st.metric("Max Temperature", f"{temp_max:.1f}°C")
            
            # Anomaly Detection Configuration
            st.subheader("Anomaly Detection Configuration")
            col1, col2 = st.columns(2)
            
            with col1:
                threshold_multiplier = st.slider(
                    "Anomaly Threshold (Standard Deviations)", 
                    min_value=1.0, 
                    max_value=4.0, 
                    value=2.0, 
                    step=0.1,
                    help="Values outside this many standard deviations are considered anomalies"
                )
            
            with col2:
                anomaly_threshold = threshold_multiplier * temp_std
                st.metric("Detection Threshold", f"±{anomaly_threshold:.1f}°C")
            
            # Detect Anomalies
            anomalies = cleaned_df[
                (cleaned_df['temperature'] < temp_mean - anomaly_threshold) | 
                (cleaned_df['temperature'] > temp_mean + anomaly_threshold)
            ]
            
            # Anomaly Summary
            st.subheader("Anomaly Detection Results")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                anomaly_percentage = (len(anomalies) / len(cleaned_df)) * 100
                st.metric("Anomalies Detected", len(anomalies))
            with col2:
                st.metric("Anomaly Rate", f"{anomaly_percentage:.1f}%")
            with col3:
                normal_readings = len(cleaned_df) - len(anomalies)
                st.metric("Normal Readings", normal_readings)
            
            # Anomaly Details
            if len(anomalies) > 0:
                st.subheader("Detected Anomalies")
                
                # Anomaly severity classification
                anomalies_copy = anomalies.copy()
                anomalies_copy['severity'] = anomalies_copy['temperature'].apply(
                    lambda x: 'High' if abs(x - temp_mean) > 3 * temp_std else 'Medium'
                )
                
                st.dataframe(
                    anomalies_copy[['device_id', 'reading_time', 'temperature', 'location_type', 'severity']],
                    use_container_width=True
                )
                
                # Download anomaly data
                csv = anomalies_copy.to_csv(index=False)
                st.download_button(
                    label="Download Anomaly Data as CSV",
                    data=csv,
                    file_name=f"temperature_anomalies_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                # Anomaly Visualization
                st.subheader("Anomaly Visualization")
                
                fig = px.scatter(
                    cleaned_df,
                    x='reading_time',
                    y='temperature',
                    color='location_type',
                    title="Temperature Readings with Anomaly Detection",
                    labels={'reading_time': 'Time', 'temperature': 'Temperature (°C)'},
                    hover_data=['device_id', 'location_type']
                )
                
                # Add threshold lines
                fig.add_hline(y=temp_mean + anomaly_threshold, line_dash="dash", line_color="red", 
                             annotation_text=f"Upper Threshold ({temp_mean + anomaly_threshold:.1f}°C)")
                fig.add_hline(y=temp_mean - anomaly_threshold, line_dash="dash", line_color="red", 
                             annotation_text=f"Lower Threshold ({temp_mean - anomaly_threshold:.1f}°C)")
                fig.add_hline(y=temp_mean, line_dash="dot", line_color="blue", 
                             annotation_text=f"Mean ({temp_mean:.1f}°C)")
                
                # Highlight anomalies
                if len(anomalies) > 0:
                    fig.add_scatter(
                        x=anomalies['reading_time'],
                        y=anomalies['temperature'],
                        mode='markers',
                        marker=dict(color='red', size=10, symbol='x'),
                        name='Anomalies',
                        showlegend=True
                    )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Anomaly Analysis by Location
                if 'location_type' in anomalies.columns:
                    st.subheader("Anomaly Analysis by Location")
                    location_anomalies = anomalies.groupby('location_type').size().reset_index()
                    location_anomalies.columns = ['Location', 'Anomaly Count']
                    
                    fig = px.bar(
                        location_anomalies,
                        x='Location',
                        y='Anomaly Count',
                        title="Anomaly Count by Location",
                        color='Anomaly Count',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.success("No temperature anomalies detected in current data with the selected threshold.")
                
                # Show normal distribution
                st.subheader("Temperature Distribution")
                fig = px.histogram(
                    cleaned_df,
                    x='temperature',
                    nbins=30,
                    title="Temperature Distribution (Normal Range)",
                    labels={'temperature': 'Temperature (°C)', 'count': 'Frequency'}
                )
                fig.add_vline(x=temp_mean, line_dash="dash", line_color="blue", 
                             annotation_text=f"Mean: {temp_mean:.1f}°C")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for anomaly detection. Start the IoT simulation and processing pipelines to see anomaly detection capabilities.")
    
    elif page == "Data Quality":
        st.header("Data Quality")
        st.markdown("**Pipeline health and data quality metrics**")
        
        device_df, location_df, time_df, quality_df = load_analytics_data()
        
        if not quality_df.empty:
            st.subheader("Quality Metrics")
            st.dataframe(quality_df)
        
        # Data freshness
        st.subheader("Data Freshness")
        cleaned_df = load_cleaned_data()
        
        if not cleaned_df.empty and 'processing_timestamp' in cleaned_df.columns:
            latest_data = cleaned_df['processing_timestamp'].max()
            st.metric("Latest Processing", str(latest_data))
        else:
            st.metric("Latest Processing", "No data available")
        
        # Data volume trends
        st.subheader("Data Volume Trends")
        
        if not cleaned_df.empty and 'reading_time' in cleaned_df.columns:
            # Convert reading_time to datetime if it's not already
            cleaned_df['reading_time'] = pd.to_datetime(cleaned_df['reading_time'])
            daily_counts = cleaned_df.groupby(cleaned_df['reading_time'].dt.date).size().reset_index()
            daily_counts.columns = ['date', 'count']
            
            fig = px.line(
                daily_counts,
                x='date',
                y='count',
                title="Daily Data Volume",
                labels={'count': 'Number of Records', 'date': 'Date'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📭 No data available for volume trends. Start the IoT simulation and processing pipelines to see data volume trends.")

if __name__ == "__main__":
    main()
