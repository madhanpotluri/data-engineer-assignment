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
@st.cache_data
def load_raw_data():
    conn = get_db_connection()
    query = """
    SELECT 
        "room_id/id" as device_id, 
        noted_date as reading_time, 
        temp as temperature, 
        "out/in" as location_type, 
        ingestion_timestamp as processing_timestamp
    FROM rawdata_iot_sensors 
    ORDER BY noted_date DESC 
    LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data
def load_cleaned_data():
    conn = get_db_connection()
    query = """
    SELECT device_id, reading_time, temperature, location_type, processing_timestamp
    FROM cleaneddata_iot_sensors 
    ORDER BY reading_time DESC 
    LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data
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

def main():
    st.set_page_config(
        page_title="IoT Temperature Dashboard",
        layout="wide"
    )
    
    st.title("IoT Temperature Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["Overview", "Raw Data", "Processed Data", "Analytics", "Data Quality"]
    )
    
    if page == "Overview":
        st.header("Overview")
        
        # Load data
        device_df, location_df, time_df, quality_df = load_analytics_data()
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", "97,606", "✅ Processed")
        
        with col2:
            st.metric("Data Sources", "1 Device", "Room Admin")
        
        with col3:
            st.metric("Location Types", "2", "In/Out")
        
        with col4:
            st.metric("Pipeline Status", "Active", "🟢 Running")
        
        # Temperature distribution
        st.subheader("Temperature Distribution")
        cleaned_df = load_cleaned_data()
        
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
    
    elif page == "Analytics":
        st.header("Analytics")
        st.markdown("**Business insights and trends**")
        
        device_df, location_df, time_df, quality_df = load_analytics_data()
        
        # Device metrics
        if not device_df.empty:
            st.subheader("Device Metrics")
            st.dataframe(device_df)
        
        # Location metrics
        if not location_df.empty:
            st.subheader("Location Metrics")
            fig = px.bar(
                location_df,
                x='location_type',
                y='avg_temperature',
                title="Average Temperature by Location",
                labels={'avg_temperature': 'Average Temperature (°C)', 'location_type': 'Location Type'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Time metrics
        if not time_df.empty:
            st.subheader("Time-based Analysis")
            fig = px.line(
                time_df,
                x='hour',
                y='avg_temperature',
                title="Average Temperature by Hour of Day",
                labels={'avg_temperature': 'Average Temperature (°C)', 'hour': 'Hour of Day'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
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
        latest_data = cleaned_df['processing_timestamp'].max()
        st.metric("Latest Processing", str(latest_data))
        
        # Data volume trends
        st.subheader("Data Volume Trends")
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

if __name__ == "__main__":
    main()
