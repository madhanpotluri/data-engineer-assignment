#!/usr/bin/env python3
"""
Simple IoT Dashboard - No more issues!
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

# Page config
st.set_page_config(
    page_title="IoT Data Dashboard",
    layout="wide"
)

def get_db_connection():
    """Simple database connection"""
    try:
        return psycopg2.connect(
            host="postgres",
            port="5432",
            database="iot_data",
            user="postgres",
            password="postgres123"
        )
    except Exception as e:
        st.error(f"Database error: {e}")
        return None

def main():
    st.title("IoT Data Dashboard")
    st.write("Simple and working dashboard with anomaly detection")
    
    # Get database connection
    conn = get_db_connection()
    if not conn:
        st.stop()
    
    try:
        # Basic metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            raw_count = pd.read_sql("SELECT COUNT(*) as count FROM rawdata_iot_sensors", conn).iloc[0]['count']
            st.metric("Raw Data Records", raw_count)
        
        with col2:
            cleaned_count = pd.read_sql("SELECT COUNT(*) as count FROM cleaneddata_iot_sensors", conn).iloc[0]['count']
            st.metric("Cleaned Data Records", cleaned_count)
        
        with col3:
            anomaly_count = pd.read_sql("SELECT COUNT(*) as count FROM cleaneddata_iot_sensors WHERE is_anomaly = 'True'", conn).iloc[0]['count']
            st.metric("Anomalies Detected", anomaly_count)
            st.write(f"Debug: Found {anomaly_count} anomalies")
        
        # Anomaly detection section
        st.subheader("Anomaly Detection")
        
        # Anomaly summary
        anomaly_summary = pd.read_sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN is_anomaly = 'True' THEN 1 END) as anomaly_count,
                COUNT(CASE WHEN is_extreme_temp = 'True' THEN 1 END) as extreme_temp,
                COUNT(CASE WHEN is_negative_indoor = 'True' THEN 1 END) as negative_indoor,
                COUNT(CASE WHEN is_outlier_indoor = 'True' THEN 1 END) as outlier_indoor,
                COUNT(CASE WHEN is_outlier_outdoor = 'True' THEN 1 END) as outlier_outdoor
            FROM cleaneddata_iot_sensors
        """, conn)
        
        if not anomaly_summary.empty:
            total = anomaly_summary.iloc[0]['total_records']
            anomalies = anomaly_summary.iloc[0]['anomaly_count']
            anomaly_rate = (anomalies / total * 100) if total > 0 else 0
            
            st.write(f"**Anomaly Rate:** {anomaly_rate:.1f}% ({anomalies} out of {total} records)")
            
            # Anomaly types breakdown
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Extreme Temperature", anomaly_summary.iloc[0]['extreme_temp'])
            with col2:
                st.metric("Negative Indoor", anomaly_summary.iloc[0]['negative_indoor'])
            with col3:
                st.metric("Outlier Indoor", anomaly_summary.iloc[0]['outlier_indoor'])
            with col4:
                st.metric("Outlier Outdoor", anomaly_summary.iloc[0]['outlier_outdoor'])
        
        # Recent anomalies
        st.subheader("Recent Anomalies")
        recent_anomalies = pd.read_sql("""
            SELECT 
                "room_id/id" as device_id,
                noted_date as reading_time,
                temp as temperature,
                "out/in" as location_type,
                data_quality_score,
                anomaly_type
            FROM cleaneddata_iot_sensors 
            WHERE is_anomaly = 'True'
            ORDER BY noted_date DESC 
            LIMIT 20
        """, conn)
        
        if not recent_anomalies.empty:
            st.dataframe(recent_anomalies)
        else:
            st.info("No anomalies found in the data")
        
        # Sample raw data
        st.subheader("Sample Raw Data")
        raw_data = pd.read_sql("SELECT * FROM rawdata_iot_sensors LIMIT 10", conn)
        st.dataframe(raw_data)
        
        # Sample cleaned data
        st.subheader("Sample Cleaned Data")
        cleaned_data = pd.read_sql("SELECT * FROM cleaneddata_iot_sensors LIMIT 10", conn)
        st.dataframe(cleaned_data)
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
