import streamlit as st
import pandas as pd
import pymongo

# MongoDB Connection (Docker)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["iiot_database"]
collection = db["sensor_data"]

# Load Data
data = list(collection.find().limit(100))  # Load the latest 100 records
df = pd.DataFrame(data)

# Check if data is empty
if df.empty:
    st.error("No data found in the MongoDB collection. Please check the Kafka consumer.")
else:
    # Check if required columns exist
    required_columns = ['machine_id', 'voltage', 'temperature', 'timestamp']
    if all(col in df.columns for col in required_columns):
        # Streamlit Dashboard
        st.title("IIoT Machine Monitoring Dashboard")

        # Line Chart: Temperature over Time
        st.subheader("Temperature Over Time")
        st.line_chart(df.set_index('timestamp')['temperature'])

        # Bar Chart: Voltage by Machine ID
        st.subheader("Voltage by Machine ID")
        st.bar_chart(df[['machine_id', 'voltage']].set_index('machine_id'))

    else:
        st.error(f"Required columns are missing in the data. Please check the MongoDB collection.")
        st.write("Available columns:", df.columns.tolist())