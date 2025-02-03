from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("IIoT Data Processing").getOrCreate()

# Load Data from MongoDB
sensor_df = spark.read.format("mongo").option("uri", "mongodb://localhost:27017/iiot_database.sensor_data").load()

# Data Processing: Filter anomalies
processed_df = sensor_df.filter(col("temperature") > 400)
processed_df.show()
