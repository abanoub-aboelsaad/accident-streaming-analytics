from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("ID", IntegerType()).add("Time", StringType()).add("Day_of_week", StringType()).add("Age_of_driver", StringType()).add("Sex_of_driver", StringType()).add("Type_of_vehicle", StringType()).add("Area_accident_occured", StringType()).add("Road_surface_type", StringType()).add("Road_surface_conditions", StringType()).add("Light_conditions", StringType()).add("Weather_conditions", StringType()).add("Number_of_casualties", IntegerType()).add("Cause_of_accident", StringType()).add("Accident_severity", StringType())
# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "C:/Users/Abanoub/Desktop/bigdata/project/Section 7/project14/data") \
    .load() \

df = streaming_df.select(to_json(struct("ID", "Time", "Day_of_week", "Age_of_driver", "Sex_of_driver", "Type_of_vehicle", "Area_accident_occured", "Road_surface_type", "Road_surface_conditions", "Light_conditions", "Weather_conditions", "Number_of_casualties", "Cause_of_accident", "Accident_severity")).alias("value"))



# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation","null") \
    .start()

# Wait for the query to finish
query.awaitTermination()