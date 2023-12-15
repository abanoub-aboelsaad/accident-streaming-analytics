import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql

logging.basicConfig(level=logging.INFO)

def insert_into_phpmyadmin(row, table_name):
    try:
        # Connection details
        host = "localhost"
        port = 3306
        database = "big_data"
        username = "root"
        password = ""

        # Connect to MySQL
        conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
        cursor = conn.cursor()

       
        # column1_value = row.name
        column2_value = row.Time
        column3_value = row.Day_of_week
        column4_value = row.Age_of_driver
        column5_value = row.Sex_of_driver
        column6_value = row.Type_of_vehicle
        column7_value = row.Area_accident_occured
        column8_value = row.Road_surface_type
        column9_value = row.Road_surface_conditions
        column10_value = row.Light_conditions
        column11_value = row.Weather_conditions
        column12_value = row.Number_of_casualties
        column13_value = row.Cause_of_accident
        column14_value = row.Accident_severity

      

#         # the SQL query based on the table name
        if table_name == "big_data":
            sql_query = f"INSERT INTO big_data ( Time, Day_of_week, Age_of_driver, Sex_of_driver, Type_of_vehicle, Area_accident_occured, Road_surface_type, Road_surface_conditions, Light_conditions, Weather_conditions, Number_of_casualties, Cause_of_accident, Accident_severity) VALUES ('{column2_value}', '{column3_value}', '{column4_value}', '{column5_value}', '{column6_value}', '{column7_value}', '{column8_value}', '{column9_value}', '{column10_value}', '{column11_value}', '{column12_value}', '{column13_value}', '{column14_value}')"
        elif table_name == "filtered_data_table":
            sql_query = f"INSERT INTO filtered_data_table (Weather_conditions, Number_of_casualties, Cause_of_accident, Accident_severity) VALUES ('{column11_value}', '{column12_value}', '{column13_value}', '{column14_value}')"


        # elif table_name == "day_stat":
        #     #  column15_value = row.accident_count_by_day
        #      sql_query = f"INSERT INTO day_stat (Day_of_week, accident_count_by_day) VALUES ('{column3_value}','{5}' )"   
            

        else:
            raise ValueError(f"Unknown table name: {table_name}")

        cursor.execute(sql_query)

        conn.commit()
        logging.info(f"Inserted row into {table_name}: {row}")

    except pymysql.IntegrityError as e:
    
        logging.warning(f"Duplicate key error: {str(e)}")
      
    except Exception as e:
        logging.error(f"Error inserting into MySQL: {str(e)}")

    finally:
        # Close the database connection in the finally block
        if conn:
            conn.close()

def insert_day_stat_into_phpmyadmin(rows, database_name):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    # database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database_name)
    cursor = conn.cursor()

    for row in rows:
        column1_value = row["Day_of_week"]
        column2_value_agg = row["accident_count_by_day"]
        print(f"Inserting: Day_of_week={column1_value}, accident_count_by_day={column2_value_agg}")
        sql_query1 = f"INSERT IGNORE INTO day_stat (Day_of_week, accident_count_by_day) VALUES ('{column1_value}', {column2_value_agg})"
        cursor.execute(sql_query1)

    conn.commit()
    conn.close()

def insert_gender_stat_into_phpmyadmin(rows, database_name):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    # database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database_name)
    cursor = conn.cursor()

    for row in rows:
        column1_value = row["Sex_of_driver"]
        column2_value = row["accident_count_by_Sex_of_driver"]
        print(f"Inserting: Sex_of_driver={column1_value}, accident_count_by_Sex_of_driver={column2_value}")
        sql_query_sex = f"INSERT IGNORE INTO gender_stat (Sex_of_driver, accident_count_by_Sex_of_driver) VALUES ('{column1_value}', {column2_value})"
        cursor.execute(sql_query_sex)

    conn.commit()
    conn.close()


def insert_vehicle_stat_into_phpmyadmin(rows, database_name):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    for row in rows:
        column1_value = row["Type_of_vehicle"]
        column2_value = row["accident_count_by_Type_of_vehicle"]
        print(f"Inserting: Type_of_vehicle={column1_value}, accident_count_by_Type_of_vehicle={column2_value}")
        sql_query_vehicle = f"INSERT IGNORE INTO Type_of_vehicle_stat (Type_of_vehicle, accident_count_by_Type_of_vehicle) VALUES ('{column1_value}', {column2_value})"
        cursor.execute(sql_query_vehicle)

    conn.commit()
    conn.close()



# Create a Spark session
# create app that read and excute the sql
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


schema = StructType().add("ID", IntegerType()).add("Time", StringType()).add("Day_of_week", StringType()).add("Age_of_driver", StringType()).add("Sex_of_driver", StringType()).add("Type_of_vehicle", StringType()).add("Area_accident_occured", StringType()).add("Road_surface_type", StringType()).add("Road_surface_conditions", StringType()).add("Light_conditions", StringType()).add("Weather_conditions", StringType()).add("Number_of_casualties", IntegerType()).add("Cause_of_accident", StringType()).add("Accident_severity", StringType())


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \


df = df.select(
    "data.Time", "data.Day_of_week", "data.Age_of_driver", 
    "data.Sex_of_driver", "data.Type_of_vehicle", 
    "data.Area_accident_occured", "data.Road_surface_type", 
    "data.Road_surface_conditions", "data.Light_conditions", 
    "data.Weather_conditions", "data.Number_of_casualties", 
    "data.Cause_of_accident", "data.Accident_severity"
)



query_big_data = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(lambda row: insert_into_phpmyadmin(row, "big_data")) \
    .start()

filtered_df = df.filter(col("Accident_severity") == "Slight Injury")


query_filtered_data = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(lambda row: insert_into_phpmyadmin(row, "filtered_data_table")) \
    .start()


df_aggregation = df.groupBy("Day_of_week").agg(count("*").alias("accident_count_by_day"))

df_aggregation_gender = df.groupBy("Sex_of_driver").agg(count("*").alias("accident_count_by_Sex_of_driver"))


df_aggregation_vehicle = df.groupBy("Type_of_vehicle").agg(count("*").alias("accident_count_by_Type_of_vehicle"))


def process_row(batch_df, epoch_id):
    database_name = "big_data"  
    rows = batch_df.collect()
    insert_day_stat_into_phpmyadmin(rows, database_name)

def process_row_gender(batch_df, epoch_id):
    database_name = "big_data" 
    rows = batch_df.collect()
    insert_gender_stat_into_phpmyadmin(rows, database_name)

def process_row_vehicle(batch_df, epoch_id):
    database_name = "big_data"  
    rows = batch_df.collect()
    insert_vehicle_stat_into_phpmyadmin(rows, database_name)


query = df_aggregation.writeStream \
    .foreachBatch(process_row) \
    .outputMode("complete") \
    .start()


query_gender = df_aggregation_gender.writeStream \
    .foreachBatch(process_row_gender) \
    .outputMode("complete") \
    .start()


query_vehicle = df_aggregation_vehicle.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_row_vehicle) \
    .start()


# Wait for the queries to finish
query.awaitTermination()
query_vehicle.awaitTermination()
query_filtered_data.awaitTermination()
query_big_data.awaitTermination()
query_gender.awaitTermination()



