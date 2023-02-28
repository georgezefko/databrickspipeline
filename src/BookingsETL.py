# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import col, isnan, count,round
from datetime import datetime
import pandas as pd
import logging
from utils import DataPreprocessor
# create logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create console handler and set level to INFO
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to console handler
ch.setFormatter(formatter)

# add console handler to logger
logger.addHandler(ch)

# COMMAND ----------

blob_account_name = 'experimentaldata'
blob_container_name = 'bookings'
blob_access_key = dbutils.secrets.get(scope="intellishore",key="storage")

# COMMAND ----------

# Define the path to the mounted directory
path = "/mnt/myblob/"

# Get a list of all files in the mounted directory
files = dbutils.fs.ls(path)

# Filter the list to include only JSON files
json_files = [f for f in files if f.name.endswith(".json")]

# Sort the list by the modification time of each file, in descending order
sorted_files = sorted(json_files, key=lambda f: f.modificationTime, reverse=True)

# Get the path to the latest modified file
latest_file_path = sorted_files[0].path

# Read the data from the latest modified file into a DataFrame
df = spark.read.option("inferSchema", "true").option("multiline", "true").json(latest_file_path)

# COMMAND ----------

# Un-nest the json format
df = df.select('root.*')
df = df.withColumn("page", explode(df["page"]))
df = df.select("page.*")
df = df.select('pageurl',"record.*")
df = df.select("pageurl", "uniq_id", "hotel_id", "hotel_name", "review_count", "rating_count", "default_rank", "price_rank", "ota", "checkin_date", "crawled_date", explode("room_type").alias("room_type"))
df = df.select("pageurl", "uniq_id", "hotel_id", "hotel_name", "review_count", "rating_count", "default_rank", "price_rank", "ota", "checkin_date", "crawled_date", "room_type.*")


# COMMAND ----------

# Correct the data types
df_silver = df.withColumn('hotel_id', df.hotel_id.cast('int')) \
                  .withColumn('review_count', df.review_count.cast('float')) \
                  .withColumn('rating_count', df.rating_count.cast('float')) \
                  .withColumn('default_rank', df.default_rank.cast('int')) \
                  .withColumn('price_rank', df.price_rank.cast('int')) \
                  .withColumn('checkin_date', to_date(col('checkin_date'), 'yyyy-MM-dd')) \
                  .withColumn("crawled_date", to_timestamp(col("crawled_date"),"yyyy-MM-dd HH:mm:ss Z")) \
                  .withColumn('room_type_price', df.room_type_price.cast('float'))

# COMMAND ----------

# Initialize utils class
preprocessor = DataPreprocessor(df_silver,threshold=10)

# COMMAND ----------

#Data checks
preprocessor.count_rows(df_silver)
preprocessor.num_columns(df_silver,16)

# COMMAND ----------

# Check for nulls
above,below = preprocessor.get_nulls(df_silver,threshold=10)
# Get null report
preprocessor.null_status(10,above,below)

# COMMAND ----------

# check for nulls and remove columns with more than 10% null values
no_nulls = preprocessor.remove_nulls(df_silver,above,rows=False)

# COMMAND ----------

#Data checks
preprocessor.count_rows(no_nulls)
no_columns = len(df_silver.columns)-len(above)
preprocessor.num_columns(no_nulls,no_columns)

# COMMAND ----------

# Remove the rows with null values
no_nulls_rows = preprocessor.remove_nulls(no_nulls,below,rows=True)

# COMMAND ----------

# Remove duplicates
no_duplicates = preprocessor.check_duplicates(no_nulls_rows)

# COMMAND ----------

# Last check for remaining nulls
last_above,last_below = preprocessor.get_nulls(no_duplicates,0)
preprocessor.null_status(0,last_above,last_below)

# COMMAND ----------

# Write the data to silver data base
no_duplicates.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/user/hive/warehouse/silver/bookings_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- what was the total income of hotels based on bookings they had in 2019
# MAGIC WITH rev AS (
# MAGIC   SELECT 
# MAGIC       hotel_id as id,
# MAGIC       hotel_name as name,
# MAGIC       round(SUM(room_type_price),0) AS revenue
# MAGIC   FROM silver.bookings_silver
# MAGIC   GROUP BY 1,2
# MAGIC   ORDER BY SUM(room_type_price) DESC)
# MAGIC   INSERT INTO gold.revenue(hotel_id,hotel_name,revenue)
# MAGIC   SELECT id,name,revenue FROM rev
# MAGIC   WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold.revenue
# MAGIC   WHERE hotel_id = id AND hotel_name = name
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Quarter AS(
# MAGIC         SELECT
# MAGIC         hotel_id as Id,
# MAGIC         hotel_name as Name,
# MAGIC         count(*) AS Bookings,
# MAGIC         
# MAGIC         date_format(checkin_date, 'MM') AS Month,
# MAGIC         
# MAGIC         CASE 
# MAGIC             WHEN date_format(checkin_date, 'MM') BETWEEN '01' AND '03' THEN 'Q1'
# MAGIC             WHEN date_format(checkin_date, 'MM') BETWEEN '03' AND '06' THEN 'Q2'
# MAGIC             WHEN date_format(checkin_date, 'MM') BETWEEN '06' AND '09' THEN 'Q3' 
# MAGIC             ELSE 'Q4'
# MAGIC         END AS Quarter 
# MAGIC         FROM silver.bookings_silver
# MAGIC         GROUP BY 1,2,4,5
# MAGIC )
# MAGIC  INSERT INTO gold.month_quarter_demand(hotel_id,hotel_name,bookings,month,quarter,month_quarter,demand)
# MAGIC   SELECT 
# MAGIC     Id,
# MAGIC     Name,
# MAGIC     Bookings,
# MAGIC     Month,
# MAGIC     Quarter,
# MAGIC     CONCAT(Month, '_', Quarter), 
# MAGIC     Bookings - COALESCE(lag(Bookings) OVER (PARTITION BY Id ORDER BY Month, Quarter), 0) AS Difference
# MAGIC FROM Quarter
# MAGIC   WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM gold.revenue
# MAGIC   WHERE hotel_id = id AND hotel_name = name
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH rooms AS (SELECT 
# MAGIC   b.hotel_id AS id, 
# MAGIC   b.hotel_name, 
# MAGIC   b.room_type_breakfast AS breakfast, 
# MAGIC   b.room_type_cancellation AS cancellation, 
# MAGIC   b.room_type_name AS room_name,
# MAGIC   round(AVG(b.room_type_price),0) AS avg_price,
# MAGIC   round(AVG(b.room_type_occupancy),0) AS avg_num_persons,
# MAGIC   COUNT(b.crawled_date) AS bookings,
# MAGIC   a.avg_days_diff
# MAGIC FROM 
# MAGIC   silver.bookings_silver b
# MAGIC   INNER JOIN (
# MAGIC     SELECT 
# MAGIC       hotel_id, 
# MAGIC       hotel_name, 
# MAGIC       room_type_breakfast, 
# MAGIC       room_type_cancellation, 
# MAGIC       room_type_name,
# MAGIC       ROUND(AVG(DATEDIFF(checkin_date,CAST(crawled_date AS DATE))),0) AS avg_days_diff
# MAGIC     FROM 
# MAGIC       silver.bookings_silver  
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC   ) a ON 
# MAGIC     b.hotel_id = a.hotel_id AND 
# MAGIC     b.hotel_name = a.hotel_name AND 
# MAGIC     b.room_type_breakfast = a.room_type_breakfast AND 
# MAGIC     b.room_type_cancellation = a.room_type_cancellation AND 
# MAGIC     b.room_type_name = a.room_type_name
# MAGIC GROUP BY 
# MAGIC   1,2,3,4,5,9
# MAGIC   )
# MAGIC INSERT INTO gold.rooms_overview(hotel_id, hotel_name, breakfast, cancelation, room_name, avg_price,avg_num_persons, bookings, avg_days_dif)
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   hotel_name,
# MAGIC   breakfast,
# MAGIC   cancellation,
# MAGIC   room_name,
# MAGIC   avg_price,
# MAGIC   avg_num_persons,
# MAGIC   bookings,
# MAGIC   avg_days_diff
# MAGIC FROM rooms
# MAGIC WHERE NOT EXISTS (
# MAGIC SELECT 1 FROM gold.rooms_overview
# MAGIC WHERE hotel_id = id AND hotel_name = hotel_name
# MAGIC )

# COMMAND ----------

database_name = "gold"
logger.info(f'Quality check on {database_name} tables')
tables = spark.catalog.listTables(database_name)
# print the table names
for table in tables:
    
    logger.info(f'Quality check on {table.name} table')
    
    # read the table into a DataFrame
    df = spark.table(f'{database_name}.{table.name}')
    
    # Get null report
    m_above,m_below = preprocessor.get_nulls(df,threshold=0)
    preprocessor.null_status(0,m_above,m_below)
    
    #check and remove duplicates
    m_duplicates = preprocessor.check_duplicates(df)

# COMMAND ----------


