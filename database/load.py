import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import yaml
import psycopg2
import sys
sys.path.insert(1, '../scrape')
sys.path.insert(1, '../transform')
import match_stats_scraper
import transform_match_data

def read_one_block_of_yaml_data():
    with open('../config/db_config.yml','r') as f:
        return yaml.safe_load(f)

#match_stats_scraper.scraper()

match_data_dict = transform_match_data.transform_match_data()

 
    
db_config = read_one_block_of_yaml_data()
postgres_config = db_config['postgres_config']

spark = SparkSession.builder \
    .appName("pyspark_test") \
    .config("spark.jars", './postgresql-42.7.5.jar') \
    .getOrCreate()



conn = psycopg2.connect(
    host=db_config['db_host'],
    port=db_config['db_port'],
    dbname=db_config['db_name'],
    user=db_config['db_user'],
    password=db_config['db_password']
)

def check_if_match_exisits(match_data_dict):
    query = "SELECT \"testImportTableId\", event, \"eventDate\", stage, round, \"teamOneName\", \"teamTwoName\" FROM test_import.test_import_table;"
    df_pandas = pd.read_sql_query(query, conn)

df_spark = spark.createDataFrame(df_pandas)

# Print the schema of the DataFrame
df_spark.printSchema()

# Show the first few rows
df_spark.show()



