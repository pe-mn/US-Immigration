import configparser
import os

from time import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.functions import udf, col, desc, substring
# from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark Session with the hadoop-aws package
    which is used for us to connect with Amazon S3 
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"). \
    enableHiveSupport().getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """
    Args:
        spark: spark session
        input_data: Path to input data
        output_data: Path to output data
    Returns:
        Outputs the fact immigration table and the date dimention table to S3
    """
    # get filepath to immigration data file
    path = input_data + "sas_data" # sas_data for local mode
    
    # read data file
    df=spark.read.parquet(path) # local mode
    
#     df = spark.read.format('com.github.saurfang.sas.spark')\
#     .load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat') # cloud mode


    # Convert Dates (sas / string) to DateTime
    df = df.withColumn('arrdate', F.expr("date_add('1960-01-01', cast(arrdate as int))")) 
    df = df.withColumn('depdate', F.expr("date_add('1960-01-01', cast(depdate as int))")) 
    df = df.withColumn('dtaddto', F.to_date("dtaddto" ,"MMddyyyy"))

    
    # Add Visa Categories (Business - Pleasure - Student)
    sql_expr = """
        CASE WHEN i94visa = 1.0 THEN 'Business' 
             WHEN i94visa = 2.0 THEN 'Pleasure'
             WHEN i94visa = 3.0 THEN 'Student'
             ELSE 'N/A' 
        END              
        """
    df = df.withColumn('i94visa', F.expr(sql_expr))
    
    
    # Add travel modes
    sql_expr = """
        CASE  WHEN i94mode = 1.0 THEN 'Air' 
              WHEN i94mode = 2.0 THEN 'Sea'
              WHEN i94mode = 3.0 THEN 'Land'
              WHEN i94mode = 9.0 THEN 'Not Reported'
              ELSE 'N/A'  
        END              
        """
    df = df.withColumn('i94mode', F.expr(sql_expr))    
    

#     # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them    
#     drop_columns = ['occup', 'entdepu','insnum']
#     df = df.drop(columns=drop_columns)

#     # drop rows where all elements are missing
#     df = df.dropna(how='all')
    
    
    df.createOrReplaceTempView('immigration')

    # extract columns to create immigration table
    fact_immigration = spark.sql("""
        SELECT
            cicid    AS cicid,
            i94yr    AS arrival_year,
            i94mon   AS arrival_month,
            i94cit   AS citizinship,
            i94res   AS residence,
            i94port  AS port,
            arrdate  AS arrival_date,
            i94mode  AS travel_mode,
            i94addr  AS us_state,
            depdate  AS departure_date,
            i94bir   AS age,
            i94visa  AS visa_category,
            visapost AS dep_issued_visa,
            dtaddto  AS visa_expiration_date,
            gender   AS gender,
            airline  AS airline,
            admnum   AS admission_number,
            fltno    AS flight_number,
            visatype AS visa_type
        FROM immigration 
    """) 
#             insnum   AS ins_number,    
#             occup    AS occupation,
#             entdepa  AS arrival_flag,
#             entdepd  AS departure_flag,
#             entdepu  AS update_flag,
#             matflag  AS match_flag,
    
    # write table to parquet files 
    fact_immigration.write\
    .partitionBy("us_state")\
    .mode('overwrite')\
    .parquet(os.path.join(output_data, 'immigration'))
    
    
# -------------------------------------------------------------------------------------------------------------------------    
    
    # create initial date df from arrdate column
    date_df = df.select(['arrdate']).distinct()

    # expand df by adding other date columns
    date_df = date_df.withColumn('arrival_day', F.dayofmonth('arrdate'))
    date_df = date_df.withColumn('arrival_week', F.weekofyear('arrdate'))
    date_df = date_df.withColumn('arrival_month', F.month('arrdate'))
    date_df = date_df.withColumn('arrival_year', F.year('arrdate'))
    date_df = date_df.withColumn('arrival_weekday', F.dayofweek('arrdate'))

#     # create an id field in calendar df
#     date_df = date_df.withColumn('id', monotonically_increasing_id())

    # write the date dimension to parquet file
    date_df.write.parquet(output_data + "arrival_dates", mode="overwrite")


# # ----------------------------------------------------------------------------
#     # extract columns to create immigration table
#     dim_immigrant = spark.sql("""
#         SELECT
#             cicid    AS id,
#             i94cit   AS citizinship,
#             i94res   AS residence,
#             i94addr  AS us_state,
#             i94bir   AS age,
#             occup    AS occupation,
#             gender   AS gender,
#             insnum   AS ins_number,
#             admnum   AS admission_number,
#         FROM immigration 
#     """) 
    
#     # write table to parquet files 
#     dim_immigrant.write\
#     .partitionBy("us_state", "citizinship")\
#     .mode('overwrite')\
#     .parquet(os.path.join(output_data, 'immigrant'))
# # ----------------------------------------------------------------------------
#     # extract columns to create immigration table
#     dim_date = spark.sql("""
#         SELECT
#             cicid    AS id,
#             i94yr    AS arrival_year,
#             i94mon   AS arrival_month,
#             arrdate  AS arrival_date,
#             depdate  AS departure_date,
#             dtaddto  AS visa_expiration_date,
#         FROM immigration 
#     """) 
    
#     # write table to parquet files 
#     dim_date.write\
#     .partitionBy("arrival_year", "arrival_month")\
#     .mode('overwrite')\
#     .parquet(os.path.join(output_data, 'dates'))
# # ----------------------------------------------------------------------------    
#     # extract columns to create immigration table
#     dim_port = spark.sql("""
#         SELECT
#             cicid    AS id,
#             i94port  AS port,
#             i94mode  AS travel_mode,   
#             airline  AS airline,
#             fltno    AS flight_number,
#         FROM immigration 
#     """) 
    
#     # write table to parquet files 
#     dim_port.write\
#     .partitionBy("travel_mode")\
#     .mode('overwrite')\
#     .parquet(os.path.join(output_data, 'port'))
# # ----------------------------------------------------------------------------    
#     # extract columns to create immigration table
#     dim_visa = spark.sql("""
#         SELECT
#             cicid    AS id,   
#             i94visa  AS visa_category,
#             visapost AS dep_issued_visa,
#             visatype AS visa_type
#         FROM immigration 
#     """) 
    
#     # write table to parquet files 
#     dim_visa.write\
#     .partitionBy("us_state")\
#     .mode('overwrite')\
#     .parquet(os.path.join(output_data, 'visa'))
    
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
 
    
def process_demographics_data(spark, input_data, output_data):
    """
    Args:
        spark: spark session
        input_data: Path to input data
        output_data: Path to output data
    Returns:
        Outputs the demographics table into S3
    """  
    # get filepath to immigration data file
    path = input_data + "us-cities-demographics.csv" 
    
    # read data file
    df = spark.read.csv(path, inferSchema=True, header=True, sep=';') 
    
    # Rename Columns
    df = df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'n_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'avg_household_size') \
        .withColumnRenamed('State Code', 'state_code')
    
#     # lets add an id column
#     df = df.withColumn('id', monotonically_increasing_id())
    
    
    df.write.parquet(output_data + "demographics", mode="overwrite")
    
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

    
def main():
    spark = create_spark_session()
#     input_data = "s3a://udacity-dend/"
#     output_data = "./Results/"
    input_data, output_data = './input_data/', './output_data/'  # Uncomment for local mode
    
    process_immigration_data(spark, input_data, output_data)    
    process_demographics_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
