import pandas as pd
import datetime as dt
import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession

config = configparser.ConfigParser()


def create_spark_session():
    """
    Create SparkSession and Config to use in ETL as below:

    - Config spark.jars.repositories to https://repos.spark-packages.org/
    - Config spark.jars.packages to org.apache.hadoop:hadoop-aws:2.7.3    
    """

    spark = SparkSession \
        .builder \
        .appName("snayakwadi_capstone_immigration_data") \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def check_immigration_data(spark, input_files_path):
    # get filepath to parquet data file
    input_data_path = os.path.join(
        input_files_path, 'immigration/immigration.parquet/arrival_year=2016/*/*.parquet')
    df_final_immigration = spark.read.parquet(input_data_path)
    df_final_immigration.createOrReplaceTempView("tbl_immigration_data")

    # Count records of tbl_immigration_data table for data quality checks
    count_tblImmigration = df_final_immigration.count()
    print('The data in "{}" table have {} record(s)'.format(
        'tbl_immigration_data', count_tblImmigration))

    # NULL check  for immigration data
    immigration_null_check = spark.sql("""
                                        SELECT * FROM tbl_immigration_data
                                        WHERE cicid IS NULL
                                        """)

    immigration_null_count = immigration_null_check.count()

    if(immigration_null_count > 1):
        print("Null values found in tbl_immigration_data , please check")
        print(
            "No of records with [cicid] as NULL Values:", immigration_null_count)


def check_city_data(spark, input_files_path):
    df_final_city_airport_info = spark.read.parquet(
        input_files_path + "city_airport_codes.parquet/*.parquet")
    df_final_city_airport_info.createOrReplaceTempView("tbl_city_airport_info")

    # Count records of df_final_city_airport_info for data quality checks
    count_tblcity_airport_info = df_final_city_airport_info.count()
    print('The data in "{}" table have {} record(s)'.format(
        'df_final_city_airport_info', count_tblcity_airport_info))

    # NULL check for airport_codes
    airport_codes_null_check = spark.sql("""
                                        SELECT * FROM tbl_city_airport_info
                                        WHERE iata_code IS NULL
                                        """)
    if(airport_codes_null_check.count() > 1):
        print("Null values found in tbl_city_airport_info data, please check")
        print(
            "No of records with [iata_code] as NULL Values:", airport_codes_null_check)


def check_US_demographics(spark, input_files_path):
    df_final_us_city_demographics = spark.read.parquet(
        input_files_path + "us_city_demographics.parquet/*.parquet")
    df_final_us_city_demographics.createOrReplaceTempView(
        "tbl_us_city_demographics")

    # Count records of df_final_us_city_demographics for data quality checks
    count_us_city_demographics = df_final_us_city_demographics.count()
    print('The data in "{}" table have {} record(s)'.format(
        'df_final_us_city_demographics', count_us_city_demographics))

    # NULL check for us_demographics data
    us_cities_demo_check = spark.sql(
        """SELECT * FROM tbl_us_city_demographics WHERE city IS NULL OR state_code IS NULL""")
    us_cities_demo_null_count = us_cities_demo_check.count()
    if(us_cities_demo_null_count > 1):
        print("NULL data found for us_city_demographics, please check")
        print(
            "No of records with city OR state_code is NULL", us_cities_demo_null_count)


def check_time_data(spark, input_files_path):
    input_data_path = os.path.join(
        input_files_path, 'time_data.parquet/*.parquet')
    df_time = spark.read.parquet(input_data_path)
    df_time.createOrReplaceTempView("tbl_time")

    # Count records of df_time for data quality checks
    count_df_time = df_time.count()
    print('The data in "{}" table have {} record(s)'.format(
        'df_time', count_df_time))

    # NULL check for time data
    time_null_check = spark.sql("""
                                SELECT * FROM tbl_time WHERE date_value IS NULL
                                """)

    time_null_count = time_null_check.count()
    if(time_null_count > 1):
        print("NULL data found for us_city_demographics, please check")
        print("No of records with date_value is NULL", time_null_count)


def main():
    """
    Create Spark Session and provide input and output locations for functions as needed 

    - Call create_spark_session() to create Spark Session for use in other function
    - assign input and output file paths
    - Call check_immigration_data() function to perform data quality checks for tbl_immigration
    - Call check_city_data() function to perform data quality checks for tbl_city_airport_info
    - Call check_US_demographics() function to perform data quality checks for tbl_us_city_demographics
    - Call check_time_data() function to perform data quality checks for tbl_datetime
    - Finally, stop the Spark session

    """

    print("*** Start data quality check process***")
    spark = create_spark_session()
    input_files_path = "outputdata/"

    print("*** Start data quality check for immigration data ***")
    check_immigration_data(spark, input_files_path)
    print("*** End data quality check for immigration data ***")

    print("*** Start data quality check for city_airport data ***")
    check_city_data(spark, input_files_path)
    print("*** End data quality check for city_airport data ***")

    print("*** Start data quality check for US city demographics data ***")
    check_US_demographics(spark, input_files_path)
    print("*** End data quality check for city demographics data ***")

    print("*** Start data quality check for time data ***")
    check_time_data(spark, input_files_path)
    print("*** End data quality check for time data ***")

    spark.stop()


if __name__ == "__main__":
    main()
