import pandas as pd
import datetime as dt
import configparser
import os
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


def generate_immigration_by_city_data(spark, input_path):

    # load all necessary tables
    # get filepath to parquet data file
    input_data_path = os.path.join(
        input_path, 'immigration/immigration.parquet/arrival_year=2016/*/*.parquet')
    df_final_immigration = spark.read.parquet(input_data_path)
    df_final_immigration.createOrReplaceTempView("tbl_immigration_data")

    df_final_city_airport_info = spark.read.parquet(
        input_path + "city_airport_codes.parquet/*.parquet")
    df_final_city_airport_info.createOrReplaceTempView("tbl_city_airport_info")

    df_final_us_city_demographics = spark.read.parquet(
        input_path + "us_city_demographics.parquet/*.parquet")
    df_final_us_city_demographics.createOrReplaceTempView(
        "tbl_us_city_demographics")

    input_data_path = os.path.join(input_path, 'time_data.parquet/*.parquet')
    df_time = spark.read.parquet(input_data_path)
    df_time.createOrReplaceTempView("tbl_time")

    # prepare final data
    df_immigration_by_city = spark.sql("""SELECT 
                                                CAST(imm.cicid AS int) AS cicid,
                                                imm.country_of_origin,
                                                cai.iata_code AS arrival_airport_code,
                                                cai.city AS arrival_city_name,
                                                cai.state_code AS arrival_state_code,
                                                imm.arrival_date,  
                                                dt.month AS arrival_month,
                                                dt.year AS arrival_year,
                                                imm.visatype AS arrival_visa_type,             
                                                imm.gender,   
                                                CAST(imm.birth_year AS int) AS birth_year,
                                                cdem.total_population AS destination_city_population,
                                                cdem.median_age AS destination_median_age
                                         FROM tbl_immigration_data imm
                                         JOIN tbl_city_airport_info cai
                                           ON LOWER(imm.i94port_city) = LOWER(cai.city)
                                          AND imm.i94port_statecode = trim(cai.state_code)
                                         JOIN tbl_us_city_demographics cdem
                                           ON imm.i94port_statecode = cdem.state_code
                                          AND LOWER(imm.i94port_city) = LOWER(cdem.city)                                         
                                         JOIN tbl_time dt
                                           ON imm.arrival_date = dt.date_value
                                     """).dropDuplicates()
    df_immigration_by_city.dropna()

    print("displaying sample data for immigration by city")
    df_immigration_by_city.show(5)

    row_count_immigration_by_city = df_immigration_by_city.count()
    print("final analytical row count is: "+str(row_count_immigration_by_city))


def main():
    """
    Create Spark Session and generate sample analytics on imigration data

    - Call create_spark_session() to create Spark Session for use in other function
    - call function to generate analytics
    - Finally, stop the Spark session    
    """

    print("*** Start of sample data analytics for immigration ***")

    spark = create_spark_session()
    input_path = "outputdata/"
    generate_immigration_by_city_data(spark, input_path)
    print("*** End of sample data analytics for immigration ***")
    spark.stop()


if __name__ == "__main__":
    main()
