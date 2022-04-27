import datetime as dt
import configparser
import os
from datetime import datetime

config = configparser.ConfigParser()


def create_spark_session():
    """
    Create SparkSession and Config to use in downloading sas immigration data:

    - Config spark.jars.repositories to https://repos.spark-packages.org/
    - Config spark.jars.packages to org.apache.hadoop:hadoop-aws:2.7.3
    - Config spark.jars.packages to saurfang:spark-sas7bdat:2.0.0-s_2.11

    """

    spark = SparkSession \
        .builder \
        .appName("snayakwadi_capstone_immigration_data") \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def download_immigration_data(spark, sas_input_path, sas_output_path):
    sas_immigration_data_path = sas_input_path
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
              'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for month in months:
        sas_file_name = "{}{}{}{}".format(
            sas_immigration_data_path, "i94_", month, "16_sub.sas7bdat")
        sas_git_path = "com.github.saurfang.sas.spark"

        df_spark_raw_sas_data = spark.read.format(
            sas_git_path).load(sas_file_name)
        output_file_path = sas_output_path + month+"2016/"
        df_spark_raw_sas_data.write.mode("overwrite").parquet(output_file_path)


def main():
    """
    Create Spark Session and initiate downloada sas data function. 

    - Call create_spark_session() to create Spark Session for use in other function
    - Prepare input_data and output_data location
    - Finally, stop the Spark session

    """

    print("*** Create spark session and start downloading immigration data ***")
    spark = create_spark_session()
    sas_input_path = "../../data/18-83510-I94-Data-2016/"
    sas_output_path = "sas_parquet_data"
    download_immigration_data(spark, sas_input_path, sas_output_path)
    print('parquet files saved to sas_output_path')
    spark.stop()


if __name__ == "__main__":
    main()
