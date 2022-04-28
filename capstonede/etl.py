import pandas as pd
import datetime as dt
import configparser
import os
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.functions import min, max
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


def process_immigration_data(spark, input_path_immigration_data, output_path):
    """
    - Read immigration sas data files, apply cleansing and generate final data
    - Read data files extracted out of various sas file section and generate final data  
    """
    # get filepath to raw immigration data files
    input_parquet_files_path = os.path.join(input_path_immigration_data, '*/*.parquet')
    #input_parquet_files_path = input_path_immigration_data +'apr2016/*.parquet'
    print("input path is", input_parquet_files_path)
    df_staging_immigration_data = spark.read.parquet(input_parquet_files_path)
    
    initial_count = df_staging_immigration_data.count()
    print("initial row count for immigration data is", str(initial_count))
    
    #udf_datetime = udf(lambda x: convert_double_to_datetime(x), T.DateType())
    udf_getdate = udf(lambda x: (dt.datetime(1960, 1, 1).date(
    ) + dt.timedelta(float(x))).isoformat() if x else None)
    df_staging_immigration_data = df_staging_immigration_data.withColumn('arrival_date', udf_getdate(
        df_staging_immigration_data.arrdate)).withColumn('departure_date', udf_getdate(df_staging_immigration_data.depdate))

    # SAS file section I94ADDR contains state code
    df_staging_I94_state_code = spark.read.option("header", True).options(
        delimiter=',').csv("datafiles/I94_state_code_name.csv")

    # SAS file section I94CIT_I94RES contains country code and country name info
    df_staging_I94_country_code = spark.read.option("header", True).options(
        delimiter=',').csv("datafiles/I94_country_code_name.csv")

    # SAS file section I94MODE contains  I94 mode type info
    df_staging_I94_mode = spark.read.option("header", True).options(
        delimiter=',').csv("datafiles/I94_mode.csv")

    # SAS file section I94PORT contains port and city info
    df_staging_I94_port_city = spark.read.option("header", True).options(
        delimiter=',').csv("datafiles/I94_port_city.csv")

    # SAS file section I94VISA contains visa type info
    df_staging_I94_visa_type = spark.read.option("header", True).options(
        delimiter=',').csv("datafiles/I94_visa_type.csv")

    df_staging_immigration_data.createOrReplaceTempView(
        "tbl_stg_immigration_data")
    df_staging_I94_state_code.createOrReplaceTempView("tbl_I94ADDR_Mapping")
    df_staging_I94_country_code.createOrReplaceTempView(
        "tbl_I94CIT_I94RES_Mapping")
    df_staging_I94_mode.createOrReplaceTempView("tbl_I94MODE_Mapping")
    df_staging_I94_port_city.createOrReplaceTempView("tbl_I94PORT_Mapping")
    df_staging_I94_visa_type.createOrReplaceTempView("tbl_I94VISA_Mapping")
    tbl_immigration = spark.sql("""
                                        SELECT DISTINCT IMM.cicid
                                                       ,IMM.i94yr
                                                       ,IMM.i94mon
                                                       ,I94CIT_I94RES1.country_name as country_of_origin
                                                       ,trim(IMM.i94port) as i94port
                                                       ,initcap(lower(trim(SPLIT(I94PORT.city_province,',')[0]))) as i94port_city
                                                       ,I94PORT.state_code as i94port_statecode
                                                       ,arrival_date
                                                       ,I94MODE.mode_name  as i94mode
                                                       ,i94addr as state_code
                                                       ,departure_date
                                                       ,i94bir as age
                                                       ,I94VISA.visa_type as i94visa
                                                       ,biryear as birth_year
                                                       ,gender
                                                       ,airline
                                                       ,fltno   
                                        FROM tbl_stg_immigration_data AS IMM 
                                        LEFT JOIN tbl_I94ADDR_Mapping AS I94ADDR on IMM.i94addr = I94ADDR.state_code 
                                        LEFT JOIN tbl_I94CIT_I94RES_Mapping AS I94CIT_I94RES1 on IMM.i94cit = I94CIT_I94RES1.country_code
                                        LEFT JOIN tbl_I94MODE_Mapping AS I94MODE on IMM.i94mode = I94MODE.mode_name 
                                        LEFT JOIN tbl_I94PORT_Mapping AS I94PORT on IMM.i94port = I94PORT.city_code 
                                        LEFT JOIN tbl_I94VISA_Mapping AS I94VISA on IMM.visatype = I94VISA.visa_type 
                                        WHERE IMM.cicid IS NOT NULL AND IMM.arrdate IS NOT NULL """)

    # perform data quality check
    tbl_immigration.createOrReplaceTempView("final_immigration")
    immigration_null_check = spark.sql(
        """SELECT COUNT(*) AS invalid_cicid FROM final_immigration WHERE cicid IS  NULL""")

    if(immigration_null_check.count() > 1):
        print("Null values found in final immigration data, please check")
        print("Displaying sample data for column [cicid] with NULL Values:")
        tbl_immigration.filter(tbl_immigration["cicid"].isNull()).show()
    else:
        tbl_immigration = tbl_immigration.withColumn('arrival_month', month(tbl_immigration.arrival_date)) \
            .withColumn('arrival_year', year(tbl_immigration.arrival_date))
        
        # Count records of Immigration table for data quality checks
        count_immigration_final_table = tbl_immigration.count()
        print("total no of records in immigration table:",
              count_immigration_final_table)
        
        # save immigration data to output folder by month
        if(count_immigration_final_table > 0):
            output_files_path = output_path + "immigration/"
            tbl_immigration.write.mode("overwrite").partitionBy(
                "arrival_year", "arrival_month").parquet(output_files_path+"immigration.parquet")


def process_airport_codes(spark, input_files_path, output_files_path):
    """
    - Read given airport data and externally downloaded iata_codes data
    - Apply appropriate filters to generate final US cities airport iata_codes data
    """
    df_staging_airport_codes = spark.read.option("header", True).options(
        delimiter=",").csv(input_files_path+"airport-codes_csv.csv")
    df_staging_iata_codes = spark.read.option("header", True).options(
        delimiter=",").csv(input_files_path+"US_iata_codes.csv")
    # drop rows with iata_code as null
    df_staging_airport_codes.dropna(subset=["iata_code"])
    df_staging_airport_codes = df_staging_airport_codes.filter(
        df_staging_airport_codes['iata_code'].rlike('^[A-Z]'))
    df_staging_airport_codes.dropna()
    # filter airport_codes data for US airports
    df_final_airport_codes = df_staging_airport_codes.filter(
        (df_staging_airport_codes['iata_code']) != 'USA')
    df_final_airport_codes = df_staging_airport_codes.filter(
        (df_staging_airport_codes['iso_country']) == 'US')

    # prepare clean airport codes
    df_final_airport_codes.createOrReplaceTempView("tbl_stg_airport_codes")
    df_staging_iata_codes.createOrReplaceTempView("tbl_stg_iata_codes")
    tbl_city_airport_info = spark.sql("""
                                    SELECT ia.City as city,
                                           ia.State as state_code,
                                           ia.Code as iata_code,
                                           ap.continent AS continent,
                                           ap.iso_country AS iso_country,
                                           ap.iso_region AS iso_region,
                                           ap.municipality,
                                           ap.gps_code,                                           
                                           ap.local_code,
                                           ap.coordinates,
                                           ap.name AS airport_name,
                                           ap.type AS airport_type
                                    FROM tbl_stg_airport_codes ap
                                    JOIN tbl_stg_iata_codes ia
                                      ON  ap.iata_code=ia.Code
                                    WHERE ap.continent = 'NA'
                                     AND ap.iso_country = 'US'
                                    """)

    # perform null checks for city_airport_data and save data
    tbl_city_airport_info.createOrReplaceTempView("airport_codes_final")
    airport_codes_null_check = spark.sql("""
                                         SELECT COUNT(*) AS invalid_airport_codes FROM airport_codes_final
                                         WHERE iata_code IS NULL
                                         """)
    if(airport_codes_null_check.count() > 1):
        print("Null values found in final airport codes data, please check")
    else:
        airport_codes_row_count = tbl_city_airport_info.count()
        print("total no of records in tbl_city_airport_info:" +str(airport_codes_row_count))
        tbl_city_airport_info.write.mode("overwrite").parquet(
            output_files_path+"city_airport_codes.parquet")


def process_US_demographics(spark, input_files_path, output_files_path):
    """
    - Read US Demographics data file
    - Apply filters to remove Nulls
    - Generate final us_demographics data using specific columns
    """
    df_staging_us_cities_demographics = spark.read.option("header", True).options(
        delimiter=";").csv(input_files_path+"us-cities-demographics.csv")
    df_staging_us_cities_demographics = df_staging_us_cities_demographics.filter(
        df_staging_us_cities_demographics.City.isNotNull())
    df_staging_us_cities_demographics = df_staging_us_cities_demographics.dropna()

    df_staging_us_cities_demographics.createOrReplaceTempView(
        "tbl_stg_us_cities_demographics")
    tbl_us_city_demographics = spark.sql("""
                                                SELECT DISTINCT LOWER(City) as city, 
                                                UPPER(State) as state, 
                                                `Median Age` AS median_age, 
                                                `Male Population` AS male_population, 
                                                `Female Population` AS female_population, 
                                                `Total Population` AS total_population, 
                                                `Number of Veterans` AS number_of_veterans, 
                                                `Foreign-born` AS foreign_born, 
                                                `Average Household Size` AS average_household_size, 
                                                rtrim(ltrim(`State Code`)) AS state_code, 
                                                race, 
                                                count
                                                FROM tbl_stg_us_cities_demographics                  
                                            """)

    # perform NULL data check and save data
    tbl_us_city_demographics.createOrReplaceTempView(
        "us_cities_demo_final")
    us_cities_demo_check = spark.sql(
        """SELECT COUNT(*) AS invalid_cities FROM us_cities_demo_final WHERE city IS NULL""")
    if(us_cities_demo_check.count() > 1):
        print("NULL data found for us_city_demographics, please check")
    else:
        row_count_us_city_demo = tbl_us_city_demographics.count()
        print("row count for us city demographics:" +
              str(row_count_us_city_demo))
        if(row_count_us_city_demo <= 0):
            print("no data found for us_city_demographics, please check")
        else:
            tbl_us_city_demographics.write.mode("overwrite").parquet(
                output_files_path+"us_city_demographics.parquet")


def process_time_data(spark, output_files_path):
    """
    - read the arrival & departure date columns from immigration data
    - generate date time columns and save it as time_data.parquet
    """
    # get filepath to parquet data file
    input_data_path = os.path.join(
        output_files_path, 'immigration/immigration.parquet/arrival_year=2016/*/*.parquet')
    df_immigration_data = spark.read.parquet(input_data_path)
    df_immigration_data.createOrReplaceTempView("tbl_immigration_data")

    immi_row_count_fortime = df_immigration_data.count()
    print("row count for tbl_immigration_data:" + str(immi_row_count_fortime))

    # Generate time data
    df_time_data = spark.sql("""
                            SELECT DISTINCT arrival_date AS date_value FROM tbl_immigration_data WHERE arrival_date IS NOT NULL 
                            UNION 
                            SELECT DISTINCT departure_date AS date_value FROM tbl_immigration_data WHERE departure_date IS NOT NULL 
                            """)
    tbl_datetime = df_time_data.withColumn('day', dayofmonth(df_time_data.date_value)) \
        .withColumn('week', weekofyear(df_time_data.date_value)) \
        .withColumn('month', month(df_time_data.date_value)) \
        .withColumn('year', year(df_time_data.date_value)) \
        .withColumn('weekday', dayofweek(df_time_data.date_value))

    row_count_time = tbl_datetime.count()
    print("row count in tbl_datetime:" + str(row_count_time))

    # write the time data to output_path
    tbl_datetime.write.mode("overwrite").parquet(
        output_files_path + "time_data.parquet")


def main():
    """
    Create Spark Session and provide input and output locations for functions as needed 

    - Call create_spark_session() to create Spark Session for use in other function
    - assign input and output file paths
    - Call process_immigration_data() function to read data from SAS source and write to parquet files for tbl_immigration
    - Call process_airport_codes() function to read data from airport & iata_codes and write to parquet for tbl_city_airport_info
    - Call process_US_demographics() function to read US-city_demographics and write to parquet files  for tbl_us_city_demographics
    - Call process_time_data() function to read date columns from immigration data and write to parquet files for tbl_datetime
    - Finally, stop the Spark session

    """

    print("*** Start etl process ***")
    spark = create_spark_session()

    input_path_immigration_data = "sas_parquet_data/"
    input_files_path = "datafiles/"
    output_files_path = "outputdata/"
    
    print("*** Start etl function for immigration data ***")
    process_immigration_data(
        spark, input_path_immigration_data, output_files_path)
    print("*** End etl function for immigration data ***")
    
    print("*** Start etl function for airport data ***")
    process_airport_codes(spark, input_files_path, output_files_path)
    print("*** End etl function for immigration data ***")
    
    print("*** Start etl function for us city demographics data ***")
    process_US_demographics(spark, input_files_path, output_files_path)
    print("*** End etl function for us city demographics data ***")
    
    print("*** Start etl function for time data ***")
    process_time_data(spark, output_files_path)
    print("*** End etl function for time data ***")
    
    spark.stop()

if __name__ == "__main__":
    main()
