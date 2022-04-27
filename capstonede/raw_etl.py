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
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create SparkSession and Config to use in ETL as below:
    
    - Config spark.jars.repositories to https://repos.spark-packages.org/
    - Config spark.jars.packages to org.apache.hadoop:hadoop-aws:2.7.3
    - Config spark.jars.packages to saurfang:spark-sas7bdat:2.0.0-s_2.11
    - Config spark.hadoop.fs.s3a.impl to org.apache.hadoop.fs.s3a.S3AFileSystem
    - Config spark.hadoop.fs.s3a.awsAccessKeyId to os.environ['AWS_ACCESS_KEY_ID']
    - Config spark.hadoop.fs.s3a.awsSecretAccessKey to os.environ['AWS_SECRET_ACCESS_KEY']
    - Config spark.hadoop.fs.s3a.multiobjectdelete.enable to false
    
    """ 
        
    spark = SparkSession \
        .builder \
        .appName("snayakwadi_capstone_immigration_data") \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .enableHiveSupport() \
        .getOrCreate()
#         .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
#         .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
#         .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
#         .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
#         .enableHiveSupport() \
#         .getOrCreate()
    
    return spark

def download_immigration_data(spark,input_path,output_path):  
    sas_immigration_data_path = input_path
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    for month in months:
        sas_file_name = "{}{}{}{}".format(sas_immigration_data_path,"i94_",month,"16_sub.sas7bdat")
        sas_git_path = "com.github.saurfang.sas.spark"
        
        df_spark_raw_sas_data = spark.read.format(sas_git_path).load(sas_file_name)
        output_file_path = output_path +month+"2016/"
        df_spark_raw_sas_data.write.mode("overwrite").parquet(output_file_path)


def process_immigration_data(spark,input_path,output_path):
    
    # get filepath to raw immigration data files
    input_parquet_files_path = os.path.join(input_path, '*/*.parquet')
    #input_parquet_files_path = input_path +'apr2016/*.parquet'
    print("input path is" ,input_parquet_files_path)
    df_staging_immigration_data = spark.read.parquet(input_parquet_files_path)
    
    #udf_datetime = udf(lambda x: convert_double_to_datetime(x), T.DateType())
    udf_getdate = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    df_staging_immigration_data = df_staging_immigration_data.withColumn('arrival_date',udf_getdate(df_staging_immigration_data.arrdate))           .withColumn('departure_date',udf_getdate(df_staging_immigration_data.depdate))
    
    df_staging_immigration_data = df_staging_immigration_data.fillna({\
         'i94yr':      0.0,\
         'i94addr':   'NA',\
         'arrdate':   'NA',\
         'depdate':   'NA',\
         'i94bir':    'NA',\
         'i94visa':    0.0,\
         'count':      0.0,\
         'dtadfile':  'NA',\
         'visapost':  'NA',\
         'occup':     'NA',\
         'entdepa':   'NA',\
         'entdepd':   'NA',\
         'entdepu':   'NA',\
         'matflag':   'NA',\
         'biryear':   'NA',\
         'dtaddto':   'NA',\
         'gender':    'NA',\
         'insnum':    'NA',\
         'airline':   'NA',\
         'admnum':     0.0,\
         'fltno':     'NA',\
         'visatype':  'NA'\
         })

    
    initial_count = df_staging_immigration_data.count()
    print("initial raw data count is",str(initial_count))
    
    #SAS file section I94ADDR contains state code
    df_staging_I94_state_code = spark.read.option("header",True).options(delimiter=',').csv("datafiles/I94_state_code_name.csv")

    #SAS file section I94CIT_I94RES contains country code and country name info
    df_staging_I94_country_code = spark.read.option("header",True).options(delimiter=',').csv("datafiles/I94_country_code_name.csv")

    #SAS file section I94MODE contains  I94 mode type info
    df_staging_I94_mode = spark.read.option("header",True).options(delimiter=',').csv("datafiles/I94_mode.csv")

    #SAS file section I94PORT contains port and city info
    df_staging_I94_port_city = spark.read.option("header",True).options(delimiter=',').csv("datafiles/I94_port_city.csv")

    #SAS file section I94VISA contains visa type info
    df_staging_I94_visa_type = spark.read.option("header",True).options(delimiter=',').csv("datafiles/I94_visa_type.csv")
    
    df_staging_immigration_data.createOrReplaceTempView("tbl_stg_immigration_data")
    df_staging_I94_state_code.createOrReplaceTempView("tbl_I94ADDR_Mapping")
    df_staging_I94_country_code.createOrReplaceTempView("tbl_I94CIT_I94RES_Mapping")
    df_staging_I94_mode.createOrReplaceTempView("tbl_I94MODE_Mapping")
    df_staging_I94_port_city.createOrReplaceTempView("tbl_I94PORT_Mapping")
    df_staging_I94_visa_type.createOrReplaceTempView("tbl_I94VISA_Mapping")
    df_final_immigration = spark.sql("""
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
                                                       ,IMM.visatype 
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
                                        LEFT JOIN tbl_I94VISA_Mapping AS I94VISA on IMM.visatype = I94VISA.visa_type WHERE IMM.arrdate IS NOT NULL
                                        """)
    
    df_final_immigration = df_final_immigration.withColumn('arrival_month',month(df_final_immigration.arrival_date)) \
                                               .withColumn('arrival_year',year(df_final_immigration.arrival_date))
    
    # Count records of Immigration table for data quality checks
    count_immigration_final_table = df_final_immigration.count()
    print("total no of records in immigration table: {} " , count_immigration_final_table)
    
    #save immigration data to output folder by month
    if(count_immigration_final_table > 0):
        output_files_path = output_path + "immigration/"
        df_final_immigration.write.mode("overwrite").partitionBy("arrival_year","arrival_month").parquet(output_files_path+"immigration.parquet")


def process_airport_codes(spark,input_files_path,output_files_path):
    
    df_staging_airport_codes = spark.read.option("header",True).options(delimiter=",").csv(input_files_path+"airport-codes_csv.csv")
    df_staging_iata_codes = spark.read.option("header",True).options(delimiter=",").csv(input_files_path+"US_iata_codes.csv")
    #drop rows with iata_code as null
    df_staging_airport_codes.dropna(subset=["iata_code"])
    df_staging_airport_codes= df_staging_airport_codes.filter(df_staging_airport_codes['iata_code'].rlike('^[A-Z]'))
    df_staging_airport_codes.dropna()
    #filter airport_codes data for US airports
    df_final_airport_codes = df_staging_airport_codes.filter((df_staging_airport_codes['iata_code']) != 'USA')
    df_final_airport_codes = df_staging_airport_codes.filter((df_staging_airport_codes['iso_country']) == 'US')
    
    #prepare clean airport codes
    df_final_airport_codes.createOrReplaceTempView("tbl_stg_airport_codes")
    df_staging_iata_codes.createOrReplaceTempView("tbl_stg_iata_codes")
    df_final_city_airport_info = spark.sql("""
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
                                           ap.type AS airport_type,
                                           ap.ident AS airport_identity
                                    FROM tbl_stg_airport_codes ap
                                    JOIN tbl_stg_iata_codes ia
                                      ON  ap.iata_code=ia.Code
                                    WHERE ap.continent = 'NA'
                                     AND ap.iso_country = 'US'
                                    """)
    
    df_final_city_airport_info.write.mode("overwrite").parquet(output_files_path+"city_airport_codes.parquet")
    
def process_US_demographics(spark,input_files_path,output_files_path):
    
    df_staging_us_cities_demographics = spark.read.option("header",True).options(delimiter=";").csv(input_files_path+"us-cities-demographics.csv")
    df_staging_us_cities_demographics = df_staging_us_cities_demographics.filter(df_staging_us_cities_demographics.City.isNotNull())
    df_stagin_us_cities_demographics = df_staging_us_cities_demographics.dropna()
    
    df_staging_us_cities_demographics.createOrReplaceTempView("tbl_stg_us_cities_demographics")
    df_final_us_city_demographics = spark.sql("""
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
    row_count_us_city_demo = df_final_us_city_demographics.count()
    print("row count for us city demographics:" +str(row_count_us_city_demo))
    #save data in parquet
    df_final_us_city_demographics.write.mode("overwrite").parquet(output_files_path+"us_city_demographics.parquet")
    
def process_time_data(spark, output_path, output_files_path):

    # get filepath to parquet data file
    input_data_path = os.path.join(output_path, 'immigration/immigration.parquet/arrival_year=2016/*/*.parquet')
#     input_data_path = output_path +"immigration/april/immigration.parquet/arrival_year=2016/arrival_month=4/*.parquet"
    df_immigration_data = spark.read.parquet(input_data_path)
    df_immigration_data.createOrReplaceTempView("tbl_immigration_data")
    
    immi_row_count_fortime = df_immigration_data.count()
    print("row counr for time from immi" + str(immi_row_count_fortime))
    
    #Generate time data
    df_time_data = spark.sql("""
                            SELECT DISTINCT arrival_date AS date_value FROM tbl_immigration_data WHERE arrival_date IS NOT NULL 
                            UNION 
                            SELECT DISTINCT departure_date AS date_value FROM tbl_immigration_data WHERE departure_date IS NOT NULL 
                            """)
    df_final_time = df_time_data.withColumn('day',dayofmonth(df_time_data.date_value)) \
    .withColumn('week',weekofyear(df_time_data.date_value)) \
    .withColumn('month',month(df_time_data.date_value)) \
    .withColumn('year',year(df_time_data.date_value)) \
    .withColumn('weekday',dayofweek(df_time_data.date_value)) 
    
    
    df_final_time.show()
    row_count_time = df_final_time.count()
    print("row count in time df:" + str(row_count_time))
    
    #write the time data to output_path
    df_final_time.write.mode("overwrite").parquet(output_files_path + "time_data.parquet")
    #df_final_time.write.mode("overwrite").parquet(output_files_path + "time_data.parquet")

def generate_immigration_by_city_data(spark,output_path, output_files_path):
    
    #load all necessary tables
    # get filepath to parquet data file
    input_data_path = os.path.join(output_path, 'immigration/immigration.parquet/arrival_year=2016/*/*.parquet')
    df_final_immigration= spark.read.parquet(input_data_path)
    df_final_immigration.createOrReplaceTempView("tbl_immigration_data")
    
    df_final_city_airport_info= spark.read.parquet(output_path + "city_airport_codes.parquet/*.parquet")
    df_final_city_airport_info.createOrReplaceTempView("tbl_city_airport_info")
    
    df_final_us_city_demographics = spark.read.parquet(output_path+ "us_city_demographics.parquet/*.parquet")
    df_final_us_city_demographics.createOrReplaceTempView("tbl_us_city_demographics")
    
    input_data_path = os.path.join(output_path,'time_data.parquet/*.parquet')
    df_time = spark.read.parquet(input_data_path)
    df_time.createOrReplaceTempView("tbl_time")
    
    df_time.show()
    #prepare final data
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
                                         JOIN tbl_us_city_demographics cdem
                                           ON imm.i94port_statecode = cdem.state_code
                                          AND LOWER(imm.i94port_city) = LOWER(cdem.city)
                                         JOIN tbl_city_airport_info cai
                                           ON LOWER(cdem.city) = LOWER(cai.city)
                                          AND trim(cdem.state_code) = trim(cai.state_code)
                                         JOIN tbl_time dt
                                           ON imm.arrival_date = dt.date_value
                                     """).dropDuplicates()
    df_immigration_by_city.dropna()

    df_immigration_by_city.show(5)
    
    row_count_immigration_by_city = df_immigration_by_city.count()
    print("final analytical row count is: "+str(row_count_immigration_by_city))

def main():
    """
    Create Spark Session and provide input and output location in S3. Its Start point for this ETL process. 
    
    - Read the config AWS credential from `dl.cfg`
    - Call create_spark_session() to create Spark Session for use in other function
    - Prepare input_data and output_data location in S3
    - Call process_immigration_data() function for Load data from S3, ETL and write to parquet files in S3 (immigration table)
    - Call process_worldtemp_data() function for Load data from S3, ETL and write to parquet files in S3 (worldtemp table)
    - Call process_demographic_data() function for Load data from S3, ETL and write to parquet files in S3 (demographic table)
    - Call process_pollution_data() function for Load data from S3, ETL and write to parquet files in S3 (pollution table)
    - Call process_airline_data() function for Load data from S3, ETL and write to parquet files in S3 (airline table)
    - Call process_time_data() function for Load data from S3, ETL and write to parquet files in S3 (time table)
    - Finally, stop the Spark session
    
    """
    
    print("## Start Downloading immigration data ##")
    
    spark = create_spark_session()
    
    sas_input_path = "../../data/18-83510-I94-Data-2016/"
    sas_output_path =  "sas_parquet_data" 
    
    input_path = "sas_parquet_data/" 
    output_path = "outputdata/"
    #output_path = "s3a://snudacitybuck/capstone/"
    
    input_files_path = "datafiles/"
    output_files_path = "outputdata/"
    #download_immigration_data(spark,sas_input_path,sas_output_path)
    #print('parquet files saved to sas_output_path')

    #process_immigration_data(spark,input_path,output_path)
    #process_airport_codes(spark,input_files_path,output_files_path)
    #process_US_demographics(spark,input_files_path,output_files_path)
    #process_time_data(spark, output_path, output_files_path)
    generate_immigration_by_city_data(spark,output_path, output_files_path)
    print("=> The ETL Process is End!!")

    spark.stop()


if __name__ == "__main__":
    main()