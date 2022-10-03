import pandas as pd
import datetime as dt
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, row_number, regexp_replace, upper, trim, split, desc
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType as TS, LongType as Long, StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date


def cleaning_data(x):
    """
    Receives the value of the pandas string being mapped to apply cleanup techniques.
    
    Parameters
    ----------
    x : pandas serie value
        The actual serie value that will be cleaning
        
    Returns
    -------
    x : pandas serie value
        The actual serie value that was cleaning 
    """
    temp = x
    temp = temp.str.strip()
    temp = temp.str.upper()
    temp = temp.str.replace("'", "")
    temp = temp.str.replace(",|-|\.", "")
    temp = temp.str.replace(".*UNKNOWN.*|.*NO PORT.*|INVALID.*|COLLAPSED.*|NO COUNTRY.*", "TO BE DETERMINATED",
                            regex = True)
    return temp


def create_spark_session():
    """
    Get or create the spark session with the hadoop config. 
    
    Variables
    --------
    
    spark : SparkSession
            Get the spark session object and return
    
    """
    
    spark = SparkSession.builder.\
            config("spark.jars.repositories", "https://repos.spark-packages.org/").\
            config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
            enableHiveSupport().getOrCreate()
    
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    return spark


def dim_airport(spark, input_path, output_path):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_paht : String
                 The root path to read the stage
    
    output_path : String
                  The root path to write the stage in parquet (the new dimension)
                  
                  
    Variables
    ---------
    
    airportSchema : StructType
                The schema to mapping the correct datatype of stage csv.
    """
    airportSchema = R([
        Fld("ident", Str()),
        Fld("type", Str()),
        Fld("name", Str()),
        Fld("elevation_ft", Int()),
        Fld("continent", Str()),
        Fld("iso_country", Str()),
        Fld("iso_region", Str()),
        Fld("municipality", Str()),
        Fld("gps_code", Str()),
        Fld("iata_code", Str()),
        Fld("local_code", Str()),
        Fld("coordinates", Str())
    ])
    
    print("*****starting process airport dimension*****\n")
    df = spark.read.option("delimiter", ",").csv(input_path, header = True, schema = airportSchema).distinct()
    
    ## data engineering
    
    ### split columns 
    df = df.withColumn('state', split(col('iso_region'),'-')[1]) \
           .withColumn('latitude', split(col('coordinates'), ',')[0].cast(Dbl())) \
           .withColumn('longitude', split(col('coordinates'), ',')[1].cast(Dbl()))
    
    ### filter us airports
    df = df.filter("UPPER(iso_country) == 'US'") \
           .drop('iso_region') \
           .drop('coordinates') \
           .drop('continent')
    
    ### filter primary key that should be not null
    df = df.filter("local_code is not null")
    
    ### cleaning dataframe
    df_clean = df
    
    for field in df_clean.schema:
        if field.dataType == Str():
            df_clean = df_clean.withColumn(field.name, upper(col(field.name)))
            df_clean = df_clean.withColumn(field.name, trim(col(field.name)))
            df_clean = df_clean.withColumn(field.name, regexp_replace(col(field.name), "'", ""))
            df_clean = df_clean.withColumn(field.name, regexp_replace(col(field.name), ",|-|\.", ""))
            df_clean = df_clean.withColumn(field.name, regexp_replace(col(field.name),
                                ".*UNKNOWN.*|.*NO PORT.*|INVALID.*|COLLAPSED.*|NO COUNTRY.*", "TO BE DETERMINATED"))
            df_clean = df_clean.fillna("TO BE DETERMINATED", subset=[field.name])
    
    ### count rows
    print(f"Airport Dimension Count: {df_clean.count()}\n")
    
    ### write dimension in parquet files
    df_clean.write.mode('overwrite').partitionBy(['state', 'local_code']).parquet(f"{output_path}dim_airports")
    
    print("*****process airport dimension completed*****\n")


def dim_city(spark, input_path, output_path):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_paht : String
                 The root path to read the stage
    
    output_path : String
                  The root path to write the stage in parquet (the new dimension)
                  
    Variables
    ---------
    
    citySchema : StructType
                The schema to mapping the correct datatype of stage csv.
    """
    
    citySchema = R([
        Fld("city", Str()),
        Fld("state", Str()),
        Fld("median_age", Dbl()),
        Fld("male_population", Int()),
        Fld("female_population", Int()),
        Fld("total_population", Int()),
        Fld("number_of_veterans", Int()),
        Fld("foreign_born", Int()),
        Fld("average_household_size", Dbl()),
        Fld("state_code", Str()),
        Fld("race", Str()),
        Fld("count", Int())
    ])

    print("*****starting process city dimension*****\n")
    
    ### read stage
    df = spark.read.option("delimiter", ";").csv(input_path, header = True, schema = citySchema).distinct()
    
    ## data engineering
    
    df_clean = df
    
    for field in df_clean.schema:
        if field.dataType == Str():
            df_clean = df_clean.withColumn(field.name, upper(col(field.name)))
            df_clean = df_clean.withColumn(field.name, trim(col(field.name)))
            df_clean = df_clean.withColumn(field.name, regexp_replace(col(field.name), "'", ""))
            df_clean = df_clean.withColumn(field.name, regexp_replace(col(field.name), ",|-|\.", ""))
            df_clean = df_clean.withColumn(field.name, regexp_replace(col(field.name), 
                                ".*UNKNOWN.*|.*NO PORT.*|INVALID.*|COLLAPSED.*|NO COUNTRY.*", "TO BE DETERMINATED"))
            df_clean = df_clean.fillna("TO BE DETERMINATED", subset=[field.name])
            
    df_clean =  df_clean.filter(df_clean.state_code.isNotNull()) \
                           .dropDuplicates(subset=['state_code', 'race', 'city'])
    
    ### count rows
    print(f"City Dimension Count: {df_clean.count()}\n")
    
    ### write dimension in parquet files
    df_clean.write.mode('overwrite').partitionBy('state').parquet(f"{output_path}dim_cities")
    
    print("*****process city dimension completed*****\n")
    

def dim_country(spark, input_stages_data, output_dim_data):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_stages_data : String
                       The root path to read the stage
    
    output_dim_data : String
                  The root path to write the stage in parquet (the new dimension)

    """
    print("*****starting process country dimension*****\n")

    ### read stage
    df_country = pd.read_csv(input_stages_data, delimiter = '=', engine='python')
    
    ## data engineering
    
    ### cleaning columns
    df_country.columns = df_country.columns.str.strip()
    df_country.columns = df_country.columns.str.replace("'", "")
    
    ### cleaning values
    df_country = df_country.apply(lambda x:  cleaning_data(x) if x.dtypes == 'object' else x)
    
    df_country = df_country.dropna()
    df_country = df_country.drop_duplicates()
    
    ### write dataframe to parquet files
    to_ins = spark.createDataFrame(df_country)
    
    print(f"Country Dimension Count: {to_ins.count()}\n")

    to_ins.write.mode('overwrite').parquet(f"{output_dim_data}dim_countries")

    print("*****process country dimension completed*****\n")
        
    
def dim_port(spark, input_stages_data, output_dim_data):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_stages_data : String
                       The root path to read the stage
    
    output_dim_data : String
                  The root path to write the stage in parquet (the new dimension)

    """

    print("*****starting process port dimension*****\n")

    ### read stages
    df_port = pd.read_csv(input_stages_data, delimiter = '=', engine = 'python')
    
    ## data engineering
    
    ### cleaning columns
    df_port.columns = df_port.columns.str.strip()
    df_port.columns = df_port.columns.str.replace("'", "")
    df_port.CITY = df_port.CITY.str.replace(',', '|')
    
    ### cleaning values
    df_port = df_port.apply(lambda x: cleaning_data(x) if x.dtypes == 'object' else x)
    
    ### Split City column to expand city and state columns
    chunk = df_port.copy()
    temp = chunk.CITY.str.split('|', expand = True).copy()
    temp = temp.apply(lambda x: x.str.strip())
    temp.columns = ['CITY', 'STATE', 'OTHER']
    temp.fillna("TO BE DETERMINATED", inplace = True)
    
    temp2 = temp[(temp.STATE.str.len() > 2) & \
                 (temp.STATE.str.contains("\(", regex = True) == False) & \
                 (temp.STATE != 'TO BE DETERMINATED')].copy()
    
    temp3 = temp2[(temp2.OTHER != 'TO BE DETERMINATED') & (temp2.OTHER.str.len() > 2)].copy()
    
    temp2['CITY'] = temp2['CITY'] + ' ' + temp2['STATE']
    temp2['STATE'] = temp2['OTHER']
    temp3['CITY'] = temp3['CITY'] + ' ' + temp3['STATE'] + ' ' + temp3['OTHER']
    temp3['STATE'] = 'TO BE DETERMINATED'
    temp2.loc[temp3.index, :] = temp3
    temp2['OTHER'] = 'TO BE DETERMINATED'
    temp.loc[temp2.index, :] = temp2
    chunk.loc[temp.index, 'CITY'] = temp['CITY']
    chunk['STATE'] = temp['STATE']
    chunk.columns = ['local_code', 'municipality', 'state']
    
    ### read airport dimension to insert the new data
    dim_airports = spark.read.parquet(f"{output_dim_data}dim_airports")
    
    chunk['ident'] = 'TO BE DETERMINATED'
    chunk['type'] = 'TO BE DETERMINATED'
    chunk['name'] = 'TO BE DETERMINATED'
    chunk['elevation_ft'] = -1
    chunk['iso_country'] = 'TO BE DETERMINATED'
    chunk['gps_code'] = 'TO BE DETERMINATED'
    chunk['iata_code'] = 'TO BE DETERMINATED'
    chunk['latitude'] = -1.0
    chunk['longitude'] = -1.0
    chunk = chunk[dim_airports.columns]
    
    ### Insert dataframe in parquet files
    to_ins = spark.createDataFrame(chunk)
    
    ### Get the port codes that not exists in the actual airport dimension
    to_ins = to_ins.join(dim_airports, to_ins.local_code == dim_airports.local_code, "left_outer") \
        .where(dim_airports.local_code.isNull()) \
        .select(to_ins.ident, to_ins.type, to_ins.name, to_ins.elevation_ft, to_ins.iso_country, \
               to_ins.municipality, to_ins.gps_code, to_ins.iata_code, to_ins.latitude, \
               to_ins.longitude, to_ins.state, to_ins.local_code)
    
    print(f"Port Dimension Count: {to_ins.count()}\n")
    
    to_ins.write.mode('append').partitionBy(['state', 'local_code']).parquet(f"{output_dim_data}dim_airports")
    
    print("*****process port dimension append to airport dimension*****\n")
    
    
def dim_mode(spark, input_stages_data, output_dim_data):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_stages_data : String
                       The root path to read the stage
    
    output_dim_data : String
                  The root path to write the stage in parquet (the new dimension)

    """
    print("*****starting process port dimension*****\n")

    ### read stage
    df_code = pd.read_csv(input_stages_data, delimiter = '=', engine = 'python')
    
    ### cleaning columns
    df_code.columns = df_code.columns.str.strip()  
    df_code.columns = df_code.columns.str.replace("'", "")
    df_code.columns = df_code.columns.str.lower()
    
    ### cleaning values
    df_code = df_code.apply(lambda x:  cleaning_data(x) if x.dtypes == 'object' else x)
    
    ### Write data in parquet files
    to_ins = spark.createDataFrame(df_code)
    
    print(f"Mode Dimension Count: {to_ins.count()}\n")
    
    to_ins.write.mode('overwrite').parquet(f"{output_dim_data}dim_modes")
    
    print("*****process mode dimension append to airport dimension*****\n")

    
def dim_city_port(spark, input_stages_data, output_dim_data):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_stages_data : String
                       The root path to read the stage
    
    output_dim_data : String
                  The root path to write the stage in parquet (the new dimension)

    """
    print("*****starting city port dimension*****\n")

    ### read stage
    df_city = pd.read_csv(input_stages_data, delimiter = '=', engine = 'python')
    
    ### cleaning columns
    df_city.columns = df_city.columns.str.strip()
    df_city.columns = df_city.columns.str.lower()
    df_city.columns = df_city.columns.str.replace("'", "")
    
    ### cleaning values
    df_city = df_city.apply(lambda x: cleaning_data(x) if x.dtypes == 'object' else x)
    
    df_city.columns = ['state_code', 'city']
    
    ### read city dimension to insert the new data
    dim_city = spark.read.parquet(f"{output_dim_data}dim_cities")
    
    df_city['state'] = 'TO BE DETERMINATED'
    df_city['median_age'] = -1.0
    df_city['male_population'] = -1
    df_city['female_population'] = -1
    df_city['total_population'] = -1
    df_city['number_of_veterans'] = -1
    df_city['foreign_born'] = -1
    df_city['average_household_size'] = -1.0
    df_city['race'] = 'TO BE DETERMINATED'
    df_city['count'] = -1
    
    df_city = df_city[dim_city.columns]
    
    ### Insert dataframe in parquet files
    to_ins = spark.createDataFrame(df_city)
    
    to_ins = to_ins.join(dim_city, to_ins.state_code == dim_city.state_code, "left_outer") \
        .where(dim_city.state_code.isNull()) \
        .select(to_ins.city, to_ins.state, to_ins.median_age, to_ins.male_population, to_ins.female_population, \
               to_ins.total_population, to_ins.number_of_veterans, to_ins.foreign_born, to_ins.average_household_size,
               to_ins.state_code, to_ins.race, to_ins['count'])
    
    to_ins =  to_ins.filter(to_ins.state_code.isNotNull()) \
                           .dropDuplicates(subset=['state_code', 'race', 'city'])
    
    print(f"City Dimension Count: {to_ins.count()}\n")

    to_ins.write.mode('append').partitionBy('state').parquet(f"{output_dim_data}dim_cities")
    
    print("*****process city code append to city dimension*****\n")

    
def dim_visa(spark, input_stages_data, output_dim_data):
    """
    Performs the reading of the data in the stage, according to the input parameters and 
    writes the result in parquet according to the output parameter.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_stages_data : String
                       The root path to read the stage
    
    output_dim_data : String
                  The root path to write the stage in parquet (the new dimension)

    """
    print("*****starting visa dimension*****\n")
    
    ### read stage
    df_visa = pd.read_csv(input_stages_data, delimiter = '=', engine = 'python')
    
    ### cleaning columns
    df_visa.columns = df_visa.columns.str.strip()
    df_visa.columns = df_visa.columns.str.replace("'", "")
    
    ### cleaning values
    df_visa = df_visa.apply(lambda x:  cleaning_data(x) if x.dtypes == 'object' else x)
    
    ### Write data in parquet files
    to_ins = spark.createDataFrame(df_visa)
    
    print(f"Visa Dimension Count: {to_ins.count()}\n")
    
    to_ins.write.mode('overwrite').parquet(f"{output_dim_data}dim_visa")
    
    print("*****process visa dimension completed*****\n")

        
def process_dimensions(spark, input_stages_data, output_dim_data):
    """
    It handles the call of the reference on all the processes of the dimensions, 
    since each one of them has its own etl.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_stages_data : String
                       The root path to read the stage
    
    output_dim_data : String
                  The root path to write the stage in parquet (the new dimension)
    """
    
    print("\n\n-------------Starting processing dimensions-------------\n")
    dim_airport(spark, input_stages_data + "airport-codes_csv.csv", output_dim_data)
    dim_city(spark, input_stages_data + "us-cities-demographics.csv", output_dim_data)
    dim_country(spark, input_stages_data + "country_code.csv", output_dim_data)
    dim_port(spark, input_stages_data + "port_code.csv", output_dim_data)
    dim_mode(spark, input_stages_data + "mode_code.csv", output_dim_data)
    dim_city_port(spark, input_stages_data + "city_code.csv", output_dim_data)
    dim_visa(spark, input_stages_data + "visa_code.csv", output_dim_data)
    print("\n\n-------------Process dimensions completed-------------\n")

    
def process_fact(spark, input_dim_data, output_fact_data):
    """
    It handles the call of the fact table process.
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_dim_data : Dictionary
                     Contains the path of all dimensions that going to be join with the fact table
    
    output_fact_data : String
                      The root path to write the fact table in parquet
    """
    
    print("*****starting fact data*****\n")

    ### Read dimensions
    dim_country = spark.read.parquet(input_dim_data['country'])
    dim_visa    = spark.read.parquet(input_dim_data['visa'])
    dim_mode    = spark.read.parquet(input_dim_data['mode'])
    dim_airport = spark.read.parquet(input_dim_data['airport'])
    dim_city    = spark.read.parquet(input_dim_data['city'])
    dim_city2   = spark.read.parquet(input_dim_data['city'])
    
    ### Get unique state_code for join in the fact table
    dim_city = dim_city.dropDuplicates(subset=['state_code'])
    
    ### Convert date sas
    format_dt_sas = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() \
                        if x else dt.datetime(1960,1,1).date().isoformat())
    
    df = spark.read.format('com.github.saurfang.sas.spark') \
                    .load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    ## data engineering
    
    ### get format date
    df_clean = df.withColumn("arrdate", format_dt_sas(df.arrdate)) \
                 .withColumn("depdate", format_dt_sas(df.depdate))
    
    ### cleaning values
    for field in df.dtypes:
        if field[1] == 'string':
            df_clean = df_clean.withColumn(field[0], upper(col(field[0])))
            df_clean = df_clean.withColumn(field[0], trim(col(field[0])))
            df_clean = df_clean.withColumn(field[0], regexp_replace(col(field[0]), "'", ""))
            df_clean = df_clean.withColumn(field[0], regexp_replace(col(field[0]), ",|-|\.", ""))
            df_clean = df_clean.withColumn(field[0], regexp_replace(col(field[0]), 
                                ".*UNKNOWN.*|.*NO PORT.*|INVALID.*|COLLAPSED.*|NO COUNTRY.*", "TO BE DETERMINATED"))
            df_clean = df_clean.fillna("TO BE DETERMINATED", subset=[field[0]])
    
    df_clean = df_clean.fillna(-1)
    
    ### Generate fact data and save in parquet files
    
    dim_country.createOrReplaceTempView("country")
    dim_visa.createOrReplaceTempView("visa")
    dim_mode.createOrReplaceTempView("mode")
    dim_airport.createOrReplaceTempView("airport")
    dim_city.createOrReplaceTempView("city")
    df_clean.createOrReplaceTempView("fact")
    
    
    fact = spark.sql("""
    SELECT 
        
        f.i94yr as yr,
        f.i94mon as mnth,
        c.Code as cty_cntry,
        f.i94res as resdnc_cntry,
        a.local_code as prt,
        f.arrdate as arrvl_dt,
        m.code as arrvl_md,
        ci.state_code as us_stt,
        f.depdate as dprtr_dt,
        f.i94bir as rpndnt_age,
        v.Code as vs_typ_id,
        f.count as cnt,
        f.dtadfile as dt_fl,
        f.visapost as vs_issd_stt,
        f.occup as occptn,
        f.entdepa as arrvl_flg,
        f.entdepd as dprtr_flg,
        f.entdepu as updt_flg,
        f.matflag as arrvl_dprtr_flg,
        f.biryear as brth_yr,
        f.dtaddto as allwd_dt,
        f.gender as gndr,
        f.insnum as ins_nmbr,
        f.airline as arln,
        f.admnum as admssn_nmbr,
        f.fltno as flght_nmbr,
        f.visatype as vs_typ
    FROM fact f inner join country c on f.i94cit = c.Code
         inner join airport a on f.i94port = a.local_code
         inner join mode m on f.i94mode = m.code
         inner join city ci on f.i94addr = ci.state_code
         inner join visa v on f.i94visa = v.Code
    """)
    
    #print(f"Fact Count: {fact.count()}\n")

    fact.write.mode("overwrite").partitionBy('yr', 'mnth', 'us_stt').parquet(f"{output_fact_data}fact_immigration")
    
    print("*****process fact data completed*****\n")
    
    dim_city2.createOrReplaceTempView("cty")

    print("""\n\n
    The following analysis shows us the pattern of cities by race and the number of inhabitants who were born abroad. 
    It is possible that the same culture and the fact that there is a good percentage of inhabitants born abroad, 
    facilitate the attention of immigrants to choose to settle in those places. In addition, see the race of the 
    inhabitants, some can be noted that they belong to the American Indians, who in fact have a history and originate
    from migrants.\n
    """)

    print(spark.sql("""
    SELECT c.city, c.race, count(c.*) foreing_born
    FROM fact f inner join cty c on f.i94addr = c.state_code
    GROUP BY c.city, c.race
    ORDER BY count(c.*) desc
    """).limit(10).toPandas())

    
def process_qa(spark, input_qa):
    """
    It handles the call of the fact table process. 
    
    Parameters
    ----------
    
    spark : SparkSession
            Session object to work with spark instance
            
    input_qa : Dictionary
               Contains the path and the metric for check the threshold data
    """
    
    logging.warning("starting qa data", exc_info=True)

    for key in input_qa:
        temp = spark.read.parquet(input_qa[key]["path"])
        count = temp.count()
        
        if count != input_qa[key]["metric"]:
            logging.warning(f"{key} {count} records were inserted", exc_info=True)
        else:
            logging.error(f"{key} didn't pass the check quantity data: {count} records found", exc_info=True)
    
    logging.warning("process qa completed", exc_info=True)


def main():
    """
    Set de global settings for work and execute the ETL process.
    
    Variables
    ---------
    
    spark : SparkSession
            Create and config the session to work with spark instance
    
    input_stages_data : string
                        Root path for read stages
    
    input_dim_data : dictionary
                     Contains the path of all dimensions that going to be join with the fact table
                     
    output_dim_data : string
                     Root path for write dimensions
    
    output_fact_data : string
                      Root path for write the fact data
    
    input_qa : dictionary
               Contains the path and the metric for check the threshold data
               
    Functions
    ---------
    
    create_spark_session : func
                           Reference to get the spark instance
    
    process_dimensions : func
                         Reference to process ETL Dimensions
    
    process_fact : func
                   Reference to process fact data
    
    process_qa : func
                 Reference to process check data

    """
    
    ## VARIABLES
    spark = create_spark_session()
    
    input_stages_data = "stages/"
    input_dim_data = {"country" : "dimensions/dim_countries", \
                     "visa"     : "dimensions/dim_visa", \
                     "mode"     : "dimensions/dim_modes", \
                     "airport"  : "dimensions/dim_airports", \
                     "city"     : "dimensions/dim_cities"}
    
    output_dim_data = "dimensions/"
    output_fact_data = "facts/"
    
    input_qa = {"dim_countries"   : {"path": "dimensions/dim_countries", "metric": 0}, \
               "dim_visa"         : {"path": "dimensions/dim_visa", "metric": 0}, \
               "dim_modes"        : {"path": "dimensions/dim_modes", "metric": 0}, \
               "dim_airports"     : {"path": "dimensions/dim_airports", "metric": 0}, \
               "dim_city"         : {"path": "dimensions/dim_cities", "metric": 0}, \
               "fact_immigration" : {"path": "facts/fact_immigration", "metric": 0}}
    
    ## RUN ETL
    process_dimensions(spark, input_stages_data, output_dim_data)    
    process_fact(spark, input_dim_data, output_fact_data)
    process_qa(spark, input_qa)

    
if __name__ == "__main__":
    main()