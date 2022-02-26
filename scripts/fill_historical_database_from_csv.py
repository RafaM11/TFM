import os
import sys

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType

currentdir = os.path.abspath(os.path.dirname(__file__))
parentdir = os.path.abspath(os.path.join(currentdir, os.pardir))

sys.path.insert(0, parentdir + '\\' + 'utils')

filename = 'DOT_Traffic_Speeds_NBE.csv'
connector_name = 'postgresql-42.3.1.jar'

credentials = config(section = 'postgresql')

filepath = parentdir + '\\' + 'data' + '\\' + filename # File is not included in repository app (it takes lot of GB!)
connector_path = parentdir + '\\' + 'utils' + '\\' + connector_name

try:
    from utils.misc_utils import config
except ModuleNotFoundError:
    from misc_utils import config

insert_data = False

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('fill_historical_database_from_csv') \
        .master('local[4]') \
        .config('spark.driver.extraClassPath', connector_path) \
        .config('spark.sql.shuffle.partitions', 4) \
        .getOrCreate()

    df = spark.read.format('csv').option('header', 'true').load(filepath)

    df = df.filter((f.col('STATUS') != '-101') & (f.col('SPEED') > 0) & (f.col('TRAVEL_TIME') > 0))

    df = df.withColumn('ID', f.col('ID').cast(IntegerType())) \
           .withColumn('SPEED', f.col('SPEED').cast(DoubleType())) \
           .withColumn('TRAVEL_TIME', f.col('TRAVEL_TIME').cast(IntegerType())) \
           .withColumn('DATA_AS_OF', f.to_timestamp(f.col('DATA_AS_OF'), format = 'MM/dd/yyyy KK:mm:ss a'))

    df = df.dropna(subset = 'DATA_AS_OF')

    columns_to_drop = 'STATUS', 'LINK_ID', 'LINK_POINTS', 'ENCODED_POLY_LINE', 'ENCODED_POLY_LINE_LVLS', 'OWNER', 'TRANSCOM_ID', 'BOROUGH', 'LINK_NAME'
    df = df.drop(*columns_to_drop)

    df = df.withColumnRenamed('ID', 'link_id') \
           .withColumnRenamed('SPEED', 'speed') \
           .withColumnRenamed('TRAVEL_TIME', 'travel_time') \
           .withColumnRenamed('DATA_AS_OF', 'measurement_date')

    df.show(truncate = False)
    
    if insert_data:
       table = 'tb_historical_traffic'
       df.write.jdbc(url = credentials['url'], table = table, properties = {'user': credentials['user'], 'password': credentials['password']}, mode = 'append')