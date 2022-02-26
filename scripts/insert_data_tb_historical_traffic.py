import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
import os
import sys

currentdir = os.path.abspath(os.path.dirname(__file__))
parentdir = os.path.abspath(os.path.join(currentdir, os.pardir))

sys.path.insert(0, parentdir + '\\' + 'utils')

try:
    from utils.misc_utils import config, credentials_path
except ModuleNotFoundError:
    from misc_utils import config, credentials_path

credentials = config(filename = credentials_path, section = 'postgresql')

connector_name = 'postgresql-42.3.1.jar'
connector_path = parentdir + '\\' + 'utils' + '\\' + connector_name

historical_traffic_temp_path = parentdir + '\\' + 'data' + '\\' + 'temp_historical_traffic_data'

table = 'tb_historical_traffic'

insert_data = True

for file in os.listdir(historical_traffic_temp_path):
    os.remove(historical_traffic_temp_path + '//' + file)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('insert_data_tb_historical_traffic') \
        .master('local[4]') \
        .config('spark.driver.extraClassPath', connector_path) \
        .config('spark.streaming.stopGracefullyOnShutdown', 'true') \
        .config('spark.sql.shuffle.partitions', 4) \
        .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true') \
        .getOrCreate()

    schema = StructType([StructField(name = 'id', dataType = StringType(), nullable = False), \
                         StructField(name = 'speed', dataType = StringType(), nullable = False), \
                         StructField(name = 'travel_time', dataType = StringType(), nullable = False), \
                         StructField(name = 'status', dataType = StringType(), nullable = False), \
                         StructField(name = 'data_as_of', dataType = TimestampType(), nullable = False)])
    
    df = spark.readStream.json(path = historical_traffic_temp_path, schema = schema)

    df = df.withColumn('id', f.col('id').cast(IntegerType())) \
           .withColumn('speed', f.col('speed').cast(DoubleType())) \
           .withColumn('travel_time', f.col('travel_time').cast(IntegerType()))

    df = df.filter((f.col('status') != '-101') & (f.col('speed') > 0) & (f.col('travel_time') > 0))
    df = df.drop('status')

    df = df.withColumnRenamed('id', 'link_id') \
           .withColumnRenamed('data_as_of', 'measurement_date')

    def foreach_batch_function(df, epoch_id):
        if insert_data:
            df.write.jdbc(url = credentials['url'], table = table, properties = {'user': credentials['user'], 'password': credentials['password']}, mode = 'append')
            print('Send data to database.')

    stream = df.writeStream \
               .outputMode('update') \
               .format('console') \
               .foreachBatch(foreach_batch_function) \
               .start()

    stream.awaitTermination()
    stream.stop()