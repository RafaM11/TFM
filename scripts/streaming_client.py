import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
import os
import sys

currentdir = os.path.abspath(os.path.dirname(__file__))
parentdir = os.path.abspath(os.path.join(currentdir, os.pardir))

sys.path.insert(0, parentdir + '\\' + 'utils')

try:
    from utils.misc_utils import config
    from utils.db_utils import DBConnection
except ModuleNotFoundError:
    from misc_utils import config
    from db_utils import DBConnection

credentials = config(section = 'postgresql')

real_time_traffic_temp_path = parentdir + '\\' + 'data' + '\\' + 'temp_real_time_traffic_data'

connector_name = 'postgresql-42.3.1.jar'
connector_path = parentdir + '\\' + 'utils' + '\\' + connector_name

table = 'tb_real_time_traffic'

for file in os.listdir(real_time_traffic_temp_path):
    os.remove(real_time_traffic_temp_path + '//' + file)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('Parse real time') \
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
    
    df = spark.readStream.json(path = real_time_traffic_temp_path, schema = schema)

    df = df.withColumn('id', f.col('id').cast(IntegerType())) \
           .withColumn('speed', f.col('speed').cast(DoubleType())) \
           .withColumn('travel_time', f.col('travel_time').cast(IntegerType()))

    df = df.filter((f.col('status') != '-101') & (f.col('speed') > 0) & (f.col('travel_time') > 0))
    df = df.drop('status')

    df = df.withColumnRenamed('id', 'link_id') \
           .withColumnRenamed('data_as_of', 'measurement_date')

    def foreach_batch_function(df, epoch_id):
        sequence = 'tb_real_time_traffic_id_measurement_seq'
        restart_autoincremental = f'ALTER SEQUENCE {sequence} RESTART WITH 1;'
       
        conn = DBConnection().connect()
        with conn.cursor() as cursor:
            cursor.execute(restart_autoincremental)
        conn.commit()
        conn.close()

        df.write.option('truncate', 'true').jdbc(url = credentials['url'], table = table, properties = {'user': credentials['user'], 'password': credentials['password']}, mode = 'overwrite')
        print('Send data to database.')

    stream = df.writeStream \
               .outputMode('update') \
               .format('console') \
               .foreachBatch(foreach_batch_function) \
               .start()

    stream.awaitTermination()
    stream.stop()