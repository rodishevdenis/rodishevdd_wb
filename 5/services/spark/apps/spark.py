import os
import tempfile
import pandas as pd

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.context import SparkContext
from clickhouse_driver import Client

def process(batch_df, batch_id):
    print('batch_id: %s' % batch_id)
    print('')

    df = batch_df.toPandas()
    
    # Добавляем объем и записываем в конечную таблицу.
    with Client.from_url(ch_connection_string) as ch:
        ch.insert_dataframe('insert into SHKonPlace values', df, settings={'use_numpy': True})

# Схема сообщений в топике kafka. Используется при формировании batch.
schema = StructType([
    StructField('shk_id', LongType(), True),
    StructField('dt', TimestampType(), True),
    StructField('employee_id', LongType(), True),
    StructField('place_cod', LongType(), True),
    StructField('tare_id', LongType(), True),
    StructField('tare_type', StringType(), True),
    StructField('isdeleted', BooleanType(), True),
    StructField('isstock', BooleanType(), True),
    StructField('chrt_id', LongType(), True),
    StructField('state_id', LongType(), True),
    StructField('transfer_box_id', LongType(), True),
    StructField('correction_dt', TimestampType(), True),
    StructField('wbsticker_id', LongType(), True),
])
schema = StructType([
    StructField('shk_id', StringType(), True),
    StructField('dt', StringType(), True),
    StructField('employee_id', StringType(), True),
    StructField('place_cod', StringType(), True),
    StructField('tare_id', StringType(), True),
    StructField('tare_type', StringType(), True),
    StructField('isdeleted', StringType(), True),
    StructField('isstock', StringType(), True),
    StructField('chrt_id', StringType(), True),
    StructField('state_id', StringType(), True),
    StructField('transfer_box_id', StringType(), True),
    StructField('correction_dt', StringType(), True),
    StructField('wbsticker_id', StringType(), True),
])

spark_master_url = os.environ.get('SPARK_MASTER_URL', 'clickhouse://admin:admin@clickhouse:9000/spark')
spark_dir = os.environ.get('SPARK_DIR', '/spark')
spark_app_name = os.environ.get('SPARK_APP_NAME', 'spark')

spark_connection_string = "spark://spark:7077"
ch_connection_string = "clickhouse://admin:admin@clickhouse:9000/spark"

conf = SparkConf()
conf = conf.setAll([
    ("spark.master", spark_connection_string),
    ("spark.app.name", spark_app_name),
    ("spark.home", spark_dir),
    ('spark.ui.port', '8081'),
    ('spark.dynamicAllocation.enabled', 'false'),
    ('spark.executor.cores', '1'),
    ('spark.task.cpus', '1'),
    ('spark.num.executors', '1'),
    ('spark.executor.instances', '1'),
    ('spark.default.parallelism', '1'),
    ('spark.cores.max', '1'),
])

ctx = SparkContext(
    batchSize=8,
    conf=conf,
)

# spark session
session = SparkSession(sparkContext=ctx)

# data reader
src = (
    session.readStream
        .format("kafka")
        .options(**{
            "maxOffsetsPerTrigger": "10",
            "startingOffsets": "earliest",
            "failOnDataLoss": "false",
            "forceDeleteTempCheckpointLocation": "true",
            "kafka.bootstrap.servers": "kafka:29092",
            "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="demo" password="demo-password";',
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_PLAINTEXT",
            "kafka.client.id": "spark-consumer",
            "kafka.group.id": "spark-consumer-group",
            "subscribe": "topic",
        })
)

# data frame
with tempfile.TemporaryDirectory() as d:
    data = (
        src
            .load()
            .select(col("value").cast("string").alias("value"))
            .select(from_json(col("value"), schema).alias("json"))
            .select(
                col('json.shk_id').alias('shk_id'),
                col('json.dt').alias('dt'),
                col('json.employee_id').alias('employee_id'),
                col('json.place_cod').alias('place_cod'),
                col('json.tare_id').alias('tare_id'),
                col('json.tare_type').alias('tare_type'),
                col('json.isdeleted').alias('isdeleted'),
                col('json.isstock').alias('isstock'),
                col('json.chrt_id').alias('chrt_id'),
                col('json.state_id').alias('state_id'),
                col('json.transfer_box_id').alias('transfer_box_id'),
                col('json.correction_dt').alias('correction_dt'),
                col('json.wbsticker_id').alias('wbsticker_id'),
            )
    )

# data writer
writer = (
    data.writeStream
        .trigger(processingTime="1 seconds")
        .foreachBatch(process)
        .options(options={})
)

writer.start().awaitTermination()

