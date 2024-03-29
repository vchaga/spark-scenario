"""
Only use this command to change package information during pyspark shell launch. This cell is not working
"""
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = \
 '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.master("local").appName("kafka stream").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc,60)
## Create a local StreamingContext with two working thread and batch interval of 60 second

# Create a DStream that will connect to hostname:port, like localhost:9999
#lines = ssc.socketTextStream("localhost", 9999)

# sql adaptive query execution adaptive.coalescePartitions.enabled will makr paritions dynamic
sc.setLogLevel("Error")
#spark.conf.set("spark.sql.shuffle.partitions",3)
#spark.conf.get("spark.sql.shuffle.partitions")
#spark.conf.set("spark.sql.adaptive.enabled","false")
#spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","false")
#spark.conf.set("spark.sql.execution.arrow.enabled",True)
#spark.conf.set("spark.sql.execution.arrow.fallback.enabled",True)

"""
Kafka integration
InPyspark - As integration between spark and kafka is lower, use 60 sec as interval
"""

#WithoutInferSchema
#headerTrue will read first row and assign column names but type is String for all
#spark job created to read first column
filepath = "file:///C:/Users/venka/PycharmProjects/pythonProject/dataset/"

#message = kafkaUtils.createDirectStream(ssc,topics=['testtopic'],
#                                       kafkaParams={"message.broker.list":"localhost:9092"})

streaming_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "testtopic") \
  .load()

#ds = df \
#  .selectExpr("CAST(key AS STRING)") \
#  .writeStream \
#  .format("kafka") \
#  .option("kafka.bootstrap.servers", "localhost:9092") \
#  .option("topic", "testtopic") \
#  .start()

words = streaming_df.selectExpr("CAST(value AS STRING)")

word_df = words.select(explode(split(col("value"), " ")).alias("word"))

# Perform word count
word_count = word_df.groupBy("word").count()

# Write the word count DataFrame to the console
query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
