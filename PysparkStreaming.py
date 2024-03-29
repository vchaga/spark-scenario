from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql.types import *
# Delete the checkpoint files
import shutil

spark = SparkSession.builder.master("local").appName("structuredStream").getOrCreate()
sc = spark.sparkContext

# sql adaptive query execution adaptive.coalescePartitions.enabled will makr paritions dynamic
sc.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions",3)
spark.conf.get("spark.sql.shuffle.partitions")

#define schema
input_schema = StructType([
    StructField("StockId",StringType(),True),
    StructField("StockName",StringType(),True),
    StructField("Price",DoubleType(),True),
    StructField("Quantity",IntegerType(),True),
    StructField("Date",DateType(),True)
])

shutil.rmtree("checkpoint")

while True:
    try:
        #Create streaming dataframe by reading data from socket
        df = spark.readStream.format("csv").schema(input_schema).option("header","True") \
        .option("maxFilesperTrigger",1) \
        .load("file:///C:/Users/venka/PycharmProjects/pythonProject/dataset/StreamData/*") \
        .withColumn("fileName",element_at(split(input_file_name(), "/"),-1)) \
        .withColumn("timestamp",current_timestamp())

        #Do some transformations on readStream
        df2 = df.groupBy("fileName","Date","timestamp") \
                .agg(sum(col("Quantity")*col("Price")))

        #.trigger(Trigger.ProcessingTime("5 seconds"))

        #writeStream.start(), 3 modes in ouput - complete, append, update
        #if using the .trigger() with processing time argument directly works to set
        #the streaming trigger interval in your version of Spark
        stream = df2.writeStream \
                .outputMode("complete") \
                .format("console") \
                .option("truncate",False) \
                .option("checkPointLocation","checkpoint") \
                .trigger(processingTime='5 seconds') \
                .start()

        #stream.awaitTermination()
    except Exception as e:
        print (f" An error occured {e}")

#print(f"Streaming methods available {dir(stream)}")
# Stop the current streaming query
stream.stop()