package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._

/**
 1. Step (1) & 2 is same as above solution rddCase2.scala
 2. After split data now CONVERT it to ROW rdd
 3. Filter products(column/index 4) element contains “Gymnastics”
 4. Impose(create) schema using StructType()
 5. Convert it into DataFrame
 6. Save result as Parquet
 */
object rddCase2Sol2 {
	def main(args: Array[String]): Unit = {

			val spark = SparkSession.builder()
					.appName("rddCase2Sol2")
					.master("local[*]")
					.getOrCreate()

					val sc = spark.sparkContext

					val input = sc.textFile("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv",2)
					println(input.getClass())
					val split = input.map(x => x.split(","))
					/* Row(...): This creates a new Row object. 
					 * In Scala, Row is a class provided by Spark that represents a row of data in a DataFrame. */
					val rowRdd = split.map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5)))
					println("--->",rowRdd.getClass)
					rowRdd.foreach(println)

					val schema = StructType(Array(
							StructField("id",StringType,true),
							StructField("tdate",StringType,true),
							StructField("amount",StringType,true),
							StructField("category",StringType,true),
							StructField("product",StringType,true),
							StructField("spendby",StringType,true)
							)
							)
							
					val df = spark.createDataFrame(rowRdd, schema)
					println("=== DF changes ===")
					df.show()
					
					df.createOrReplaceTempView("filterTable")
					val filterData = spark.sql("select * from filterTable where tdate = '12-17-2011' ")
					println("=== fiterData ===")
					filterData.show()
					
					df.coalesce(1).write.mode("overwrite").parquet("Output/rddCase2")

	}
}