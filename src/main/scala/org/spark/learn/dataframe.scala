package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._

/**
 * 1. Create DataFrame from RDD
 * 2. Using createDataFrame() with the Row type
 * 3. Using createDataFrame() from SparkSession
 */
object dataframe {
	def main(args: Array[String]): Unit = {

			val spark = SparkSession.builder()
					.appName("createDF")
					.master("local[*]")
					.getOrCreate()
					val sc = spark.sparkContext
					import spark.implicits._ //import from sql context

					val columns = Seq("language", "users_count")
					val data = Seq(("java", "20000"), ("Python", "10000"), ("Scala", "3000"))
					//1.Create RDD
					val rdd = sc.parallelize(data)
					val df1 = rdd.toDF(columns: _*) // Pass columns as individual arguments
					df1.show()

					//2.createDataFrame with the Row type
					// Convert Seq[Row] to RDD[Row]
					val rowRDD = sc.parallelize(data)
					val rowData = rowRDD.map(x => Row(x._1, x._2))
					rowData.foreach(println)
					println(rowData.getClass())
					val schema = StructType(Array(
							StructField("language", StringType, true),
							StructField("users_count", StringType, true)))
					val df2 = spark.createDataFrame(rowData, schema)
					println("=== df2 ===")
					df2.show()

					//3.createDataFrame from SparkSession
					val rowRDD2 = sc.parallelize(data)
					val df3 = spark.createDataFrame(rowRDD2).toDF(columns: _*)
					println("=== Method3 ===")
					df3.show()

	}
}