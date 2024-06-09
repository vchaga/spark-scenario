package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/**
 * Two ways to write files
 * 1.Use specifc write format 
 * 2.Use generic mode to write files
 *  There are 4 modes → error(default), append, overwrite, ignore
 *  overwrite - Write new data in existing folder but remove all old data files
 *  append -  Write/Add new data in existing folder but also keep all old data
 *  ignore - Write new data in folder but if folder already exists don’t write data
 *  error - Throw an exception if data already exists.
 */
object writeData {

	def main(args: Array[String]): Unit =  {

			val spark = SparkSession.builder
					.appName("writeData")
					.master("local[*]")
					.getOrCreate()

					import spark.implicits._

					val sc = spark.sparkContext

					val dfOrc = spark.read
					.format("orc")
					.option("header",true)
					.load("file:///c:/users/venka/Onedrive/desktop/BD/Data/orcfile.orc")

					dfOrc.show()
					dfOrc.createOrReplaceTempView("filterTable")
					val filterData = spark.sql("select * from filterTable where age > 10")
					
					/* 1st way to write data */
					filterData.write.mode("overwrite").orc("Output/writeDataOrc")
					
					/*second way to write data */
					filterData.write.format("orc").mode("overwrite")
					.save("Output/writeDataOrc2")
					
					filterData.write.format("orc").mode("append")
					.save("Output/writeDataOrc2")
					
					filterData.write.format("orc").mode("ignore")
					.save("Output/writeDataOrc2")

	}

}