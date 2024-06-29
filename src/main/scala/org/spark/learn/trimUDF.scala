package org.spark.learn

import org.apache.spark.sql.{SparkSession, Column, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction

object TrimUDF {

	def main(args: Array[String]): Unit = {

			val spark = SparkSession.builder()
					.appName("withColumn")
					.master("local[*]")
					.getOrCreate()

					import spark.implicits._

					// Read CSV file into DataFrame
					val dfCsv = spark.read.format("csv")
					.option("header", true)
					.csv("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv")

					// Define and register the UDF
					val trimStringUDF: UserDefinedFunction = udf((s: String) => {
						if (s == null) null else s.trim
					})

					spark.udf.register("trimString", trimStringUDF)

					// Apply the UDF to each string column in the dataset
					val trimmedDf = dfCsv.columns.foldLeft(dfCsv) { (df, colName) =>
					df.schema(colName).dataType match {
					case StringType => df.withColumn(colName, trimStringUDF(col(colName)))
					case _ => df
					}
			}

			// Show final schema and data
			trimmedDf.printSchema()
			trimmedDf.show()

	}
}
