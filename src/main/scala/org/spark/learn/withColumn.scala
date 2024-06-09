package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

/**
 * 1. Perform operations on a single column without altering other columns.
 *    Using selectExpr(), we must specify all column names present in the table
 *    along with the expression column. With withColumn(), we can simplify this process.
 * 2. If the colName value 'column' exists in the table, the expression value will override it.
 *    Otherwise, if the column does not exist but the expression is valid, a new column
 *    will be created at the end of the table with the expression's value.
 */

object withColumn {
  
  def main(args: Array[String]):Unit = {
    
    val spark = SparkSession.builder()
    .appName("withColumn")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val sc = spark.sparkContext
    
    val dfCsv = spark.read.format("csv")
    .option("header",true)
    .csv("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv")
    
    println("=== Using withColumn, select only year from date, withcolumnRenamed")
    dfCsv.withColumn("tdate",expr("element_at(split(tdate,'-'),3)"))
    .withColumnRenamed("tdate","year")
    .withColumn("status",expr("case when spendby = 'cash' then 0 else 1 end"))
    .show()
    
    println("#2")
    dfCsv.withColumn("tdate",expr("split(tdate,'-')[2]")).show()
    
    println("non existent column")
    dfCsv.withColumn("year",expr("split(tdate,'-')[2]")).show()
    
    println("===Using selectExpr ====")
    dfCsv.selectExpr("id"
        ,"split(tdate,'-')[2] as year"
        ,"amount"
        ,"concat(category,', done')"
        ,"product").show()
  }
}