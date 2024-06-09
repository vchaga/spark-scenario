package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 1. Read data from the file and save it in variable
 2. Create SQL view (the view is like a table for temporary to execute options that should
 not affect the original table)
 3. Run SQL command using spark.sql(command) on that view just like a table.
 */

object sql {
  
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
    .appName("SparkSql")
    .master("local[*]")
    .getOrCreate()
    
    val df = spark.read.format("json")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/devices.json")
    
    df.show()
    
    df.createOrReplaceTempView("table")
    
    val finaldf = spark.sql("select * from table where temp = 30")
    finaldf.show()
  }
}