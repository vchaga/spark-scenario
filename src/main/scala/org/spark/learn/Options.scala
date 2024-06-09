package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
/**
 * 1. header - will enable headers to display in the table otherwise, it will be
 treated as one of the rows of data
   2. Delimiter - it will split data from that ‘~’ into column
   3. multiline - In order to process the JSON/CSV file with values in rows
 scattered across multiple lines, use option("multiLine", true)
 */
object Options {
  def main(args: Array[String]):Unit = {
    
    val spark = SparkSession.builder()
    .appName("Options")
    .master("local[*]")
    .getOrCreate()
      
    val csvdf = spark.read.format("csv")
    .option("header",true)
    .option("delimiter","~")
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/allc.delTilde.csv")
    
    csvdf.show()
    
    val multilinedf = spark.read.format("json")
    .option("header",true)
    .option("multiline",true) //will receive an error for false
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/pic.json")
    
    multilinedf.show()
    
  }
}