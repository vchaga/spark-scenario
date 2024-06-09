package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.util._
import sys.process._

/**
 * 
Read the data file (data.csv)
Filter rows greater than which length > 200
Flatten(split) the Filtered data with comma
Replace hyphen with Nothing (Remove the Hyphen)
Concat string →",zeyo" at the End of each element
Store result in a file
*/
object rddCase1 {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
    .master("local[*]")
    .appName("rddcase")
    .getOrCreate()
    val sc = spark.sparkContext
    val input = spark.read.csv("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv")
    val df = input.filter(x => x.length > 5).rdd
    .flatMap(x => x.toString().split(","))
    .map(x => x.replace("-",""))
    .map(x => x.toString().concat("->zeyo"))
    df.foreach(println)
    
  }
}