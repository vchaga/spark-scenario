package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * Create Partitioned data using scala
 * 1.Read file form Local
 * 2.Data Partition by 'state'
 * 3.Data Partition by 'state' and "county"
 */
object writePartition {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
    .appName("writePartitionData")
    .master("local[*]")
    .getOrCreate()
    
    val sc=spark.sparkContext
    
    val df = spark.read.format("orc")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/orcfile.orc")
    
    df.show()
    
    df.write.format("csv")
    .partitionBy("state")
    .mode(SaveMode.Overwrite)
    .save("Output/writeParititon")
    
    df.write.format("csv")
    .partitionBy("state","county")
    .mode(SaveMode.Overwrite)
    .save("Output/writeParititon2")
  }
}