package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
/**
 * There are two ways to read file and create dataframe
 * 1.Use specific file format
 * 2.Generic file source options
 */
object readData {
  
  def main(args: Array[String]) : Unit = {
    
    val spark = SparkSession.builder()
    .appName("readData")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val sc = spark.sparkContext
    
    val dfCsv = spark.read.csv("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv")
    val dfTxt = spark.read.text("file:///c:/users/venka/Onedrive/desktop/BD/Data/dtnew.txt")
    val dfJson = spark.read.json("file:///c:/users/venka/Onedrive/desktop/BD/Data/devices.json")
        
    println("=== CSV format === ")
    dfCsv.show()
    
    println("=== Txt format === ")
    dfTxt.show()
    
    println("=== Json format === ")
    dfJson.show()
    
    /**
     * Read using generic file source options
     */
    println("*** CSV format ***")
    val dfCsv1 = spark.read.format("csv")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv")
    dfCsv1.show()
    
    println("*** Json format ***")
    val dfJson1 = spark.read.format("json")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/devices.json")
    dfJson1.show()
    
    println("*** Orc format ***")
    val dfOrc1 = spark.read.format("orc")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/orcfile.orc")
    dfOrc1.show()
    
    println("*** Parquet format ***")
    val dfParquet1 = spark.read.format("parquet")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/parfile.parquet")
    dfParquet1.show()
    
    println("*** XML format ***")
    //need to add jar or dependency –>spark-xml_2.12
    val dfXml1 = spark.read.format("xml")
    //.option("rootTag",true)
    .option("rowTag","txndata")
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/file6.xml")
    dfXml1.show()
    
    println("*** Avro format ***")
    //need to add jar or dependency –>spark-avro_2.12
    val dfAvro1 = spark.read.format("avro")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/part.avro")
    dfAvro1.show()
  }
  
}