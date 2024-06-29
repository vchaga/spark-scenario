package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
/**
 * Add rowId at the start */
object rowId {
  
  def main(args: Array[String]) : Unit = {
    
    val spark = SparkSession.builder()
    .appName("readData")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val sc = spark.sparkContext
       
    println("*** Parquet format ***")
    val dfParquet1 = spark.read.format("parquet")
    .option("header",true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/parfile.parquet")
    //dfParquet1.show()
    
    val windowSpec = Window.orderBy(monotonically_increasing_id())
    val dfParquet2 = dfParquet1.withColumn("row_id",row_number.over(windowSpec))    
    
    /*
     * Create a sequence allColumns containing the line_number column followed by all other columns.
     * Use the select method with the allColumns sequence mapped to col objects.
     * */
    val allColumns = Seq("row_id") ++ dfParquet2.columns.filter(_ != "row_id")
    val dfParquet3 = dfParquet2.select(allColumns.map(col): _*)
    //dfParquet3.show()
    dfParquet3.printSchema()
    dfParquet2.printSchema()
  }
  
}