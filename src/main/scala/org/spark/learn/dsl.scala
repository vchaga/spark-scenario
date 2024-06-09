package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.array_contains

/**
 * Spark DSL (Domain Specific Language)
 * 1. Filters with column conditions and operations
 *    Sql Expression 
 *    Array Condition
 *    Struct Condition
 * 2. withColumn use case
 * 3. DSL Functions Queries
 * 4. Joins
 * 5. Window functions
 *    Processing complex data
 *    Generating complex data
 *    Tasks
 */

object dsl {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
    .appName("filterDSL")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val csvdf = spark.read.format("csv")
    .option("header",value = true)
    .load("file:///c:/users/venka/Onedrive/desktop/BD/Data/dt.txt")
    
    csvdf.show()
    
    println("====== Or '||' filter =======")
    csvdf.filter(col("category") === "Exercise"
        || col("spendby") === "Cash").show()
        
    println("====== And '&&' filter ======")
    csvdf.filter(col("category") === "Exercise"
        && col("spendby") === "cash").show()
        
    println("====== multi value filter ======")
    csvdf.filter(col("category") isin ("Exercise","Gymnastics")).show()
    
    print("====== not in multi value filter ======")
    csvdf.filter(!(col("category") isin ("Exercise", "Gymnastics"))).show()
    
    print("====== like operator using filter ======")
    csvdf.filter(col("category") like "%Gymnastics%").show()
    
    print("====== 'null' value filter ======")
    csvdf.filter(col("id") isNull ).show()
    
    print("====== not 'null' value filter ======")
    csvdf.filter(col("id") isNotNull ).show()
    
    println("====== Sql Expression ======")
    csvdf.filter("amount == '200'").show(false)
    
    println("====== Array condition - array_contains() is inbuilt SQL pack function ======")
    //csvdf.filter(array_contains(csvdf("category"),"Gymnastics")).show(false)
    
    println("====== Struct condition ======")
    //csvdf.filter(csvdf("category.name") === "Williams").show(false)
    
    /*Few altenartives for filter the column where category is column name*/
    println("====== #1 ======")
    //$"column_name" syntax for referencing columns in a DataFrame
    csvdf.filter($"category" === "Gymnastics").show() //spark.implicits._
    println("====== #2 ======")
    csvdf.filter(col("category") === "Gymnastics").show()
    
    println("====== #3 ======")
    csvdf.where(col("category") === "Gymnastics").show()
    println("====== #4 ======")
    csvdf.where($"category" === "Gymnastics").show()
    println("====== #5 ======")
    csvdf.where(csvdf("category") === "Gymnastics").show()
  }
  
}