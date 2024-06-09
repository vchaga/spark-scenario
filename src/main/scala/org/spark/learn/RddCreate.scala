package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/**
 * 1. parallelizing data collection
 * 2. Referencing external data file
 * 
 * Shuffle Operations:
 * Although the set of elements in each partition of newly shuffled data will be deterministic, 
 * and so is the ordering of partitions themselves, the ordering of these elements is not. 
 * If one desires predictably ordered data following shuffle then it’s possible to use:
 *  - mapPartitions to sort each partition using, for example, .sorted
 *  - repartitionAndSortWithinPartitions to efficiently sort partitions while simultaneously repartitioning
 *  - sortBy to make a globally ordered RDD
 */
object RddCreate {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
    .appName("WaysToCreateRDD")
    .master("local[*]")
    .getOrCreate()
    val sc = spark.sparkContext
    
    /**
     * 1.Parallelizing data collection
     * Characters are using ascii code to print
     * Once created, the distributed dataset (distData) can be operated on in parallel
     */
    val data1 = sc.parallelize(List('x',2,4,5,7))
    
    println("=== data1 ===")
    data1.foreach(println)
    
    val List1 = List('x',2,'y',4)
    val data2 = sc.parallelize(List1)
    println("=== data2 ===")
    data2.foreach(println)
    
    
    /**
     * 2.Referencing to external data file
     * Once created, distFile can be acted on by dataset operations. 
     * SparkContext.wholeTextFiles lets you read a directory containing multiple small text files, 
     * and returns each of them as (filename, content) pairs. 
     * This is in contrast with textFile, which would return one record per line in each file
     */
    val data3 = sc.textFile("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv", 2)
    println("=== data3 ===")
    data3.foreach(println)
  }
  
}