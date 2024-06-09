package org.spark.learn

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object word {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("wordCount")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val input = sc.textFile("file:///c:/users/venka/Onedrive/desktop/BD/Data/dt.txt", 2)
    val count = input.flatMap(x => x.split(","))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("Output/wordCount_op.txt")

  }

}