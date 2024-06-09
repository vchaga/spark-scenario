package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.util._
import sys.process._

object dflearn {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder()
      .master("local[1]")
      .appName("dflearn")
      .getOrCreate()
    var sc = spark.sparkContext
    val input = spark.read.csv("file:///c:/users/venka/Onedrive/desktop/BD/BDdataset.csv")
    input.show()

  }
}