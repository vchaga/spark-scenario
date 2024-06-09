package org.spark.learn

import org.apache.spark._
import sys.process._

object rdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rddLearn").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val input = sc.textFile("file:///c:/users/venka/Onedrive/desktop/BD/Data/dt.txt")
    val count = input.flatMap(x=>x.split(","))
    .map(word => (word.toString().toLowerCase(),1))
    .reduceByKey(_+_)
    .saveAsTextFile("Output/rdd_scala_op.txt")
    
  }
}