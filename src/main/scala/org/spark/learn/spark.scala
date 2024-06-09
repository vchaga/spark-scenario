package org.spark.learn

import org.apache.spark._
import sys.process._

object spark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("firstspark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///c:/users/venka/Onedrive/desktop/BD/BDdataset.csv")
    /*
    val gymdata = data.filter(x => x.contains("King"))
    "hadoop fs -rmr /user/cloudera/datag".!
    gymdata.saveAsTextFile("/user/cloudera/datag")
    println("===done===")
    * 
    */
    data.take(5).foreach(println)
    
    println()
    data.filter(x => x.toLowerCase().contains("royal")).foreach(println)
    
  }

}