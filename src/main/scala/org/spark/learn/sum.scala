package org.spark.learn

object sum {
  def main(args:Array[String]):Unit = {
    println(sum(1,2))
    println(sum(2,2))
  }
  def sum(x:Int,y:Int):Int = {
    return if (x==y) (x+y)*3 else (x+y)
  }
}