package org.spark.learn

/**
 * flatMap,Map,Filter
 */
object transformations {
  def main(args: Array[String]): Unit = {
    var list = List(
      "Amazon-Jeff-America",
      "Microsoft-BillGates-America",
      "TCS-TATA-india",
      "Reliance-Ambani-INDIA")
      println("String List" + list)
      
      var newList = list.filter(x=>x.toLowerCase().contains("india"))
          .flatMap(x=>x.split("-"))
          .map(x=>x.replace("india","home"))
          .map(x=>x.concat(" country"))
      newList.foreach(println)
  }
}