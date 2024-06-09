package org.spark.learn

import org.apache.spark.sql.SparkSession
import java.sql.Date
import java.text.SimpleDateFormat


/**
 * Solution 1 using schema rdd
 1. Read data from text file
 2. Split each column with comma like below
 3. Impose(create) schema using case class
 4. Convert it into schemaRDD
 5. Filter products(column/index 4) element contains “Gymnastics”
 6. Convert it into DataFrame
 7. Save result as Parquet
 */

/*case class schema(id: Int , tdate : Date , 
    amount: Float ,category: String ,product: String ,spendby: String) */
case class schema(id: String , tdate : String , 
    amount: String ,category: String ,product: String ,spendby: String)
object rddCase2 {
  
  def main(args:Array[String]) : Unit = {
    val spark = SparkSession.builder()
    .master("local[*]")
    .appName("rddCase2")
    .getOrCreate()
    
    /* To convert an RDD of case class objects to a DataFrame in Spark, 
     * you need to import implicits and use the toDF() method.  */
    import spark.implicits._ 
    
    val sc = spark.sparkContext
    val data = sc.textFile("file:///c:/users/venka/Onedrive/desktop/BD/Data/df.csv", 2)
    val splitData = data.map(x => x.split(","))
    val dateFormat = new SimpleDateFormat("MM-dd-yyyy")
    //val dateFmt = splitData.map(x=> dateFormat.parse(x(1)).getTime())
    val schemaRdd = splitData.map( x => schema(x(0),x(1),x(2),x(3),x(4),x(5)))
    //val schemaRdd = splitData.map( x => schema(x(0).toInt,dateFmt,x(2).toFloat,x(3),x(4),x(5)))
    .filter(x => x.product.contains("Gymnastics"))
    println("==== schemaRdd result ====")
    schemaRdd.foreach(println)
    
    //createOrReplaceTemp View
    val df = schemaRdd.toDF()
    df.createOrReplaceTempView("filterTable")
    val dfResult = spark.sql("select * from filterTable where tdate = '06-26-2011'")
    println("==== DF result ====")
    dfResult.show()
    
    df.coalesce(1).write.mode("overwrite").parquet("Output/rddCase2")
  }
    
}