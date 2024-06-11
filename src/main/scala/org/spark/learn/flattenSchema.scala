package org.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, functions => F }

object flattenSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("flattenSchema")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val dfParquet = spark.read.format("csv")
      .option("header", true)
      .parquet("file:///c:/users/venka/Onedrive/desktop/BD/Data/parfile.parquet")

    dfParquet.show()

    // Construct a struct column from the address section
    val dfWithAddressStruct = dfParquet.withColumn("address_struct", F.struct(
      F.col("address"),
      F.col("city"),
      F.col("county"),
      F.col("state"),
      F.col("zip")))

    // Select required columns, including the new struct column
    val dfParStruct = dfWithAddressStruct.select(
      F.col("first_name"),
      F.col("last_name"),
      F.col("company_name"),
      F.col("address_struct"),
      F.col("age"),
      F.col("phone1"),
      F.col("phone2"),
      F.col("email"),
      F.col("web"))

    //dfParStruct.write.json("file:///c:/users/venka/Onedrive/desktop/BD/Data/parfile.json")

    val dfJson = spark.read.json("file:///c:/users/venka/Onedrive/desktop/BD/Data/parfile1.json")

    dfJson.show()

    // Function to flatten the DataFrame
    /*def flattenSchema(df: DataFrame): DataFrame = {
      val fields = df.schema.fields
      val fieldNames = fields.map(_.name)

      println("Fields: " + fields.mkString(", ")) // Debug statement to print fields
      println("Field Names: " + fieldNames.mkString(", ")) // Debug statement to print field names

      //Iterate over the schema fields. If a field is of type StructType,
      //iterate over its fields and flatten them by using the col function with appropriate aliases.
      val flattenedFields = fields.flatMap(f => {
        f.dataType match {
          case structType: StructType =>
            structType.fields.map(structField =>
              col(s"${f.name}.${structField.name}").alias(s"${f.name}_${structField.name}"))
          case _ => Seq(col(f.name))
        }
      })


      df.select(flattenedFields: _*)
    }*/

    // Function to flatten the DataFrame
    def flattenSchema(df: DataFrame): DataFrame = {
      var dfToFlatten = df
      var doContinue = true

      while (doContinue) {
        val fields = dfToFlatten.schema.fields
        println("Fields: " + fields.mkString(", ")) // Debug statement to print fields

        val fieldNames = fields.map(_.name)
        println("Field Names: " + fieldNames.mkString(", ")) // Debug statement to print field names

        val flattenedFields = fields.flatMap(f => {
          f.dataType match {
            case structType: StructType =>
              structType.fields.map(structField => col(s"${f.name}.${structField.name}").alias(s"${f.name}_${structField.name}"))
            case arrayType: ArrayType =>
              // Explode the array and create columns for its elements
              Seq(explode_outer(col(f.name)).alias(f.name))
            case _ => Seq(col(f.name))
          }
        })

        // Select the flattened fields
        dfToFlatten = dfToFlatten.select(flattenedFields: _*)

        // Check if any struct or array type is still present
        doContinue = dfToFlatten.schema.fields.exists(f => f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType])
      }

      dfToFlatten
    }

    // Flatten the DataFrame
    val flattenedDF = flattenSchema(dfJson)

    // Show the flattened DataFrame schema
    flattenedDF.printSchema()

    // Show the flattened DataFrame
    flattenedDF.show(false)

    flattenedDF.write.option("header",true)
    .csv("file:///c:/users/venka/Onedrive/desktop/BD/Data/parfile2.csv")

  }

}