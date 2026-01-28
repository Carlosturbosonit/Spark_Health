package com.sparkhealth

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FluviewETL {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "input_path output_path")

    val spark = SparkSession.builder()
      .appName("Fluview ETL")
      .getOrCreate()

    val input = spark.read.json(args(0))
    val cleaned = input.selectExpr("*")

    cleaned.write.mode("overwrite").parquet(args(1))

    spark.stop()
  }
}
