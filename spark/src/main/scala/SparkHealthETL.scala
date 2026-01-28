package com.sparkhealth

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

/*
OBJECTIVE
---------
This Spark job performs the "SILVER" transformation step for FluView data.

INPUT  (Bronze / Raw):
- JSON files produced by Airflow ingestion:
  - fluview_ili.json
  - fluview_clinical.json

OUTPUT (Silver / Clean):
- Cleaned, normalized Parquet datasets:
  - ili/
  - clinical/

This job is executed by Airflow using spark-submit.
*/

object SparkHealthETL {

  /*
  Config class to capture arguments passed by Airflow:
  --mode   : which transformation to run (clean, join, etc.)
  --raw    : raw input path
  --clean  : output clean path
  */
  case class Config(
    mode: String = "",
    raw: String = "",
    clean: String = ""
  )

  /*
  Parses command-line arguments passed by SparkSubmitOperator
  Example:
    --mode clean
    --raw /opt/airflow/data/raw/fluview/ingest_date=2026-01-27
    --clean /opt/airflow/data/clean/fluview/ingest_date=2026-01-27
  */
  def parseArgs(args: Array[String]): Config = {
    args.sliding(2, 2).foldLeft(Config()) {
      case (c, Array("--mode", v))  => c.copy(mode = v)
      case (c, Array("--raw", v))   => c.copy(raw = v)
      case (c, Array("--clean", v)) => c.copy(clean = v)
      case (c, _) => c
    }
  }

  def main(args: Array[String]): Unit = {

    // Parse and validate arguments
    val config = parseArgs(args)
    require(config.mode.nonEmpty, "--mode required")
    require(config.raw.nonEmpty, "--raw required")
    require(config.clean.nonEmpty, "--clean required")

    // Create Spark session
    val spark = SparkSession.builder()
      .appName(s"spark-health-${config.mode}")
      .getOrCreate()

    // Dispatch transformation based on mode
    config.mode match {
      case "clean" =>
        runClean(spark, config.raw, config.clean)

      case other =>
        throw new IllegalArgumentException(s"Unknown mode: $other")
    }

    spark.stop()
  }

  /*
  CLEAN TRANSFORMATION
  --------------------
  1. Reads raw JSON files (ILI + Clinical)
  2. Adds ingestion timestamp
  3. Removes duplicates
  4. Writes Parquet output partitioned by dataset
  */
  def runClean(spark: SparkSession, rawPath: String, cleanPath: String): Unit = {

    // Read raw datasets
    val ili = spark.read.json(s"$rawPath/fluview_ili.json")
    val clinical = spark.read.json(s"$rawPath/fluview_clinical.json")

    // Basic cleaning and normalization
    val cleanedIli = ili
      .withColumn("ingest_ts", current_timestamp()) // lineage & auditing
      .dropDuplicates()                             // data quality

    // Write Silver layer
    cleanedIli.write
      .mode("overwrite")
      .parquet(s"$cleanPath/ili")

    clinical.write
      .mode("overwrite")
      .parquet(s"$cleanPath/clinical")
  }
}

