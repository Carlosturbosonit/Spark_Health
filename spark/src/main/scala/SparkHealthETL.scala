package com.sparkhealth

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
OBJECTIVE
---------
This Spark job performs the SILVER transformation step for FluView data.

INPUT (Bronze / Raw)
--------------------
JSON files produced by the Airflow ingestion DAG:
- fluview_ili.json
- fluview_clinical.json

Example input path:
  /opt/airflow/data/raw/fluview/ingest_date=2026-01-27/

OUTPUT (Silver / Clean)
----------------------
Cleaned and normalized Parquet datasets:
- ili/
- clinical/

Example output path:
  /opt/airflow/data/clean/fluview/ingest_date=2026-01-27/

EXECUTION
---------
Triggered by Airflow using SparkSubmitOperator:
spark-submit --class com.sparkhealth.SparkHealthETL \
  spark-health_2.12-0.1.0.jar \
  --mode clean \
  --raw <raw_path> \
  --clean <clean_path>
*/

object SparkHealthETL {

  /*
  Configuration model capturing parameters passed by Airflow
  */
  case class Config(
    mode: String = "",
    raw: String = "",
    clean: String = ""
  )

  /*
  Parses command-line arguments of the form:
    --mode clean
    --raw /path/to/raw
    --clean /path/to/clean
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

    // -----------------------
    // Argument validation
    // -----------------------
    val config = parseArgs(args)
    require(config.mode.nonEmpty,  "--mode is required")
    require(config.raw.nonEmpty,   "--raw is required")
    require(config.clean.nonEmpty, "--clean is required")

    // -----------------------
    // Spark session
    // -----------------------
    val spark = SparkSession.builder()
      .appName(s"spark-health-${config.mode}")
      .getOrCreate()

    // -----------------------
    // Dispatch by mode
    // -----------------------
    config.mode match {
      case "clean" =>
        runClean(spark, config.raw, config.clean)

      case other =>
        throw new IllegalArgumentException(s"Unknown mode: $other")
    }

    spark.stop()
  }

  /*
  CLEAN TRANSFORMATION (Bronze → Silver)
  -------------------------------------
  Transformations applied:
  1. Read raw JSON datasets (ILI + Clinical)
  2. Add ingestion timestamp (lineage / auditing)
  3. Remove duplicate rows
  4. Write clean Parquet outputs
  */
  def runClean(spark: SparkSession, rawPath: String, cleanPath: String): Unit = {

    // -----------------------
    // Read raw data
    // -----------------------
    val ili = spark.read.json(s"$rawPath/fluview_ili.json")
    val clinical = spark.read.json(s"$rawPath/fluview_clinical.json")

    // -----------------------
    // Clean + normalize
    // -----------------------
    val cleanedIli = ili
      .withColumn("ingest_ts", current_timestamp())
      .dropDuplicates()

    val cleanedClinical = clinical
      .withColumn("ingest_ts", current_timestamp())
      .dropDuplicates()

    // -----------------------
    // Write Silver layer
    // -----------------------
    cleanedIli.write
      .mode("overwrite")
      .parquet(s"$cleanPath/ili")

    cleanedClinical.write
      .mode("overwrite")
      .parquet(s"$cleanPath/clinical")
  }
}


