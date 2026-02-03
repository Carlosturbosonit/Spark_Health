package com.tuorg.fluview.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlueviewTransforms {

  /** Normaliza/castea y filtra mínimos obligatorios */
  def buildBase(raw: DataFrame, runId: String): DataFrame = {
    raw
      .withColumn("epiweek", col("epiweek").cast("int"))
      .withColumn("issue", col("issue").cast("int"))
      .withColumn("wili", col("wili").cast("double"))
      .withColumn("ili", col("ili").cast("double"))
      // En Postgres tienes bigint -> usa long
      .withColumn("num_ili", col("num_ili").cast("long"))
      .withColumn("num_patients", col("num_patients").cast("long"))
      .withColumn("run_id", lit(runId))
      .filter(
        col("region").isNotNull &&
        col("epiweek").isNotNull &&
        col("issue").isNotNull
      )
  }

  def toStgIli(base: DataFrame): DataFrame = {
    base.select(
      col("region"),
      col("epiweek"),
      col("issue"),
      col("wili"),
      col("ili"),
      col("num_ili"),
      col("num_patients"),
      col("run_id")
    )
  }

  def toRegionWeek(base: DataFrame, runId: String): DataFrame = {
    base
      .groupBy(col("region"), col("epiweek"))
      .agg(
        max(col("issue")).as("latest_issue"),
        avg(col("wili")).as("avg_wili"),
        avg(col("ili")).as("avg_ili"),
        sum(col("num_ili")).as("sum_num_ili"),
        sum(col("num_patients")).as("sum_num_patients")
      )
      .withColumn("run_id", lit(runId))
  }
}
