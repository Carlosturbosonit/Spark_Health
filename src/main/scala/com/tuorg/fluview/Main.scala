package com.tuorg.fluview

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OParser
import java.util.Properties

final case class Config(
  input: String = "",
  jdbcUrl: String = "",
  jdbcUser: String = "",
  jdbcPassword: String = "",
  runId: String = ""
)

object Main {

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("fluview-spark-job"),
        opt[String]("input").required().action((x, c) => c.copy(input = x)),
        opt[String]("jdbcUrl").required().action((x, c) => c.copy(jdbcUrl = x)),
        opt[String]("jdbcUser").required().action((x, c) => c.copy(jdbcUser = x)),
        opt[String]("jdbcPassword").required().action((x, c) => c.copy(jdbcPassword = x)),
        opt[String]("runId").required().action((x, c) => c.copy(runId = x))
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(cfg) => run(cfg)
      case None      => System.exit(2)
    }
  }

  private def run(cfg: Config): Unit = {
    val spark = SparkSession.builder()
      .appName(s"fluview_etl_${cfg.runId}")
      .getOrCreate()

    try {
      val raw = spark.read.json(cfg.input)

      val base = raw
        .withColumn("epiweek", col("epiweek").cast("int"))
        .withColumn("issue", col("issue").cast("int"))
        .withColumn("wili", col("wili").cast("double"))
        .withColumn("ili", col("ili").cast("double"))
        .withColumn("num_ili", col("num_ili").cast("int"))
        .withColumn("num_patients", col("num_patients").cast("int"))
        .withColumn("run_id", lit(cfg.runId))
        .filter(col("region").isNotNull && col("epiweek").isNotNull && col("issue").isNotNull)

      val stgIli = base.select(
        col("region"),
        col("epiweek"),
        col("issue"),
        col("wili"),
        col("ili"),
        col("num_ili"),
        col("num_patients"),
        col("run_id")
      )

      val regionWeek = base
        .groupBy(col("region"), col("epiweek"))
        .agg(
          max(col("issue")).as("latest_issue"),
          avg(col("wili")).as("avg_wili"),
          avg(col("ili")).as("avg_ili"),
          sum(col("num_ili")).as("sum_num_ili"),
          sum(col("num_patients")).as("sum_num_patients")
        )
        .withColumn("run_id", lit(cfg.runId))

      val props = new Properties()
      props.setProperty("user", cfg.jdbcUser)
      props.setProperty("password", cfg.jdbcPassword)
      props.setProperty("driver", "org.postgresql.Driver")

      writeTable(stgIli, cfg.jdbcUrl, "flu.stg_fluview_ili", props)
      writeTable(regionWeek, cfg.jdbcUrl, "flu.stg_fluview_region_week", props)

    } finally {
      spark.stop()
    }
  }

  private def writeTable(df: DataFrame, jdbcUrl: String, table: String, props: Properties): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("truncate", "true")
      .jdbc(jdbcUrl, table, props)
  }
}
