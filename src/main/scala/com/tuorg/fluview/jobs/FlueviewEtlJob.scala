package com.tuorg.fluview.jobs

import com.tuorg.fluview.config.JobConfig
import com.tuorg.fluview.io.Writer
import com.tuorg.fluview.transform.FlueviewTransforms
import com.tuorg.fluview.util.SparkSessionUtil

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.AnalysisException

import java.sql.SQLException

object FlueviewEtlJob {

  private val log = LogManager.getLogger(getClass)

  def run(cfg: JobConfig): Unit = {
    val spark = SparkSessionUtil.create(appName = s"fluview_etl_${cfg.runId}")

    try {
      log.info(s"Starting FlueviewEtlJob runId=${cfg.runId} input=${cfg.input}")

      val raw = spark.read.json(cfg.input)

      val base = FlueviewTransforms.buildBase(raw, cfg.runId)
      val stgIli = FlueviewTransforms.toStgIli(base)
      val regionWeek = FlueviewTransforms.toRegionWeek(base, cfg.runId)

      Writer.overwriteTruncate(stgIli, cfg.jdbcUrl, "flu.stg_fluview_ili", cfg.jdbcUser, cfg.jdbcPassword)
      Writer.overwriteTruncate(regionWeek, cfg.jdbcUrl, "flu.stg_fluview_region_week", cfg.jdbcUser, cfg.jdbcPassword)

      log.info(s"FlueviewEtlJob finished OK runId=${cfg.runId}")

    } catch {
      case e: AnalysisException =>
        log.error(s"Spark AnalysisException runId=${cfg.runId}: ${e.getMessage}", e)
        throw e

      case e: SQLException =>
        log.error(s"JDBC SQLException runId=${cfg.runId}: ${e.getMessage}", e)
        throw e

      case e: IllegalArgumentException =>
        log.error(s"IllegalArgumentException runId=${cfg.runId}: ${e.getMessage}", e)
        throw e

      case e: Throwable =>
        log.error(s"Unhandled exception runId=${cfg.runId}: ${e.getMessage}", e)
        throw e

    } finally {
      log.info(s"Stopping SparkSession runId=${cfg.runId}")
      spark.stop()
    }
  }
}
