package com.tuorg.fluview.util

import org.apache.spark.sql.SparkSession

object SparkSessionUtil {

  def create(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      // útil si quieres forzar UTC desde aquí en vez del DAG
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }
}
