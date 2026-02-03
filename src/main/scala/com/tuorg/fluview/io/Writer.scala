package com.tuorg.fluview.io

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object Writer {

  def overwriteTruncate(
    df: DataFrame,
    jdbcUrl: String,
    table: String,
    user: String,
    password: String
  ): Unit = {

    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "org.postgresql.Driver")

    df.write
      .mode(SaveMode.Overwrite)
      .option("truncate", "true")
      .jdbc(jdbcUrl, table, props)
  }
}
