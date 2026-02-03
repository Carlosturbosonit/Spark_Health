package com.tuorg.fluview.config

import scopt.OParser

object ArgsParser {

  def parse(args: Array[String]): Option[JobConfig] = {
    val builder = OParser.builder[JobConfig]

    val parser = {
      import builder._
      OParser.sequence(
        programName("fluview-spark-job"),
        head("fluview-spark-job", "1.0"),
        opt[String]("input")
          .required()
          .action((x, c) => c.copy(input = x))
          .text("Path del JSON/JSONL de entrada"),
        opt[String]("jdbcUrl")
          .required()
          .action((x, c) => c.copy(jdbcUrl = x))
          .text("JDBC URL, ej: jdbc:postgresql://postgres_dw:5432/dw"),
        opt[String]("jdbcUser")
          .required()
          .action((x, c) => c.copy(jdbcUser = x))
          .text("Usuario JDBC"),
        opt[String]("jdbcPassword")
          .required()
          .action((x, c) => c.copy(jdbcPassword = x))
          .text("Password JDBC"),
        opt[String]("runId")
          .required()
          .action((x, c) => c.copy(runId = x))
          .text("Identificador del run (Airflow run_id)")
      )
    }

    OParser.parse(parser, args, JobConfig())
  }
}
