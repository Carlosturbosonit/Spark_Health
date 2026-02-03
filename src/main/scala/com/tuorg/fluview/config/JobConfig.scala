package com.tuorg.fluview.config

final case class JobConfig(
  input: String = "",
  jdbcUrl: String = "",
  jdbcUser: String = "",
  jdbcPassword: String = "",
  runId: String = ""
)
