package com.tuorg.fluview

import com.tuorg.fluview.config.ArgsParser
import com.tuorg.fluview.jobs.FlueviewEtlJob

object Main {

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args) match {
      case Some(cfg) =>
        FlueviewEtlJob.run(cfg)

      case None =>
        System.exit(2)
    }
  }
}
