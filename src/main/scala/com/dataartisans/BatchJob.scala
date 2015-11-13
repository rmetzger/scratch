package com.dataartisans

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * Created by robert on 11/12/15.
  */
class BatchJob {

}

object BatchJob {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val sim = env.fromCollection(List("a", "b").iterator)
    sim.print()
  }
}
