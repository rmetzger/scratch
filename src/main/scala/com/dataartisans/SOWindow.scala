package com.dataartisans

import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.windowing.Time
import org.slf4j.{LoggerFactory, Logger}

import scala.util.Random
import org.apache.flink.api.scala._
import scala.concurrent.duration._

class SOWindow {

}
object SOWindow {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SOWindow])

  def generateWords(ctx: SourceContext[String]) = {
    val words = List("amigo", "brazo", "pelo")
    while (true) {
      Thread.sleep(300)
      ctx.collect(words(Random.nextInt(words.length)))
    }
  }

  def main(args: Array[String]) {
    EnvironmentInformation.logEnvironmentInfo(LOG, "test", args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.addSource(generateWords _)


   /* val windowedStream = stream.map((_, 1)).startNewChain()
      .window(Time of(10, SECONDS)).every(Time of(5, SECONDS))


    val wordCount = windowedStream
      .groupBy("_1")
      .sum("_2")

    wordCount
      .getDiscretizedStream()
      .print() */

    env.execute("sum randoms")
  }
}
