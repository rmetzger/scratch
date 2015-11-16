package com.dataartisans

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector

object Main {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val words : DataStream[String] = text.flatMap[String](
      new FlatMapFunction[String,String] {
        override def flatMap(t: String, collector: Collector[String]): Unit = t.split(" ")
      })

    env.execute("Window Stream wordcount")
  }
}
