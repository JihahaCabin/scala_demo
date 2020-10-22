package com.haha.wc

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    //创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.put("bootstrap.servers","localhost:9092")
    properties.put("group.id","consumer-group")
    //添加数据源
    val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    //打印输出
    stream.print()

    //启动
    env.execute("kafka read")

  }
}
