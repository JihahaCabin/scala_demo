package com.haha.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

//流处理word count
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度（几个线程处理）
    env.setParallelism(3)

    //接收一个socket文本流
    val inputDataStream = env.socketTextStream("192.168.255.168",7777)

    //进行转换处理统计
    val resultDataStream = inputDataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

    resultDataStream.print()

    //启动excutor,执行任务
    env.execute("stream word count")
  }
}
