package com.haha.apitest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {

    // 创建一个批处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath: String = "D:\\Flink\\MyDemo\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream = env.readTextFile(inputPath)

    //先转换为样例类
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    //分组聚合，输出每个传感器最小值
    val aggStream = dataStream.keyBy("id") //根据id进行分组
      // .min("temperature") //输出结果中，提取出该字段最小值，其他字段使用读取到的数据的第一个
      .minBy("temperature") //输出该字段最小值所对应的数据数据

    aggStream.print()

    env.execute("transform test")
  }
}
