package com.haha.apitest

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import sun.management.Sensor

object TransformTest2 {
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
    // 输出当前最小的温度值， 还有最近的时间戳
    val aggStream = dataStream.keyBy("id") //根据id进行分组
      .reduce((curState, newData) =>
      SensorReading(curState.id, newData.timeStamp, curState.temperature.min(newData.temperature))
    )

    aggStream.print()

    env.execute("transform test")
  }
}
