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


    // 多流转换操作
    //分流操作 将传感器温度根据温度分成高温和低温
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30.0) {
        Seq("high")
      } else {
        Seq("low")
      }
    })

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    high.print("high")
    low.print("low")
    all.print("all")

    //合流操作
    val warningStream = high.map(data => (data.id, data.temperature))
    //connect可以合并不同类型的流 得到 ConnectedStreams
    val connected = warningStream.connect(low)

    //CoMap操作 ConnectedStreams → DataStream，针对不同的流，设置不同的函数
    val coMapResultStream = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      logData => (logData.id, "healty")
    )

    coMapResultStream.print()



    env.execute("transform test")
  }
}
