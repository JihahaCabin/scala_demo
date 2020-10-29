package com.haha.apitest

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    // 从文件中读取数据
    ////    val inputPath: String = "D:\\Flink\\MyDemo\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    ////    val stream = env.readTextFile(inputPath)

    val stream = env.socketTextStream("192.168.255.188", 7777)

    //先转换为样例类
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // 每15s统计一次，窗口各传感器所有温度的最小值，以及最新的时间戳
    val resultStream = dataStream.map(data => (data.id, data.temperature, data.timeStamp))
      .keyBy(_._1) //按照二元组的第一个元素分组
      //   .window(TumblingEventTimeWindows.of(Time.seconds(15))) //定义15s的滚动时间窗口
      //   .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5))) //定义15s大小，5s间隔的滑动时间窗口
      .timeWindow(Time.seconds(15)) //简写方式
      .reduce((cureRes, newData) => (cureRes._1, cureRes._2.min(newData._2), newData._3))

    resultStream.print()

    env.execute()
  }

}
