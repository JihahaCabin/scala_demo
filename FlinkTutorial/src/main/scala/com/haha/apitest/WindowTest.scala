package com.haha.apitest

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
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

    val resultStream = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1) //按照二元组的第一个元素分组
      //   .window(TumblingEventTimeWindows.of(Time.seconds(15))) //定义15s的滚动时间窗口
      //   .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5))) //定义15s大小，5s间隔的滑动时间窗口
      .timeWindow(Time.seconds(15)) //简写方式
      .reduce((r1, r2) => (r1._1, r1._2.max(r2._2)))

    resultStream.print()

    env.execute()


  }

}
