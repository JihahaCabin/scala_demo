package com.haha.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置时间语义为事件时间
    //    env.getConfig.setAutoWatermarkInterval(50) //设置watermark时间间隔

    //    // 从文件中读取数据
    ////    val inputPath: String = "D:\\Flink\\MyDemo\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    ////    val stream = env.readTextFile(inputPath)

    val stream = env.socketTextStream("192.168.133.158", 7777)

    //先转换为样例类
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      //    .assignAscendingTimestamps(_.timeStamp *1000) //升序数据，直接使用数据的时间戳生成 watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
      override def extractTimestamp(t: SensorReading): Long = t.timeStamp * 1000L //当前的时间戳
    }) //乱序数据，周期性生成watermark 要设置最大乱序程度  hold住最大的乱序数据3秒




    // 每15s统计一次，窗口各传感器所有温度的最小值，以及最新的时间戳
    val resultStream = dataStream.map(data => (data.id, data.temperature, data.timeStamp))
      .keyBy(_._1) //按照二元组的第一个元素分组
      //   .window(TumblingEventTimeWindows.of(Time.seconds(15))) //定义15s的滚动时间窗口
      //   .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5))) //定义15s大小，5s间隔的滑动时间窗口
      .timeWindow(Time.seconds(15)) //简写方式
      .allowedLateness(Time.minutes(1)) //处理1分钟的迟到数据
      .sideOutputLateData(new OutputTag[(String, Double, Long)]("later")) //给迟到的数据设置标签，把迟到的数据侧输出流
      .reduce((cureRes, newData) => (cureRes._1, cureRes._2.min(newData._2), newData._3))

    //打印侧输出流
    resultStream.getSideOutput(new OutputTag[(String, Double, Long)]("later")).print("later")

    resultStream.print("sensor")
    env.execute()
  }

}
