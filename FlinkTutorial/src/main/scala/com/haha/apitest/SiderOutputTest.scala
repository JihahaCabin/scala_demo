package com.haha.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object SiderOutputTest {

  def main(args: Array[String]): Unit = {

    // 创建一个批处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

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
    //高低温分流
    val highTempSteam = dataStream.process(new SplitTempProcess(30.0))

    highTempSteam.print("high")

    highTempSteam.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    env.execute("sider output test")
  }

}

// 实现自定义ProcessFuntion,进行分流
class SplitTempProcess(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature > threshold) {
      //如果温度大于阈值，输出到主流
      collector.collect(i)
    } else {
      //如果温度低于阈值，输出到侧输出流
      context.output(new OutputTag[(String, Long, Double)]("low"), (i.id, i.timeStamp, i.temperature))
    }
  }
}
