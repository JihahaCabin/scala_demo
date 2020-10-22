package com.haha.wc


import com.haha.apitest.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object ReadFromMySensor {

  def main(args: Array[String]): Unit = {
    //创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new MySensorSource())
    //打印输出
    stream.print()

    //启动
    env.execute("read from  my sensor")

  }
}

class MySensorSource extends SourceFunction[SensorReading]{

  //定义一个flag,用于表示数据源是否正常运行，发送数据
  var running:Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    val random = new Random()

    //随机生成一组10个的传感器温度
    var currentTemp = 1.to(10).map(
      i => ("sensor_" + i, 65 + random.nextGaussian() * 20)
    )

    //定义无限循环，产生数据
    while (running){

      //在上次基础上微调数据
      currentTemp = currentTemp.map(
        t => (t._1, t._2 + random.nextGaussian())
      )

      val currentTime = System.currentTimeMillis()

      //循环数据，调用collect发送数据
      currentTemp.foreach(
        t=> sourceContext.collect(SensorReading(t._1,currentTime,t._2))
      )

      Thread.sleep(200)
    }
  }


}
