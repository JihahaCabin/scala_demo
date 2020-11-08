package com.haha.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {

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
    //      .keyBy(_.id)
    //        .process(new MyKeyedProcessFunction)

    var warningStream = dataStream.keyBy(_.id).process(new TempIncreWarning(10000L))

    warningStream.print()
    env.execute("process funtion test")
  }

}

// 实现自定义的KeyedProcessFunction,判断温度是否连续上升
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
  // 定义状态，保存上一个温度值，进行比较，保存注册定时器的时间戳用于删除
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))


  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出状态
    val lastTemp = lastTempState.value()
    val timerTs = timerTsState.value()

    // 当前温度值和上次温度进行比较
    if (i.temperature > lastTemp && timerTs == 0) {
      //如果温度上升，且没有定时器，那么注册当前数据时间时间戳10s之后的定时器
      val ts = context.timerService().currentProcessingTime() + interval
      context.timerService().registerProcessingTimeTimer(ts)
      timerTsState.update(ts)
    } else if (i.temperature < lastTemp) {
      //如果当前温度下降，删除定时器
      context.timerService().deleteProcessingTimeTimer(timerTs)
      //清空状态
      timerTsState.clear()
    }
    //更新上一次温度数据
    lastTempState.update(i.temperature)

  }

  // 定时器被触发的逻辑
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval / 1000 + "秒连续上升")
    timerTsState.clear()

  }
}


class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {

  //声明
  var myState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    //初始化
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("mystate", classOf[Int]))
  }


  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    context.getCurrentKey
    context.timestamp()
    context.timerService().currentWatermark()
    context.timerService().registerEventTimeTimer(context.timestamp() + 60 * 1000L) //1分钟后的定时器
  }

  //重写定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}