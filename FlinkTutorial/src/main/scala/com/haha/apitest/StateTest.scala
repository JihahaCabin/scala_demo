package com.haha.apitest

import java.util
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object StateTest {
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

    //需求，对温度传感器，温度不能跳变，如果超过10度，则告警
    val alertStream = dataStream.keyBy(_.id)
      .flatMap(new TempChangeAlert(10.0))

    alertStream.print()

    env.execute("state test")
  }

}


//实现自定义RichFlatmapFuntion
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  //定义状态，保存上一次温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))


  override def flatMap(in: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值，作比较
    val diff = (in.temperature - lastTemp).abs
    // 如果超过阈值，输出
    if (diff > threshold) {
      out.collect((in.id, lastTemp, in.temperature))
    }
    //更新状态
    lastTempState.update(in.temperature)
  }

}


// keyed State 测试，必须定义在richFunction中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading, String] {

  // 普通值，列表，键值对

  private var valueState: ValueState[Double] = _
  //懒加载，在读取数据时初始化
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))
  //  lazy val reduceState:ReducingState[SensorReading] =
  //    getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduceState",new MyReducer,classOf[SensorReading]))

  //包装在open生命周期中
  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  }


  override def map(in: SensorReading): String = {
    val myV = valueState.value() //取值
    valueState.update(in.temperature) //更新

    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)

    listState.addAll(new util.ArrayList[Int]()) //追加
    listState.add(1) //追加
    listState.update(list) //替换


    return in.id
  }
}