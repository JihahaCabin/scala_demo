package com.haha.apitest.udftest

import com.haha.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row


object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env)

    //1. 从文件中读取数据
    val inputPath: String = "D:\\Flink\\MyDemo\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream = env.readTextFile(inputPath)

    //先转换为样例类
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timeStamp * 1000L
      })

    //使用.proctime 定义处理时间
    //    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timeStamp, 'pt.proctime)
    // 使用.rowtime 可以定义事件时间属性
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timeStamp.rowtime as 'ts)

    // 定义自定义hash函数，对id进行hash运算
    // 1 table api
    //首先new一个UDF的实例
    val hashCode = new HashCode(23)
    val resultTable = sensorTable
      .select('id, 'ts, hashCode('id))

    resultTable.toAppendStream[Row].print("result")

    // 2 sql
    // 需要在环境里注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id,ts,hashCode(id) from sensor")

    resultSqlTable.toAppendStream[Row].print("resultSql")

    env.execute("sclar function test")
  }
}


//自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {

  def eval(s: String): Int = {
    s.hashCode * factor - 1000
  }
}
