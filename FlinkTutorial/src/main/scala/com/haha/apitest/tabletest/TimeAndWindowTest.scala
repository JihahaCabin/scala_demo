package com.haha.apitest.tabletest

import com.haha.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env)

    // 从文件中读取数据
    val inputPath: String = "D:\\Flink\\MyDemo\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream = env.readTextFile(inputPath)

    //先转换为样例类
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    //使用.proctime 定义处理时间
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timeStamp, 'pt.proctime)
    //打印表结构
    sensorTable.printSchema()
    sensorTable.toAppendStream[Row].print()

    env.execute("time and window test")

  }
}
