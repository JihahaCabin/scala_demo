package com.haha.apitest.tabletest

import com.haha.apitest.SensorReading
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

object Example {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从文件中读取数据
    val inputPath: String = "D:\\Flink\\MyDemo\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream = env.readTextFile(inputPath)

    //    val stream = env.socketTextStream("192.168.133.158", 7777)

    //先转换为样例类
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 首先创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 调用 table api 进行转换
    val resultTable = dataTable.select("id,temperature").filter("id == 'sensor_1'")

    resultTable.toAppendStream[(String, Double)].print("result")

    // 直接使用sql实现
    tableEnv.createTemporaryView("datatable", dataTable)
    val sql: String = "select id,temperature from datatable where id= 'sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)
    resultSqlTable.toAppendStream[(String, Double)].print("resultSql")


    env.execute("table api test")
  }
}
