package com.haha.wc

import org.apache.flink.api.java.utils.ParameterTool

object ReadParams {

  def main(args: Array[String]): Unit = {
      val params = ParameterTool.fromArgs(args)

    val host:String = params.get("host")
    val port:Int = params.getInt("port")

    println(host)
    println(port)
  }
}
