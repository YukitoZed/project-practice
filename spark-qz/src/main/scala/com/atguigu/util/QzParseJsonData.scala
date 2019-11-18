package com.atguigu.util

import com.alibaba.fastjson.{JSON, JSONObject}

object QzParseJsonData {

  def parseObject(data: String): JSONObject = {

    try {
      return JSON.parseObject(data)
    } catch {
      case e: Exception => null
    }
  }
}
