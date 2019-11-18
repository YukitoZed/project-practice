package com.atguigu.controller

import com.atguigu.service.DwsMemberService
import com.atguigu.until.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsMemberController {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩

    DwsMemberService.createWide_ZipperTable(sparkSession,"20190722") //
  }

}
