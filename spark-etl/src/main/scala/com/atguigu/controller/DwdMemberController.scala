package com.atguigu.controller

import com.atguigu.service.Logs2Hive._
import com.atguigu.service.MemberLogETL
import com.atguigu.until.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdMemberController {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩

    baseAdLog2Hive(ssc, sparkSession) //导入基础广告表数据
    basewesiteLog2Hive(ssc, sparkSession) //导入基础网站表数据
    memberRegTypeLog2Hive(ssc, sparkSession) //清洗用户注册数据
    pcenterMemViplevelLog2Hive(ssc, sparkSession) //导入vip基础数据
    pcenterMemPayMoneyLog2Hive(ssc, sparkSession) //导入用户支付情况记录
    MemberLogETL.memberLogetl(sparkSession) //清洗用户数据


  }

}
