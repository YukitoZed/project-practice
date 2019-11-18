package com.atguigu.controller

import com.atguigu.service.AdsqzService
import com.atguigu.until.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AdsQzController {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setMaster("ads_result")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFs","hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices","nameservice1")

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)

    AdsqzService.getDwsUserPaperDetail(spark,"20190722")
  }

}
