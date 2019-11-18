package com.atguigu.controller

import com.atguigu.service.DwsqzService
import com.atguigu.until.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsQzController {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dws_create").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)

//    DwsqzService.createDwsQzChapter(spark,"20190722") //章节表数据
//    DwsqzService.createDwsQzCourse(spark,"20190722") //课程表数据
//    DwsqzService.createDwsQzMajor(spark,"20190722") //主修表数据
//    DwsqzService.createDwsQzPaper(spark,"20190722") //试卷表数据
//    DwsqzService.createDwsQzQue(spark,"20190722") //题目表数据
    DwsqzService.createDwsUserPaperDetail(spark,"20190722")

  }
}
