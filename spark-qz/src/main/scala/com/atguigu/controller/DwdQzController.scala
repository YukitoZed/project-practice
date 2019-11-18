package com.atguigu.controller

import com.atguigu.service.DwdqzService
import com.atguigu.until.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdQzController {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("qz-etl")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = spark.sparkContext

    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)

    DwdqzService.qzBusinessLogETL(ssc, spark) // 所属行业数据导入
    DwdqzService.qzWebsiteLogETL(ssc, spark) //网站日志数据导入
    DwdqzService.qzSiteCourseLogETL(ssc, spark) //网站课程日志数据导入
    DwdqzService.qzQueTypeLogETL(ssc, spark) //题目类型数据导入
    DwdqzService.qzQueLogETL(ssc, spark) //做题日志数据导入
    DwdqzService.qzPointQueLogETL(ssc, spark) //做题知识点关联数据导入
    DwdqzService.qzPointLogETL(ssc, spark) //知识点数据导入
    DwdqzService.qzPaperViewLogETL(ssc, spark) //试卷视图数据导入
    DwdqzService.qzPaperLogETL(ssc, spark) //做题试卷日志数据导入
    DwdqzService.qzMemberPaperQueLogETL(ssc, spark) //学员做题数据导入
    DwdqzService.qzMajorLogETL(ssc, spark) // 主修数据导入
    DwdqzService.qzCourseEduSubjectLogETL(ssc, spark) //课程辅导数据导入
    DwdqzService.qzCourseETL(ssc, spark) //题库数据导入
    DwdqzService.qzChapterListLogETL(ssc, spark) //章节列表数据导入
    DwdqzService.qzChapterLogETL(ssc, spark) //章节数据导入
    DwdqzService.qzCenterPaperETL(ssc, spark) //试卷主题关联数据导入
    DwdqzService.qzCenterLogETL(ssc, spark) //主题数据导入

  }
}
