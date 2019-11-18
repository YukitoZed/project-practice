package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwdQzChapterDao {


  def queryDwdQzChapter(spark: SparkSession, date: String) = {

    spark.sql(s"select chapterid,chapterlistid,chaptername,sequence,showstatus, creator as chapter_creator," +
      s"createtime as chapter_createtime,courseid as chapter_courseid,chapternum,outchapterid,dt,dn " +
      s"from dwd.dwd_qz_chapter where dt='${date}'")
  }


  def queryDwdQzChapterList(spark: SparkSession, date: String) = {

    spark.sql("select chapterlistid,chapterlistname,chapterallnum,status,dn from dwd.dwd_qz_chapter_list " +
      s"where dt='$date'")

  }

  def queryDwdQzPoint(spark: SparkSession, date: String) = {

    spark.sql("select pointid,pointname,pointyear,chapter,excisenum,pointlistid,chapterid," +
      "pointdescribe,pointlevel,typelist,score as point_score,thought,remid,pointnamelist,typelistids,pointlist,dn from " +
      s"dwd.dwd_qz_point where dt='$date'")

  }


  def queryDwdQzPointQuestion(spark: SparkSession, date: String) = {

    spark.sql(s"select pointid,questionid,questype,dn from dwd.dwd_qz_point_question where dt='$date'")
  }
}
