package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwdQzPaperDao {

  def queryDwdQzPaperView(spark: SparkSession, date: String) = {
    spark.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest," +
      "contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator as paper_view_creator," +
      "createtime as paper_view_createtime,paperviewcatid,modifystatus,description,papertype,downurl,paperuse," +
      s"paperdifficult,testreport,paperuseshow,dt,dn from dwd.dwd_qz_paper_view where dt='${date}'")
  }


  def queryDwdQzCenter(spark: SparkSession, date: String) = {
    spark.sql("select centerid,centername,centeryear,centertype,centerparam,provideuser," +
      s"centerviewtype,stage,dn from dwd.dwd_qz_center where dt='${date}'")

  }


  def queryDwdQzCenterPaper(spark: SparkSession, date: String) = {
    spark.sql(s"select paperviewid,sequence,centerid,dn from dwd.dwd_qz_center_paper where dt='${date}'")
  }

  def queryDwdQzPaper(spark: SparkSession, date: String) = {
    spark.sql("select paperid,papercatid,courseid,paperyear,chapter,suitnum,papername,totalscore,chapterid," +
      s"chapterlistid,dn from dwd.dwd_qz_paper where dt='${date}'")

  }
}
