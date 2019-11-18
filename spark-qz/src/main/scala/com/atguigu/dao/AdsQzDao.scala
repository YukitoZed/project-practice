package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object AdsQzDao {


  def getAvgDetailDwsUserPaperDetail(spark: SparkSession, date: String) = {

    spark.sql("select paperviewid,paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail ")

  }

  def getMaxMinScoreDetailDwsUserPaperDetail(spark: SparkSession, date: String)={
    spark.sql("select paperviewid,paperviewname,score,dt,dn from dws.dws_user_paper_detail")

  }

  def getAllDetailDwsUserPaperDetail(spark: SparkSession, date: String)={
    spark.sql("select * from dws.dws_user_paper_detail")
  }

}
