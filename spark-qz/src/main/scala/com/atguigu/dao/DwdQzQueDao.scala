package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwdQzQueDao {

  def queryDwdQzQue(spark: SparkSession, date: String) = {
    spark.sql("select questionid,parentid,questypeid,quesviewtype,content,answer,analysis,limitminute," +
      "score,splitscore,status,optnum,lecture,creator,createtime,modifystatus,attanswer,questag,vanalysisaddr,difficulty," +
      s"quesskill,vdeoaddr,dt,dn from  dwd.dwd_qz_question where dt='${date}'")

  }

  def queryDwdQzQueType(spark: SparkSession, date: String) = {
    spark.sql("select questypeid,viewtypename,description,papertypename,remark,splitscoretype,dn from " +
      s"dwd.dwd_qz_question_type where dt='${date}'")

  }

}
