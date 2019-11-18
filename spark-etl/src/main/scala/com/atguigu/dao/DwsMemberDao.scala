package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwsMemberDao {

  def getMemberInfo(spark: SparkSession, date: String) = {
    spark.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
  }


  /**
    * 使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数
    */

  def getAppRegURLCount(spark: SparkSession, date: String) = {
    spark.sql(s"select appregurl,count(uid),dn,dt from dws.dws_member where dt='${date}' group by appregurl,dn,dt")
  }

  /**
    * 使用Spark DataFrame Api统计各所属网站（sitename）的用户数
    */

  def getSitenameCount(spark: SparkSession, date: String) = {
    spark.sql(s"select sitename,count(uid), dn, dt from dws.dws_member where dt='${date}' group by sitename,dn,dt")
  }

  /**
    * 使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数
    */

  def getRegSourceNameCount(spark: SparkSession, date: String) = {
    spark.sql(s"select resourcename,count(uid), dn, dt from dws.dws_member where dt='${date}' group by resourcename,dn,dt")
  }

  /**
    * 使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数
    */
  def getAdNameCont(spark: SparkSession, date: String) = {
    spark.sql(s"select adname,count(uid), dn, dt from dws.dws_member where dt='${date}' group by adname,dn,dt")
  }

  /**
    * 使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数
    */

  def getMemberLevelCount(spark: SparkSession, date: String) = {
    spark.sql(s"select memberlevel,count(uid),dn,dt from dws.dws_member where dt='${date} group by memberlevel, dn,dt'")
  }

  /**
    * 使用Spark DataFrame Api统计各vip等级人数
    */

  def getVIPCount(spark: SparkSession,date:String) = {

    spark.sql(s"select vip_level,count(uid),dn,dt from dws.dws_member group by vip_level, dn, dt")
  }

  /**
    *
    * 使用Spark DataFrame Api统计各分区网站、用户级别下(website、memberlevel)的支付金额top3用户
    */

  def getWebWithMemberLevelTop3(spark: SparkSession,date:String) = {

  spark.sql(s"select * from (select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcemname,adname," +
    s"siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,row_number() over(partition by " +
    s"dn,memberlevel order by cast(paymoney ad decimal(10,4)) desc) as rownum, dn from dws.dws_member where dt='${date})" +
    s" where rownum<4 order by memberlevel,rownum'")
  }
}
