package com.atguigu.service

import com.atguigu.bean.QueryResult
import com.atguigu.constants.Constants
import com.atguigu.dao.DwsMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object AdsMemberService {
  /**
    * SQL实现
    *
    * @param spark
    * @param date
    * @return
    */
  def queryAppRegURLCount(spark: SparkSession, date: String) = {

    DwsMemberDao.getAppRegURLCount(spark, date)
    DwsMemberDao.getRegSourceNameCount(spark,date)
    DwsMemberDao.getSitenameCount(spark,date)
    DwsMemberDao.getAdNameCont(spark,date)
    DwsMemberDao.getMemberLevelCount(spark,date)
    DwsMemberDao.getVIPCount(spark,date)
    DwsMemberDao.getWebWithMemberLevelTop3(spark,date)

  }

  /**
    * API实现
    *
    * @param spark
    * @param date
    */
  def queryAppRegURLCountAPI(spark: SparkSession, date: String) = {

    import spark.implicits._

    // 获取所有用户数据
    val result = DwsMemberDao.getMemberInfo(spark, date).as[QueryResult].where(s"dt='${date}'")

    result.cache()

    //统计通过各注册跳转地址(appregurl)进行注册的用户数
    result.mapPartitions(partition => {
      partition.map(item => {
        //http:www.webA.com/sale/register/index.html_20190722_webA,1
        (item.appregurl + "_" + item.dt + "_" + item.dn, 1)
      })
    })
      //(http:www.webA.com/sale/register/index.html_20190722_webA,(1,1,1,1)
      .groupByKey(_._1)
      //(1,1,1,1,1)
      .mapValues(_._2)
      //(http:www.webA.com/sale/register/index.html_20190722_webA,5)
      .reduceGroups(_ + _)
      .map(item => {
        val fileds: Array[String] = item._1.split("_")
        val appregurl: String = fileds(0)
        val dt: String = fileds(1)
        val dn: String = fileds(2)
        (appregurl, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_APPREGURLNUM)

    //统计各所属网站（sitename）的用户数
    result.mapPartitions(partiton => {
      partiton.map(item => (item.sitename + "_" + item.dn + "_" + item.dt, 1))
    })
      .groupByKey(_._1)
      .mapValues((item => item._2))
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (sitename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_SITENAME)

    //统计各所属平台的（regsourcename）用户数
    result.mapPartitions(partiton => {
      partiton.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt, 1))
    })
      .groupByKey(_._1)
      .mapValues((item => item._2))
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (regsourcename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_REGSOURCENAME)

    //统计通过各广告跳转（adname）的用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt, 1))
    })
      .groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (adname, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_ADNAME)
    //统计各用户级别（memberlevel）的用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
    })
      .groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (memberlevel, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_MEMBERLEVEL)

    //统计各vip等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
    })
      .groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (vip_level, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_VIPLEVEL)

    //统计各分区网站、用户级别下(website、memberlevel)的支付金额top3用户

    import org.apache.spark.sql.functions._
    //开窗
    result.withColumn("rownum", row_number().over(Window.partitionBy("dn","memberlevel").orderBy(desc("paymoney"))))
      // 取top3
      .where("rownum<4").orderBy("memberlevel","rownum")
      //字段
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
        , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn").toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto(Constants.ADS_REGISTER_TOP3MEMBERPAY)
  }


}
