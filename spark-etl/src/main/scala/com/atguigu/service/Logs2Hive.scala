package com.atguigu.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean._
import com.atguigu.constants.Constants
import com.atguigu.until.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object Logs2Hive {

  /**
    * 基础广告表数据清洗
    *
    * @param ssc
    * @param sparkSession
    */
  def baseAdLog2Hive(ssc: SparkContext, sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val inputStream: RDD[String] = ssc.textFile(Constants.HDFS_BASEAD_LOG_PATH)

    inputStream.filter(item => {
      val obj: JSONObject = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val adid = jsonObject.getIntValue("adid")
        val adname = jsonObject.getString("adname")
        val dn = jsonObject.getString("dn")
        (adid, adname, dn)
      })
    }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(Constants.BASEAD_TABLE)

  }

  /**
    * 基础网站表数据清洗
    *
    * @param ssc
    * @param sparkSession
    */
  def basewesiteLog2Hive(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    ssc.textFile(Constants.HDFS_BASE_WEBSITE_LOG_PATH).filter(item => {
      val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
      jSONObject.isInstanceOf[JSONObject]
    }).mapPartitions(partitions => {
      partitions.map(
        item => {
          val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
          val siteid = jsonObject.getIntValue("siteid")
          val sitename = jsonObject.getString("sitename")
          val siteurl = jsonObject.getString("siteurl")
          val delete = jsonObject.getIntValue("delete")
          val createtime = jsonObject.getString("createtime")
          val creator = jsonObject.getString("creator")
          val dn = jsonObject.getString("dn")
          (siteid, sitename, siteurl, delete, createtime, creator, dn)
        }
      )
    }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(Constants.BASE_WEBSITE_TABLE)
  }

  /**
    * 用户注册数据清洗
    *
    * @param ssc
    * @param sparkSession
    */
  def memberRegTypeLog2Hive(ssc: SparkContext, sparkSession: SparkSession) = {
    /*ssc.textFile(Constants.HDFS_MEMBERREGTYPE_LOG_PATH)
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitoin => {
      partitoin.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val appkey = jsonObject.getString("appkey")
        val appregurl = jsonObject.getString("appregurl")
        val bdp_uuid = jsonObject.getString("bdp_uuid")
        val createtime = jsonObject.getString("createtime")
        val isranreg = jsonObject.getString("isranreg")
        val regsource = jsonObject.getString("regsource")
        val regsourceName = regsource match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "App"
          case "4" => "WeChat"
          case _ => "other"
        }
        val uid = jsonObject.getIntValue("uid")
        val websiteid = jsonObject.getIntValue("websiteid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid, appkey, appregurl, bdp_uuid, createtime, isranreg, regsource, regsourceName, websiteid, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto(Constants.MEMBERREGTYPE_TABLE)*/
    import sparkSession.implicits._
    val ds: Dataset[String] = sparkSession.read.textFile(Constants.HDFS_MEMBERREGTYPE_LOG_PATH)

    val result: Dataset[MemberRegtypeInfo] = ds.mapPartitions(
      line => {
        val temp = line.map(
          data => {
            val memberRegtypeInfo: MemberRegtypeInfo = JSON.parseObject(data, classOf[MemberRegtypeInfo])
            memberRegtypeInfo.regsourceName = memberRegtypeInfo.regsource match {
              case "1" => "PC"
              case "2" => "MOBILE"
              case "3" => "APP"
              case "4" => "WECHAT"
              case _ => "something else"
            }
            memberRegtypeInfo
          }
        )
        temp
      }
    )
    result.show()

    result.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto(Constants.MEMBERREGTYPE_TABLE)

  }

  /**
    * 用户等级数据清洗
    *
    * @param ssc
    * @param sparkSession
    */
  def pcenterMemViplevelLog2Hive(ssc: SparkContext, sparkSession: SparkSession) = {

    import sparkSession.implicits._
    val ds: Dataset[String] = sparkSession.read.textFile(Constants.HDFS_PCENTERMEMVIPLEVEL_LOG_PATH)

    val result: Dataset[(String, String, String, String, String, String, String, String, String, String)] = ds.map(record => {
      val vipLevelInfo: PcenterMemVipLevelInfo = JSON.parseObject(record, classOf[PcenterMemVipLevelInfo])
      (
        vipLevelInfo.vip_id,
        vipLevelInfo.vip_level,
        vipLevelInfo.start_time,
        vipLevelInfo.end_time,
        vipLevelInfo.last_modify_time,
        vipLevelInfo.max_free,
        vipLevelInfo.min_free,
        vipLevelInfo.next_level,
        vipLevelInfo.operator,
        vipLevelInfo.dn)
    })
    result.show()

    result.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto(Constants.VIP_LEVEL_TABLE)
  }

  /**
    * 用户支付数据清洗
    *
    * @param ssc
    * @param sparkSession
    */
  def pcenterMemPayMoneyLog2Hive(ssc: SparkContext, sparkSession: SparkSession) = {

    import sparkSession.implicits._
    val ds: Dataset[String] = sparkSession.read.format("json").textFile(Constants.HDFS_PCENTERMEMPAYMONEY_LOG_PATH)

    val result: Dataset[(String, String, String, String, String, String)] = ds.map(record => {

      val payMoneyInfo: PcenterMemPayMoneyInfo = JSON.parseObject(record, classOf[PcenterMemPayMoneyInfo])
      (payMoneyInfo.uid,
        payMoneyInfo.paymoney,
        payMoneyInfo.siteid,
        payMoneyInfo.vip_id,
        payMoneyInfo.dt,
        payMoneyInfo.dn)
    })

    result.show()

    result.toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(Constants.PCENTMEMPAYMONEY_TABLE)
  }

}
