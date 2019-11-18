package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwdMemberDao {

  def getBaseAd(spark: SparkSession) = {
    spark.sql(
      """
        |select
        | adid,
        | adname,
        | dn
        |from dwd.dwd_base_ad
      """.stripMargin)
  }

  def getBaseWebsite(spark: SparkSession) = {

    spark.sql(
      """
        |select
        |    siteid,
        |    sitename,
        |    siteurl,
        |    `delete`,
        |    createtime,
        |    creator,
        |    dn
        |from dwd.dwd_base_website
      """.stripMargin)
  }


  def getMember(spark: SparkSession) = {

    spark.sql(
      """
        |select
        |  uid,
        |  ad_id,
        |  email,
        |  fullname,
        |  iconurl,
        |  lastlogin,
        |  mailaddr,
        |  memberlevel,
        |  password,
        |  phone,
        |  qq,
        |  register,
        |  regupdatetime,
        |  unitname,
        |  userip,
        |  zipcode,
        |  dt,
        |  dn
        |from
        |  dwd.dwd_member
      """.stripMargin)

  }

  def getMemberRegType(spark: SparkSession) = {

    spark.sql(
      """
        |select
        |  uid,
        |  appkey,
        |  appregurl,
        |  bdp_uuid,
        |  createtime as reg_createtime,
        |  isranreg,
        |  regsource,
        |  regsourcename,
        |  websiteid as siteid,
        |  dn
        |from
        |  dwd.dwd_member_regtype
      """.stripMargin)
  }

  def getPcenterMemPayMoney(spark: SparkSession) = {
    spark.sql(
      """
        |select
        |    uid,
        |    cast(paymoney as decimal(10,4)) paymoney,
        |    siteid,
        |    vip_id,
        |    dt,
        |    dn
        |from dwd.dwd_pcentermempaymoney
      """.stripMargin)
  }

  def getVipLevel(spark: SparkSession) = {

    spark.sql(
      """
        |select
        |  vip_id,
        |  vip_level,
        |  start_time as vip_start_time,
        |  end_time as vip_end_time,
        |  last_modify_time as vip_last_modify_time,
        |  max_free as vip_max_free,
        |  min_free as vip_min_free,
        |  next_level as vip_next_level,
        |  operator as vip_operator,
        |  dn
        |from
        |  dwd.dwd_vip_level
      """.stripMargin)

  }
}
