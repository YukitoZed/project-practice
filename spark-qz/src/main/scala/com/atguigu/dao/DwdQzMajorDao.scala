package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwdQzMajorDao {

  def queryDwdQzMajor(spark:SparkSession,date:String)={
    spark.sql("select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator," +
      s"createtime as major_createtime,dt,dn from dwd.dwd_qz_major where dt='${date}'")

  }

  def queryDwdQzWebsite(spark:SparkSession,date:String)={
    spark.sql("select siteid,sitename,domain,multicastserver,templateserver,creator," +
      s"createtime,multicastgateway,multicastport,dn from dwd.dwd_qz_website where dt='${date}'")

  }

  def queryDwdQzBusiness(spark:SparkSession,date:String)={
    spark.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt='${date}'")
  }

}
