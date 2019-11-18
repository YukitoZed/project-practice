package com.atguigu.service

import com.atguigu.bean.MemberInfo
import com.atguigu.constants.Constants
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object MemberLogETL {

  def memberLogetl(sparkSession: SparkSession) = {

    val df: DataFrame = sparkSession.read.json(Constants.HDFS_MEMBER_LOG_PATH)

    import sparkSession.implicits._

    val ds: Dataset[MemberInfo] = df.as[MemberInfo]

    val mapDSet = ds.map(data => {

      //用户脱敏
      var fullName: String = data.fullName
      fullName = data.fullName.split("")(0) + "xx"

      //手机号脱敏
      var phone: String = data.phone
      phone = phone.splitAt(3)._1 + "****" + phone.splitAt(7)._2

      //密码脱敏
      var password: String = data.password
      password = "******"

      (
        data.uid,
        data.ad_id,
        data.birthday,
        data.email,
        fullName,
        data.iconUrl,
        data.lastLogin,
        data.mailAddr,
        data.memberLevel,
        password,
        data.payMoney,
        phone,
        data.qq,
        data.register,
        data.regUpdatetime,
        data.unitName,
        data.userIp,
        data.zipcode,
        data.dt,
        data.dn)
    })

    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    mapDSet.show()

    mapDSet.toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(Constants.MEMBER_TABLE)


  }

}
