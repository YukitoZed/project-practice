package com.atguigu.service

import com.atguigu.bean.{MemberZiper, MemberZiperResult}
import com.atguigu.constants.Constants
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DwsMemberService {

  def createWide_ZipperTable(spark: SparkSession, date: String) = {
    //    val df: DataFrame = DwdMemberDao.getBaseAd(spark)
    /* spark.sql(
       """
         |select
         |
         |from(
         |    select
         |
         |    from dwd.dwd_member dm join dwd.dwd_regtype dr
         |    on dm.uid=dr.uid
         |    join (select
         |
         |            from dwd.dwd_pcentermempaymoney dp join dwd_base_website dbw
         |            on dp.siteid=dbw.siteid) t1
         |    on dm.uid join t1.uid
         |    join dwd_base_ad dba
         |    on dm.ad_id=dba.adid
         |    where dm.dt=date
         |) t2
         |join dwd.dwd_vip_level dvl
         |on t1.vip_id=dvl.vip_id
       """.stripMargin)*/

    //所有表进行join组成宽表

    spark.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
      "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
      "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
      "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime)," +
      "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
      "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
      "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
      "first(vip_operator),dt,dn from" +
      "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
      "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
      "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.isranreg,b.regsource," +
      "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
      "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
      "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
      "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
      s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
      "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
      " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
      s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${date}')r  " +
      "group by uid,dn,dt").toDF().write.mode(SaveMode.Append).insertInto(Constants.DWS_MEMBER_TABLE)


    //pcenterpaymoney与vip_level表进行join得到每日更新数据

    import spark.implicits._

    val dayResult = spark.sql(s"select dp.uid,sum(cast(dp.paymoney as decimal(10,4))) as paymoney, max(dvl.vip_level) as vip_level," +
      s"from_unixtime(unix_timestamp('$date','yyyyMMdd'),'yyyy-MM-dd') as start_time, '9999-12-31' as end_time,first(dp.dn)as dn " +
      s"from dwd.dwd_pcentermempaymoney dp join dwd.dwd_vip_level dvl on dp.vip_id=dvl.vip_id and dp.dn=dvl.dn where dp.dt='$date' group by uid").as[MemberZiper]

    /*  1001,200,2,2019-11-15,2019-11-20
        1001,500,2,2019-11-20,2019-11-25
        1001,500,2,2019-11-25,9999-12-31
        1001,500,2,2019-11-30,9999-12-31
        1002,200,2,2019-11-15,2019-11-20
        1003,500,2,2019-11-20,2019-11-25
     */

    // 历史拉链表
    val historyZipper: Dataset[MemberZiper] = spark.sql("select * from dws.dws_member_zipper").as[MemberZiper]

    import spark.implicits._

    dayResult.union(historyZipper).groupByKey(data => data.uid + "_" + data.dn)
      .mapGroups({
        case (key, itr) =>
          val fileds: Array[String] = key.split("_")
          val uid: String = fileds(0)
          val dn: String = fileds(1)
          val memberZipers: List[MemberZiper] = itr.toList.sortBy(_.start_time)
          if(memberZipers.size>1 && "9999-12-31".equals(memberZipers(memberZipers.size-2).end_time)){
            val curLastLog = memberZipers(memberZipers.size - 1)
            val hisLastLog = memberZipers(memberZipers.size - 2)

            //倒数第二条设置endtime
            hisLastLog.end_time = curLastLog.start_time

            //最后一条的金额应该是今日金额+历史金额
            curLastLog.paymoney = (BigDecimal.apply(curLastLog.paymoney)+BigDecimal.apply(hisLastLog.paymoney)).toString()

          }

          MemberZiperResult(memberZipers)
      }).flatMap(_.memberZipers).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(Constants.MEMBER_ZIPPER_TABLE)


  }


}
