package com.atguigu.service

import com.atguigu.dao.AdsQzDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object AdsqzService {

  def getDwsUserPaperDetail(spark: SparkSession, date: String) = {
    import org.apache.spark.sql.functions._

    //基于宽表统计各试卷平均耗时、平均分，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api。
    /*
    select paperviewid,paperviewname,cast(avg(score) as decimal,4,1) avgscore,cast(avg(spendtime) as decimal 4,1) avgspendtime
     dt, dn from dws_user_paper_detail group by paperviewid,paperviewname,dt,dn where dt='20190722'
     */

    val avgDetail = AdsQzDao.getAvgDetailDwsUserPaperDetail(spark, date)


    avgDetail
      .where(s"dt='${date}'")
      .groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(avg("score").cast("decimal(4,1)")).as("avgscore")
      .agg(avg("spendtime").cast("decimal(10,1)")).as("avgspendtime")
      .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
      .toDF().show()


    //统计各试卷最高分、最低分，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api。

    /*
    select paperviewid,paperviewname,max(score),min(score),dt,dn from dws.dws_user_paper_detail group by
    paperviewid,paperviewname,dt,dn where dt='20190722';
     */
    val scoreDetail: DataFrame = AdsQzDao.getMaxMinScoreDetailDwsUserPaperDetail(spark, date)

    scoreDetail
      .where(s"dt='${date}'")
      .groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(max("score")).as("maxscore")
      .agg(min("score")).as("minscore")
      .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
      .toDF().show()


    //按试卷分组统计每份试卷的前三用户详情，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api。

    /*
      select * from(select userid,paperviewid,paperviewname,chaptername,pointname,stiecoursename,coursename,majorname
      ,shortname,papername dense_rank() over(partition by paperviewid,paperviewname dt,dn order by score)rk dt,dn from
      dws.dws_user_paper_detail ) where rk<=3 group by paperviewid,paperviewname dt,dn where dt='20190722'
     */

    val allDetail: DataFrame = AdsQzDao.getAllDetailDwsUserPaperDetail(spark, date)

    allDetail
      .where(s"dt='${date}'")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
        , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank() over (Window.partitionBy("paperviewid").orderBy(desc("score"))))
      .where("rk<=3")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn"
      )
      .toDF().show()
    //按试卷分组统计每份试卷的倒数前三的用户详情，先使用Spark Sql 完成指标统计，再使用Spark DataFrame Api

    allDetail
      .where(s"dt='${date}'")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
        , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank() over (Window.partitionBy("paperviewid").orderBy("score")))
      .where("rk<=3")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn"
      )
      .toDF().show()
    //统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
    /*
           select
            paperviewid,
            paperviewname,
            score_segment,
            concat_ws(",",collect_set(userid)) userids,
            dt,
            dn
          from(
            select
              paperviewid,
              paperviewname,
              score,
              userid,
              (case when score>=0 and score <20 then 0-20
                when score >=20 and score<40 then 20-40
                when score>=40 and score<60 then 40-60
                when score>=60 and score<80 then 60-80
                when score>=80 and score<=100 then 80-100
                else 'other' end) score_segment
              dt,
              dn
            from dws.dws_user_paper_detail
            where dt='20190722')
          group by paperviewid,paperviewname,score_segment,dt,dn
          order by paperviewid,score_segment
     */
    allDetail
      .where(s"dt='${date}'")
      .select("paperviewid", "paperviewname", "userid", "score", "dt", "dn")
      .withColumn("score_segment",
        when(col("score").between(0, 20), "0-20")
          .when(col("score").between(20, 40), "20-40")
          .when(col("score") > 40 && col("score") <= 60, "40-60")
          .when(col("score") > 60 && col("score") <= 80, "60-80")
          .when(col("score") > 80 && col("score") <= 100, "80-100")
      ).drop("score").groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
      .agg(concat_ws(",", collect_list(col("userid").cast("string").as("userids"))).as("userids"))
      .orderBy("paperviewid", "score_segment")
      .toDF().show()

    //统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60

    /*
    select
      *
     from(
       select
          paperviewid,
          paperviewname,
          sum(if(score<60),1,0) unpasscount,
          sum(if(score>=60),1,0) passcount,
          cast(sum(if(score<60),1,0) / sum(if(score>=60),1,0) as decimal 10,2) rate  ??
          dt,
          dn
        from dws.dws_user_paper_detail
        group by paperviewid,paperviewname,dt,dn)
     order by rate

     */

    val unPassDetail = allDetail
      .select("paperviewid", "paperviewname", "dn", "dt")
      .where(s"dt='${date}'")
      .where("score between 0 and 60")
      .groupBy("paperviewid", "paperviewname", "dn", "dt")
      .agg(count("paperviewid").as("unpasscount"))

    val passDetail = allDetail.select("paperviewid", "dn")
      .where(s"dt='${date}'")
      .where("score >60")
      .groupBy("paperviewid", "paperviewname", "dn", "dt")
      .agg(count("paperviewid").as("passcount"))

    unPassDetail.join(passDetail, Seq("paperviewid", "dn")).
      withColumn("rate", (col("passcount")./(col("passcount") + col("unpasscount")))
        .cast("decimal(4,2)"))
      .select("paperviewid", "paperviewname", "unpasscount", "passcount", "rate", "dt", "dn")
      .show()
    //.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")

    //    allDetail.unpersist()

    //统计各题的错误数，正确数，错题率
    /*
    select
      *
     from(
       select
          questionid,
          paperviewname,
          sum(if(user_question_answer=1),1,0) right,
          sum(if(user_question_answer=1),1,0) err,
          cast(sum(if(user_question_answer=0),1,0) / sum(if(user_question_answer=1),1,0) as decimal 10,2) rate ??
          dt,
          dn
        from dws.dws_user_paper_detail
        group by questionid,dt,dn)
     order by rate

     */


    val userQuestionError = allDetail
      .select("questionid", "dt", "dn", "user_question_answer")
      .where(s"dt='${date}'")
      .where("user_question_answer='0'").drop("user_question_answer")
      .groupBy("questionid", "dt", "dn")
      .agg(count("questionid").as("errcount"))

    val userQuestionRight = allDetail.select("questionid", "dn", "user_question_answer")
      .where(s"dt='${date}'").where("user_question_answer='1'").drop("user_question_answer")
      .groupBy("questionid", "dn")
      .agg(count("questionid").as("rightcount"))

    userQuestionError.join(userQuestionRight, Seq("questionid", "dn"))
      .withColumn("rate", (col("errcount") / (col("errcount") + col("rightcount"))).cast("decimal(4,2)"))
      .orderBy(desc("errcount")).coalesce(1)
      .select("questionid", "errcount", "rightcount", "rate", "dt", "dn")
      .show()
    //.write.mode(SaveMode.Append).insertInto("ads.ads_user_question_detail")

    allDetail
      .select("questionid", "paperviewname")
      .where(s"dt='${date}'")
      .groupBy("questionid", "dt", "dn")
      .agg(sum(col("user_question_answer").when(col("user_question_ansewer").equalTo("1"), 1)),
        sum(col("user_question_answer").when(col("user_question_answer").equalTo("0"), 1)),
        sum(col("user_question_answer"))
      )
      .agg(sum(col("user_question_answer")
        .when(col("user_question_ansewer")
          .equalTo("1"), 1)) / sum(col("user_question_answer"))).as("rightRate")
      .agg(sum(col("user_question_answer")
        .when(col("user_question_ansewer")
          .equalTo("0"), 1)) / sum(col("user_question_answer"))).as("errRate")
      .select("questionid", "dt", "dn", "rightRate", "errRate")
      .orderBy("errRate")

  }
}
