package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwsUserPaperDetailQzDao {

  def queryDwsQzChapter(spark: SparkSession, date: String) = {
    spark.sql(s"select chapterid,chaptername,sequence as chapter_sequence," +
      s"showstatus as chapter_showstatus,status as chapter_status,chapter_creator,chapter_createtime,chapter_courseid," +
      s"chapternum,chapterallnum,outchapterid,chapterlistname,pointid,questype,pointname,pointyear,chapter,excisenum," +
      s"pointdescribe,pointlevel,typelist,point_score,thought,remid,pointnamelist," +
      s"typelistids,pointlist,dn from dws.dws_qz_chapter where dt='${date}'")
  }

  def queryDwsQzCourse(spark: SparkSession, date: String) = {
    spark.sql(s"select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence as course_sequence," +
      "status as course_status,sitecourse_creator as course_creator,sitecourse_createtime as course_createtime," +
      "helppaperstatus,servertype,boardid,showstatus," +
      s"coursename,isadvc,chapterlistid,pointlistid,courseeduid,edusubjectid,dn from dws.dws_qz_course where dt='${date}'")
  }

  def queryDwsQzMajor(spark: SparkSession, date: String) = {
    spark.sql(s"select majorid,businessid,majorname,shortname,status as major_status,sequence  as major_sequence," +
      "major_creator,major_createtime,businessname,sitename,domain,multicastserver,templateserver,multicastgateway,multicastport," +
      s"dn from dws.dws_qz_major where dt='${date}'")
  }

  def queryDwsQzPaper(spark: SparkSession, date: String) = {
    spark.sql(s"select paperviewid,paperviewname,paperparam,openstatus,explainurl,iscontest,contesttime, " +
      s"conteststarttime,contestendtime,contesttimelimit,dayiid,status as paper_status,paper_view_creator, " +
      s"paper_view_createtime,paperviewcatid,modifystatus,description,paperuse,testreport, " +
      s"centerid,sequence as paper_sequence,centername,centeryear,centertype,provideuser,centerviewtype, " +
      s"stage as paper_stage,papercatid,paperyear,suitnum,papername, " +
      s"totalscore,dn from dws.dws_qz_paper where dt='${date}'")
  }

  def queryDwsQzQue(spark: SparkSession, date: String) = {
    spark.sql(s"select questionid,parentid as question_parentid,questypeid,quesviewtype," +
      s"content as question_content, answer as question_answer,analysis as question_analysis," +
      s"limitminute as question_limitminute, score as question_score,splitscore,lecture,creator as question_creator, " +
      s"createtime as question_createtime,modifystatus as question_modifystatus, " +
      s"attanswer as question_attanswer,questag as question_questag,vanalysisaddr as question_vanalysisaddr, " +
      s"difficulty as question_difficulty,quesskill,vdeoaddr,description as question_description, " +
      s"splitscoretype as question_splitscoretype,dn from dws.dws_qz_question where dt='${date}'")
  }

  def queryDwdMemberPaperQue(spark: SparkSession, date: String) = {
    spark.sql(s"select userid,paperviewid,chapterid,sitecourseid,questionid,majorid,useranswer,istrue," +
      s"lasttime,opertype,paperid,spendtime,score,question_answer, dt,dn from dwd.dwd_qz_member_paper_question where dt='${date}'")
  }
}
