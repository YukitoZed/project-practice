package com.atguigu.service

import com.atguigu.dao._
import com.atguigu.util.QzConstants
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DwsqzService {

  /**
    * 章节维度表
    * @param spark
    * @param date
    */
  def createDwsQzChapter(spark: SparkSession, date: String) = {

    val dwdQzChapter: DataFrame = DwdQzChapterDao.queryDwdQzChapter(spark, date)
    val dwdQzChapterList: DataFrame = DwdQzChapterDao.queryDwdQzChapterList(spark, date)
    val dwdQzPoint: DataFrame = DwdQzChapterDao.queryDwdQzPoint(spark, date)
    val dwdQzPointQue: DataFrame = DwdQzChapterDao.queryDwdQzPointQuestion(spark, date)

    val result: DataFrame = dwdQzChapter
      .join(dwdQzChapterList, Seq("chapterlistid", "dn"))
      .join(dwdQzPoint, Seq("chapterid", "dn"))
      .join(dwdQzPointQue, Seq("pointid", "dn"))

    result.select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus","status",
      "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
      "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
      "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn"
    ).coalesce(1).write.mode(SaveMode.Append).insertInto(QzConstants.DWS_QZ_CHAPTER_TABLE)

    println("章节维度表导入完毕")
  }

  /**
    * 课程维度表
    * @param spark
    * @param date
    */
  def createDwsQzCourse(spark: SparkSession, date: String) = {
    val dwdQzCourse: DataFrame = DwdQzCourseDao.queryDwdQzCourse(spark, date)
    val dwdQzCourseEduSub: DataFrame = DwdQzCourseDao.queryDwdQzCourseEduSubject(spark, date)
    val dwdQzSiteCourse: DataFrame = DwdQzCourseDao.queryDwdQzSiteCourse(spark, date)


    val result: DataFrame = dwdQzSiteCourse
      .join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEduSub, Seq("courseid", "dn"))

    result.select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter", "sequence", "status"
      , "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid", "showstatus", "majorid",
      "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Append).insertInto(QzConstants.DWS_QZ_COURSE_TABLE)

    println("课程维度表导入完毕")
  }

  /**
    * 主修维度表
    * @param spark
    * @param date
    */
  def createDwsQzMajor(spark: SparkSession, date: String) = {
    val dwdQzBusiness: DataFrame = DwdQzMajorDao.queryDwdQzBusiness(spark, date)
    val dwdQzMajor: DataFrame = DwdQzMajorDao.queryDwdQzMajor(spark, date)
    val dwdQzWebsite: DataFrame = DwdQzMajorDao.queryDwdQzWebsite(spark, date)

    val result = dwdQzMajor
      .join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
    .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
      "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
      "multicastgateway", "multicastport", "dt", "dn")
  result.coalesce(1).write.mode(SaveMode.Append).insertInto(QzConstants.DWS_QZ_MAJOR_TABLE)

  println("主修维度表导入完毕")
  }

  /**
    * 试卷维度表
    * @param spark
    * @param date
    */
  def createDwsQzPaper(spark: SparkSession, date: String)={
    val dwdQzCenter: DataFrame = DwdQzPaperDao.queryDwdQzCenter(spark,date)
    val dwdQzCenterPaper: DataFrame = DwdQzPaperDao.queryDwdQzCenterPaper(spark,date)
    val dwdQzPaper: DataFrame = DwdQzPaperDao.queryDwdQzPaper(spark,date)
    val dwdQzPaperView: DataFrame = DwdQzPaperDao.queryDwdQzPaperView(spark,date)

    val result: DataFrame = dwdQzPaperView
      .join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))

    result.select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
      , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
      "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
      "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
      "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
      "dt", "dn").coalesce(1).write.mode(SaveMode.Append).insertInto(QzConstants.DWS_QZ_PAPER_TABLE)

    println("试卷维度表导入完毕")
  }

  /**
    * 题目维度表
    * @param spark
    * @param date
    */
  def createDwsQzQue(spark:SparkSession,date:String)={
    val dwdQzQue: DataFrame = DwdQzQueDao.queryDwdQzQue(spark,date)
    val dwdQzQueType: DataFrame = DwdQzQueDao.queryDwdQzQueType(spark,date)

    val result: DataFrame = dwdQzQue.join(dwdQzQueType,Seq("questypeid","dn"))

    result.select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
      , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
      , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
      "remark", "splitscoretype", "dt", "dn").coalesce(1).write.mode(SaveMode.Append).insertInto(QzConstants.DWS_QZ_QUE_TABLE)

    println("题目维度表导入完毕")
  }

  def createDwsUserPaperDetail(spark:SparkSession,date:String)={
    val dwdMemberPaperQue: DataFrame = DwsUserPaperDetailQzDao.queryDwdMemberPaperQue(spark,date)
      .withColumnRenamed("question_answer", "user_question_answer")
    val dwsChapter: DataFrame = DwsUserPaperDetailQzDao.queryDwsQzChapter(spark,date)
    val dwsCourse: DataFrame = DwsUserPaperDetailQzDao.queryDwsQzCourse(spark,date)
    val dwsMajor: DataFrame = DwsUserPaperDetailQzDao.queryDwsQzMajor(spark,date)
    val dwsPaper: DataFrame = DwsUserPaperDetailQzDao.queryDwsQzPaper(spark,date)
    val dwsQue: DataFrame = DwsUserPaperDetailQzDao.queryDwsQzQue(spark,date)

    val result: DataFrame = dwdMemberPaperQue
      .join(dwsCourse, Seq("sitecourseid", "dn"))
      .join(dwsChapter, Seq("chapterid", "dn"))
      .join(dwsMajor, Seq("majorid", "dn"))
      .join(dwsPaper, Seq("paperviewid", "dn"))
      .join(dwsQue, Seq("questionid", "dn"))

    result.select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
      "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
      "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
      , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
      "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
      , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
      "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
      "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
      "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
      "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
      "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
      "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
      "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
      "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
      "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
      "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
      "question_splitscoretype", "user_question_answer", "dt", "dn"
    ).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWS_QZ_USER_PAPER_DETAIL_TABLE)

    println("宽表导入完毕")
  }

}
