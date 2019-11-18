package com.atguigu.service

import com.alibaba.fastjson.JSONObject
import com.atguigu.bean.{PaperView, PointInfo, QuestionInfo}
import com.atguigu.util.{QzConstants, QzParseJsonData}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwdqzService {

  /**
    * 主题数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzCenterLogETL(ssc: SparkContext, spark: SparkSession): Unit = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZCENTERLOG_PATH)
    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val centerid: Int = jsonObject.getIntValue("centerid")
            val centername: String = jsonObject.getString("centername")
            val centeryear: String = jsonObject.getString("centeryear")
            val centertype: String = jsonObject.getString("centertype")
            val openstatus: String = jsonObject.getString("openstatus")
            val centerparam: String = jsonObject.getString("centerparam")
            val description: String = jsonObject.getString("description")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val sequence: String = jsonObject.getString("sequence")
            val provideuser: String = jsonObject.getString("provideuser")
            val centerviewtype: String = jsonObject.getString("centerviewtype")
            val stage: String = jsonObject.getString("stage")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
              sequence, provideuser, centerviewtype, stage, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_CENTER_TABLE)
  }

  /**
    * 试卷主题关联数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzCenterPaperETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZCENTERPAPERLOG_PATH)
    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val paperviewid: Int = jsonObject.getIntValue("paperviewid")
            val centerid: Int = jsonObject.getIntValue("centerid")
            val openstatus: String = jsonObject.getString("openstatus")
            val sequence: String = jsonObject.getString("sequence")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_CENTER_PAPER_TABLE)
  }

  /**
    * 章节数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzChapterLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZCHAPTERLOG_PATH)
    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val chapterid: Int = jsonObject.getIntValue("chapterid")
            val chapterlistid: Int = jsonObject.getIntValue("chapterlistid")
            val chapterlistname: String = jsonObject.getString("chapterlistname")
            val sequence: String = jsonObject.getString("sequence")
            val showstatus: String = jsonObject.getString("showstatus")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val courseid: Int = jsonObject.getIntValue("courseid")
            val chapternum: Int = jsonObject.getIntValue("chapternum")
            val outchapterid: Int = jsonObject.getIntValue("outchapterid")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (chapterid, chapterlistid, chapterlistname, sequence, showstatus, creator, createtime, courseid, chapternum,
              outchapterid, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_CHAPTER_TABLE)
  }

  /**
    * 章节列表数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzChapterListLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZCHAPTERLISTLOG_PATH)
    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val chapterlistid: Int = jsonObject.getIntValue("chapterlistid")
            val chapterlistname: String = jsonObject.getString("chapterlistname")
            val courseid: Int = jsonObject.getIntValue("courseid")
            val chapterallnum: Int = jsonObject.getIntValue("chapterallnum")
            val sequence: String = jsonObject.getString("sequence")
            val status: String = jsonObject.getString("status")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_CHAPTERLIST_TABLE)
  }

  /**
    * 题库数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzCourseETL(ssc: SparkContext, spark: SparkSession) = {

    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZCOURSELOG_PATH)
    import spark.implicits._


    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val courseeduid: Int = jsonObject.getIntValue("courseeduid")
            val majorid: Int = jsonObject.getIntValue("majorid")
            val coursename: String = jsonObject.getString("coursename")
            val coursechapter: String = jsonObject.getString("coursechapter")
            val sequence: String = jsonObject.getString("sequence")
            val isadvc: String = jsonObject.getString("isadvc")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val status: String = jsonObject.getString("status")
            val chapterlistid: Int = jsonObject.getIntValue("chapterlistid")
            val pointlistid: Int = jsonObject.getIntValue("pointlistid")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (courseeduid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime,
              status, chapterlistid, pointlistid, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_COURSE_TABLE)

  }

  /**
    * 课程辅导数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzCourseEduSubjectLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZCOURSEEDUSUJECTLOG_PATH)
    import spark.implicits._


    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val courseeduid: Int = jsonObject.getIntValue("courseeduid")
            val edusubjectid: Int = jsonObject.getIntValue("edusubjectid")
            val courseid: Int = jsonObject.getIntValue("courseid")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val majorid: Int = jsonObject.getIntValue("majorid")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_COURSE_EDUSUBJECT_TABLE)
  }

  /**
    * 主修数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzMajorLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZMAJORLOG_PATH)
    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val majorid: Int = jsonObject.getIntValue("majorid")
            val businessid: Int = jsonObject.getIntValue("businessid")
            val siteid: Int = jsonObject.getIntValue("siteid")
            val majorname: String = jsonObject.getString("majorname")
            val shortname: String = jsonObject.getString("shortname")
            val status: String = jsonObject.getString("status")
            val sequence: String = jsonObject.getString("sequence")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val column_sitetype: String = jsonObject.getString("columm_sitetype")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (majorid, businessid, siteid, majorname, shortname, status, sequence, creator,
              createtime, column_sitetype, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_MAJOR_TABLE)

  }

  /**
    * 学员做题数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzMemberPaperQueLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZMEMBERPAPERQUELOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val userid: Int = jsonObject.getIntValue("userid")
            val paperviewid: Int = jsonObject.getIntValue("paperviewid")
            val chapterid: Int = jsonObject.getIntValue("chapterid")
            val sitecourseid: Int = jsonObject.getIntValue("sitecourseid")
            val questionid: Int = jsonObject.getIntValue("questionid")
            val majorid: Int = jsonObject.getIntValue("majorid")
            val useranswer: String = jsonObject.getString("useranswer")
            val istrue: String = jsonObject.getString("istrue")
            val lasttime: String = jsonObject.getString("lasttime")
            val opertype: String = jsonObject.getString("opertype")
            val paperid: Int = jsonObject.getIntValue("paperid")
            val spendtime: Int = jsonObject.getIntValue("spendtime")
            val score: String = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP).toString()
            val question_answer: Int = jsonObject.getIntValue("question_answer")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype, paperid,
              spendtime, score, question_answer, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_MEMBER_PAPER_QUESTION_TABLE)
  }

  /**
    * 做题试卷日志数据
    *
    * @param ssc
    * @param spark
    */
  def qzPaperLogETL(ssc: SparkContext, spark: SparkSession) = {

    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZPAPERLOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val paperid: Int = jsonObject.getIntValue("paperid")
            val papercatid: Int = jsonObject.getIntValue("papercatid")
            val courseid: Int = jsonObject.getIntValue("courseid")
            val paperyear: String = jsonObject.getString("paperyear")
            val chapter: String = jsonObject.getString("chapter")
            val suitnum: String = jsonObject.getString("suitnum")
            val papername: String = jsonObject.getString("papername")
            val status: String = jsonObject.getString("status")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val totalscore: String = BigDecimal(jsonObject.getDouble("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP).toString()
            val chapterid: Int = jsonObject.getIntValue("chapterid")
            val chapterlistid: Int = jsonObject.getIntValue("chapterlistid")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator,
              createtime, totalscore, chapterid, chapterlistid, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_PAPER_TABLE)
  }

  /**
    * 试卷视图数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzPaperViewLogETL(ssc: SparkContext, spark: SparkSession) = {

    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZPAPERVIEWLOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val paperviewid: Int = jsonObject.getIntValue("paperviewid")
            val paperid: Int = jsonObject.getIntValue("paperid")
            val paperviewname: String = jsonObject.getString("paperviewname")
            val paperparam: String = jsonObject.getString("paperparam")
            val openstatus: String = jsonObject.getString("openstatus")
            val explainurl: String = jsonObject.getString("explainurl")
            val iscontest: String = jsonObject.getString("iscontest")
            val contesttime: String = jsonObject.getString("contesttime")
            val conteststarttime: String = jsonObject.getString("conteststarttime")
            val contestendtime: String = jsonObject.getString("contestendtime")
            val contesttimelimit: String = jsonObject.getString("contesttimelimit")
            val dayiid: Int = jsonObject.getIntValue("dayiid")
            val status: String = jsonObject.getString("status")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val paperviewcatid: Int = jsonObject.getIntValue("paperviewcatid")
            val modifystatus: String = jsonObject.getString("modifystatus")
            val description: String = jsonObject.getString("description")
            val papertype: String = jsonObject.getString("papertype")
            val downurl: String = jsonObject.getString("downurl")
            val paperuse: String = jsonObject.getString("paperuse")
            val paperdifficult: String = jsonObject.getString("paperdifficult")
            val testreport: String = jsonObject.getString("testreport")
            val paperuseshow: String = jsonObject.getString("paperuseshow")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")

            PaperView(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime, conteststarttime,
              contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus, description,
              papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_PAPER_VIEW_TABLE)

  }

  /**
    * 知识点数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzPointLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZPOINTLOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val pointid: Int = jsonObject.getIntValue("pointid")
            val courseid: Int = jsonObject.getIntValue("courseid")
            val pointname: String = jsonObject.getString("pointname")
            val pointyear: String = jsonObject.getString("pointyear")
            val chapter: String = jsonObject.getString("chapter")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val status: String = jsonObject.getString("status")
            val modifystatus: String = jsonObject.getString("modifystatus")
            val excisenum: Int = jsonObject.getIntValue("excisenum")
            val pointlistid: Int = jsonObject.getIntValue("pointlistid")
            val chapterid: Int = jsonObject.getIntValue("chapterid")
            val sequece: String = jsonObject.getString("sequece")
            val pointdescribe: String = jsonObject.getString("pointdescribe")
            val pointlevel: String = jsonObject.getString("pointlevel")
            val typelist: String = jsonObject.getString("typelist")
            val score: String = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP).toString()
            val thought: String = jsonObject.getString("thought")
            val remid: String = jsonObject.getString("remid")
            val pointnamelist: String = jsonObject.getString("pointnamelist")
            val typelistids: String = jsonObject.getString("typelistids")
            val pointlist: String = jsonObject.getString("pointlist")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            PointInfo(pointid, courseid, pointname, pointyear, chapter, creator, createtime, status, modifystatus,
              excisenum, pointlistid, chapterid, sequece, pointdescribe, pointlevel, typelist, score, thought, remid,
              pointnamelist, typelistids, pointlist, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_POINT_TABLE)
  }

  /**
    * 做题知识点关联数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzPointQueLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZPOINTQUELOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)

            val pointid: Int = jsonObject.getIntValue("pointid")
            val questionid: Int = jsonObject.getIntValue("questionid")
            val questype: Int = jsonObject.getIntValue("questype")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (pointid, questionid, questype, creator, createtime, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_POINTQUETABLE)
  }

  /**
    * 做题日志数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzQueLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZQUELOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val questionid: Int = jsonObject.getIntValue("questionid")
            val parentid: Int = jsonObject.getIntValue("parentid")
            val questypeid: Int = jsonObject.getIntValue("questypeid")
            val quesviewtype: Int = jsonObject.getIntValue("quesviewtype")
            val content: String = jsonObject.getString("content")
            val answer: String = jsonObject.getString("answer")
            val analysis: String = jsonObject.getString("analysis")
            val limitminute: String = jsonObject.getString("limitminute")
            val score: String = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP).toString()
            val splitscore: String = BigDecimal(jsonObject.getDouble("splitscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP).toString()
            val status: String = jsonObject.getString("status")
            val optnum: Int= jsonObject.getIntValue("optnum")
            val lecture: String = jsonObject.getString("lecture")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val modifystatus: String = jsonObject.getString("modifystatus")
            val attanswer: String = jsonObject.getString("attanswer")
            val questag: String = jsonObject.getString("questag")
            val vanalysisaddr: String = jsonObject.getString("vanalysisaddr")
            val difficulty: String = jsonObject.getString("difficulty")
            val quesskill: String = jsonObject.getString("quesskill")
            val vdeoaddr: String = jsonObject.getString("vdeoaddr")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            QuestionInfo(questionid, parentid, questypeid, quesviewtype, content, answer, analysis,
              limitminute, score, splitscore, status, optnum, lecture, creator, createtime, modifystatus,
              attanswer, questag, vanalysisaddr, difficulty, quesskill, vdeoaddr, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_QUE)
  }

  /**
    * 题目类型数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzQueTypeLogETL(ssc: SparkContext, spark: SparkSession) = {

    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZQUETYPELOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val quesviewtype: Int = jsonObject.getIntValue("quesviewtype")
            val viewtypename: String = jsonObject.getString("viewtypename")
            val questypeid: Int = jsonObject.getIntValue("questypeid")
            val description: String = jsonObject.getString("description")
            val status: String = jsonObject.getString("status")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val papertypename: String = jsonObject.getString("papertypename")
            val sequence: String = jsonObject.getString("sequence")
            val remark: String = jsonObject.getString("remark")
            val splitscoretype: String = jsonObject.getString("splitscoretype")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (quesviewtype, viewtypename, questypeid, description, status,
              creator, createtime, papertypename, sequence, remark, splitscoretype, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_QUETYPETABLE)
  }

  /**
    * 网站课程日志数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzSiteCourseLogETL(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZSITECOURSELOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val sitecourseid: Int = jsonObject.getIntValue("sitecourseid")
            val siteid: Int = jsonObject.getIntValue("siteid")
            val courseid: Int = jsonObject.getIntValue("courseid")
            val sitecoursename: String = jsonObject.getString("sitecoursename")
            val coursechapter: String = jsonObject.getString("coursechapter")
            val sequence: String = jsonObject.getString("sequence")
            val status: String = jsonObject.getString("status")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val helppaperstatus: String = jsonObject.getString("helpparperstatus")
            val servertype: String = jsonObject.getString("servertype")
            val boardid: Int = jsonObject.getIntValue("boardid")
            val showstatus: String = jsonObject.getString("showstatus")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status,
              creator, createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
          }
        )
      }).toDF().toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_SITE_COURSE_TABLE)

  }

  /**
    * 网站日志数据清洗
    *
    * @param ssc
    * @param spark
    */
  def qzWebsiteLogETL(ssc: SparkContext, spark: SparkSession) = {

    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZWEBSITELOG_PATH)

    import spark.implicits._

    inputRDD.filter(item => {
      val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
      jsonObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(partitions => {
        partitions.map(
          item => {
            val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
            val siteid: Int = jsonObject.getIntValue("siteid")
            val sitename: String = jsonObject.getString("sitename")
            val domain: String = jsonObject.getString("domain")
            val sequence: String = jsonObject.getString("sequence")
            val multicastserver: String = jsonObject.getString("multicastserver")
            val templateserver: String = jsonObject.getString("templateserver")
            val status: String = jsonObject.getString("status")
            val creator: String = jsonObject.getString("creator")
            val createtime: String = jsonObject.getString("createtime")
            val multicastgateway: String = jsonObject.getString("multicastgateway")
            val multicastport: String = jsonObject.getString("multicastport")
            val dt: String = jsonObject.getString("dt")
            val dn: String = jsonObject.getString("dn")
            (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
              multicastgateway, multicastport, dt, dn)
          }
        )
      }).toDF().coalesce(3).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_WEBSITE_TABLE)
  }


  /**
    * 解析日志导入表中，简单数据清洗,要求对所有score 分数字段进行保留1位小数并且四舍五入
    *
    * @param ssc
    * @param spark
    */
  def qzBusinessLogETL(ssc: SparkContext, spark: SparkSession) = {

    /*
    {
      "businessid": 0,
      "businessname": "bsname0",
      "createtime": "2019-07-22 10:40:54",
      "creator": "admin",
      "dn": "webA",
      "dt": "20190722",
      "sequence": "-",
      "siteid": 1,
      "status": "-"
    }
     */

    val inputRDD: RDD[String] = ssc.textFile(QzConstants.HDFS_QZBUSINESSLOG_PATH)

    import spark.implicits._

    inputRDD
      .filter(
        item => {
          val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
          jsonObject.isInstanceOf[JSONObject]
        }
      )
      .mapPartitions(
        partitions => {
          partitions.map(
            item => {
              val jsonObject: JSONObject = QzParseJsonData.parseObject(item)
              val businessid: Int = jsonObject.getIntValue("businessid")
              val businessname: String = jsonObject.getString("businessname")
              val sequence: String = jsonObject.getString("sequence")
              val status: String = jsonObject.getString("status")
              val creator: String = jsonObject.getString("creator")
              val createtime: String = jsonObject.getString("createtime")
              val dt: String = jsonObject.getString("dt")
              val dn: String = jsonObject.getString("dn")
              val siteid: Int = jsonObject.getIntValue("siteid")
              (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto(QzConstants.DWD_QZ_BUSINESS_TABLE)

  }

}
