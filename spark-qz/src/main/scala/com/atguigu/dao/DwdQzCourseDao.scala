package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwdQzCourseDao {

  def queryDwdQzSiteCourse(spark:SparkSession,date:String)={
    spark.sql(s"select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence,status," +
      s"creator as sitecourse_creator,createtime as sitecourse_createtime, helppaperstatus,servertype,boardid,showstatus," +
      s"dt, dn from dwd.dwd_qz_site_course " +
      s" where dt='${date}'")
  }

  def queryDwdQzCourse(spark:SparkSession,date:String)={

    spark.sql(s"select courseid,majorid,coursename,isadvc,chapterlistid,pointlistid,dn from dwd.dwd_qz_course" +
      s" where dt='${date}'")
  }

  def queryDwdQzCourseEduSubject(spark:SparkSession,date:String)={

    spark.sql(s"select courseeduid,edusubjectid,courseid,dn from dwd.dwd_qz_course_edusubject" +
      s" where dt='${date}'")
  }

}
