package com.share.spark.project.domain

/**
  * Created by share on 2018/4/6.
  * 实战课程点击数实体类
  */

/**
  * @param day_course 对应HBase中的Rowkey, 20171111_1
  * @param click_count 对应的20171111_1的访问数
  */
case class CourseClickCount(day_course:String, click_count:Long)
