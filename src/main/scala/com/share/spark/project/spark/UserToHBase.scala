package com.share.spark.project.spark

import com.share.spark.project.dao.CourseClickCountDAO
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by share on 2018/4/11.
  */
object UserToHBase {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("FoodRecommendStreamingApp").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)

    //初始化users表
    for (i <- 1 to 100) {
      val uid = i.toString
      val pwd = "share123456"
      CourseClickCountDAO.put("users", i.toString, "info", "uid", uid)
      CourseClickCountDAO.put("users", i.toString, "info", "pwd", pwd)
    }
    sparkContext.stop()
  }
}
