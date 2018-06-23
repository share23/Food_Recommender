package com.share.spark.project.spark

import com.share.spark.project.dao.CourseClickCountDAO
import com.share.spark.project.domain.{FoodInfo, RatingInfo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by share on 2018/4/10.
  * 初始化 'users' 'foods' 'ratings' 三张表
  */
object RatingToHBase {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("FoodRecommendStreamingApp").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)

    //初始化ratings表
    val rating = sparkContext.textFile("hdfs://hadoop:8020/data/input/rating").map { lines =>
      val fields = lines.split("::")
      new RatingInfo(fields(0), fields(1), fields(2))
    }.cache()

    var rowId2 = 1
    rating.foreach { rating =>
      CourseClickCountDAO.put("ratings", rowId2.toString, "info", "uid", rating.uid)
      CourseClickCountDAO.put("ratings", rowId2.toString, "info", "fid", rating.fid)
      CourseClickCountDAO.put("ratings", rowId2.toString, "info", "rating", rating.rating)
      rowId2 = rowId2 + 1
    }
    sparkContext.stop()
  }
}
