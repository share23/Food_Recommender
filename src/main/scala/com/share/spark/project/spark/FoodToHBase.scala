package com.share.spark.project.spark

import com.share.spark.project.dao.CourseClickCountDAO
import com.share.spark.project.domain.FoodInfo
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by share on 2018/4/11.
  */
object FoodToHBase {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("FoodRecommendStreamingApp").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)

    //初始化foods表
    val food = sparkContext.textFile("hdfs://hadoop:8020/data/input/food1").map { lines =>
      val fields = lines.split("::")
      new FoodInfo(fields(0), fields(1))
    }.cache()

    var rowId1 = 1
    food.foreach { food =>
      CourseClickCountDAO.put("foods", rowId1.toString, "info", "id", food.fid)
      CourseClickCountDAO.put("foods", rowId1.toString, "info", "name", food.name)
      rowId1 += 1
    }
    sparkContext.stop()
  }
}
