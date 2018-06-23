package com.share.spark.project.spark

import com.share.spark.project.dao.CourseClickCountDAO
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark 和 Spark Streaming 分别对用户产生离线和实时的推荐结果
  */
object FoodRecommendStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //设置日志提示等级
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //args为 hadoop:2181 test streamingtopic 1
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("FoodRecommendStreamingApp").setMaster("local[2]").set("spark.akka.frameSize", "2000").set("spark.network.timeout", "1200")
    val sparkContext = new SparkContext(sparkConf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("zookeeper.session.timeout", "6000000")

    println("\n=====================step 2 load data==========================")
    //加载HBase中的数据

    //读取数据并转化成rdd
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "ratings")
    val ratingsData = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val hbaseRatings = ratingsData.map { case (_, res) =>
      val foodId = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("fid")))
      val rating = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("rating")))
      val userId = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("uid")))
      Rating(userId.toInt, foodId.toInt, rating.toDouble)
    }.cache()

    val numTrainRatings = hbaseRatings.count()
    println(s"[DEBUG]get $numTrainRatings train data from hbase")

    val rank = 10
    val lambda = 0.01
    val numIter = 10

    //第一次运行，初始化用户的推荐信息

      println("\n=====================system initiallizing...==========================")
      println("\n[DEBUG]training model...")
      val firstTrainTime = System.nanoTime()
      val model = ALS.train(hbaseRatings, rank, numIter, lambda)
      val firstTrainEndTime = System.nanoTime() - firstTrainTime
      println("[DEBUG]first training consuming:" + firstTrainEndTime / 1000000000 + "s")

      println("\n[DEBUG]save recommended data to hbase...")
      val firstPutTime = System.nanoTime()

      //为每一个用户产生初始的推荐食物，取top10
      for (i <- 1 to 60) {
        val topRatings = model.recommendProducts(i, 10)
        var recFoods = ""
        for (r <- topRatings) {
          val rating = r.rating.toString.substring(0, 4)
          recFoods += r.product + ":" + rating + ","
        }
        CourseClickCountDAO.put("users", i.toString, "info", "recFoods", recFoods.substring(0, recFoods.length - 1))
      }
      val firstPutEndTime = System.nanoTime() - firstPutTime
      println("[DEBUG]finish job consuming:" + firstPutEndTime / 1000000000 + "s")


    //实时推荐引擎部分
    println("\n=====================start real-time recommendation engine...==========================")
    val streamingTime = 120
    println(s"[DEBUG]The time interval to refresh model is: $streamingTime s")

    //接受实时的用户行为数据
//    val streamingContext = new StreamingContext(sparkContext, Seconds(streamingTime))
//    val ssc = new StreamingContext(sparkContext, Seconds(60))


    val ssc = new StreamingContext(sparkContext, Seconds(10))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // TODO... Spark Streaming 如何对接 Kafka

    val logs = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val cleanData = logs.map(line => {
      val infos =line.split("::")
      Rating(infos(0).toInt, infos(1).toInt, infos(2).toDouble)
    })

    var allData = hbaseRatings
    allData.cache.count()
    hbaseRatings.unpersist()
    var index = 0
    cleanData.foreachRDD { rdd =>
      index += 1
      println("\n[DEBUG]this round (" + index + ") received: " + rdd.count + " data lines.")
      val refreshStartTime = System.nanoTime()
      val tmpData = allData.union(rdd).cache
      tmpData.count()
      allData = tmpData
      tmpData.unpersist()
      allData = allData.union(rdd).repartition(10).cache()
      val model = ALS.train(allData, rank, numIter, lambda)
      val refreshEndTime = System.nanoTime() - refreshStartTime
      println("[DEBUG]training consuming:" + refreshEndTime / 1000000000 + " s")
      println("[DEBUG]begin refresh hbase user's recBooks...")
      val refreshAgainStartTime = System.nanoTime()

      //只更新当前有行为产生的用户的推荐数据
      val usersId = rdd.map(_.user).distinct().collect()
      for (u <- usersId) {
        val topRatings = model.recommendProducts(u, 10)
        var recFoods = ""
        for (r <- topRatings) {
          val rating = r.rating.toString.substring(0, 4)
          recFoods += r.product + ":" + rating + ","
        }
        CourseClickCountDAO.put("users", u.toString, "info", "recFoods", recFoods.substring(0, recFoods.length - 1))
      }
      val refreshAgainConsumingTime = System.nanoTime() - refreshAgainStartTime
      println("[DEBUG]finish refresh job,consuming:" + refreshAgainConsumingTime / 1000000000 + " s")
    }

      ssc.start()
      ssc.awaitTermination()
      sparkContext.stop()

  }
}
