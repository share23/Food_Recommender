package com.share.spark.project.dao

import com.share.spark.project.domain.CourseClickCount
import com.share.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by share on 2018/4/6.
  */
object CourseClickCountDAO {
  val tableName = "course_share"
  val cf = "info"
  val qualifer = "click_count"
  /**
    * 保存数据到HBase
    * @param list  CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }
  /**
    * 根据rowkey查询值
    */
  def count(day_course: String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)
    if(value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  //写入
  def put(tableName: String, rowKey: String, family: String, qualifier: String, value: String) {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val put = new Put(Bytes.toBytes(rowKey))
    /*qualifierValue.map(x => {
      if (!(x._2.isEmpty))
        put.add(Bytes.toBytes(family), Bytes.toBytes(x._1), Bytes.toBytes(x._2))
    })*/
    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    table.put(put)
  }

  //获得所有行健
  def getAllRow(tableName: String): Array[String] = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val resultScaner = table.getScanner(new Scan())
    val resIter = resultScaner.iterator()
    var resArr = new ArrayBuffer[String]()
    while (resIter.hasNext) {
      val res = resIter.next()
      if (res != null && !res.isEmpty) {
        resArr += Bytes.toString(res.getRow)
      }
    }
    resArr.toArray
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8))
    list.append(CourseClickCount("20171111_9",9))
    list.append(CourseClickCount("20171111_1",100))
    save(list)
    println(count("20171111_8") + " : " + count("20171111_9")+ " : " + count("20171111_1"))
  }
}
