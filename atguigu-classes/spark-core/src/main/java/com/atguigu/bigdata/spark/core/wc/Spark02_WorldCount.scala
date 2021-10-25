package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WorldCount {
  def main(args: Array[String]): Unit = {

    // Application

    // Spark 框架

    // TODO 建立和Spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1. 读取文件
    val lines: RDD[String] = sc.textFile("data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordtoone: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordtoone.groupBy(t => t._1)

    val wordtocount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        val wordcount: (String, Int) = list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
        wordcount
      }
    }

    val array: Array[(String, Int)] = wordtocount.collect()
    array.foreach(println)


    // TODO 关闭连接
    sc.stop()

  }

}
