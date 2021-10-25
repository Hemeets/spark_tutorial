package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WorldCount {
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

    // spark框架提供了更多功能，可以将分组和聚合使用一个方法实现
    // reduceByKey: 相同的key数据可以对value进行reduce聚合
//    wordtoone.reduceByKey((x, y) => {x + y})
    val wordtocount: RDD[(String, Int)] = wordtoone.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordtocount.collect()
    array.foreach(println)


    // TODO 关闭连接
    sc.stop()

  }

}
