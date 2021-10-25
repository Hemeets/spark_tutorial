package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WorldCount {
  def main(args: Array[String]): Unit = {

    // Application

    // Spark 框架

    // TODO 建立和Spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1. 读取文件
    val lines: RDD[String] = sc.textFile("data")

    // 2. 分词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 单词分组，便于统计
    val wordgroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 4. 对分组后数据进行转换
    val wordtocount = wordgroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordtocount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sc.stop()

  }

}
