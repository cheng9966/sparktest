package com.huajie.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-2.7.3")
    System.setProperty("HADOOP_USER_NAME", "lisa")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("WC")
    val sc = new SparkContext(conf)

    val arrayAdd = Array("hello spark", " hello scala", "hello flink")
    val inputRdd = sc.parallelize(arrayAdd)
    val outputRdd = inputRdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    println("")
    println("================内容打印==============")
    outputRdd.foreach(println(_))
    println("")
    println("======================================")

  }

}
