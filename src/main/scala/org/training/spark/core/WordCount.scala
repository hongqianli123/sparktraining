package org.training.spark.core

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * <p>简要说明...</p>
  * HelloWord
  *
  * @author hongqianli
  * @version 1.0
  * @date 2016/10/20
  */
object WordCount {
  val INPUT = "hdfs://192.168.200.241:8020/user/hongqianli/test.txt"

  def getRDD(sc: SparkContext): RDD[String] = {
    println("获取数据了...")
    sc.textFile(INPUT,1)
  }

  def main(args: Array[String]) {
    val workaround = new File(".");
    System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
    new File("./bin").mkdirs();
    new File("./bin/winutils.exe").createNewFile();

    val conf = new SparkConf()
    conf.setAppName("wordcount")
    conf.setMaster("local[1]")
//    conf.setSparkHome("D:\\software\\spark-1.6.2-bin-hadoop2.6")

    val sc = new SparkContext(conf)
    val rdd:RDD[String] = getRDD(sc)
    val key: RDD[(String, Int)] = rdd
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    key.collect().toList.foreach(println)
    sc.stop()
  }


}
