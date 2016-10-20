package org.training.spark.core

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.HashSet

/**
 * 得分最高的10部电影；看过电影最多的前10个人；女性看多最多的10部电影；男性看过最多的10部电影
 */
object TopKMovieAnalyzer {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    val WOMEN = "F"
    val MAN = "M"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    // Create a SparContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("TopKMovieAnalyzer")
    val sc = new SparkContext(conf)

    /**
     * Step 1: Create RDDs
     */
    val DATA_PATH = dataPath

    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")

    /**
     * Step 2: Extract columns from RDDs
     */

    //rating: RDD[Array(userID, movieID, ratings, timestamp)]
    val ratings = ratingsRdd.map(_.split("::")).map { x =>
      (x(0), x(1), x(2))
    }.cache


    /**
     * Step 3: analyze result
     */
    //得分最高的10部影片
    val topKScoreMostMovie = ratings.map{ x =>
      //(movieID,(score,1))
      (x._2, (x._3.toInt, 1))
    }.reduceByKey { (v1, v2) =>
      //聚合每一个movieID 的sum(score),sum(1) 也就是统计每一个影片的 总得分，总观看次数
      (v1._1 + v2._1, v1._2 + v2._2)
    }.map { x =>
      //每一部影片的总得分/总观看次数 得到影片实际得分 （得分，影片ID）
      (x._2._1.toFloat / x._2._2.toFloat, x._1)
    }.sortByKey(false). //根据得分进行排名
        take(10).
        foreach(println)

    //看过电影最多的前10个人
    //ratings:RDD[(userID, movieID, ratings)]
    val topKmostPerson = ratings.map{ x =>
      (x._1, 1)
    }.reduceByKey(_ + _).
        map(x => (x._2, x._1)).
        sortByKey(false).
        take(10).
        foreach(println)


    /**
      * Transfrom filmID to fileName
      */
    val moviesRdd = sc.textFile(DATA_PATH + "movies.dat")
    val movieID2Name = moviesRdd.map(_.split("::")).map { x =>
      (x(0), x(1))
    }.collect().toMap

    val usersRdd = sc.textFile(DATA_PATH + "users.dat").cache()
    //女性看多最多的10部电影
    //users: RDD[(userID, (gender, age))]

    val womenUsers = usersRdd.map(_.split("::")).map { x =>
      (x(0), x(1))
    }.filter(_._2.equals(WOMEN)).map(_._1).collect()

    //所有女性用户
    val broadcastWomenUsers: Broadcast[Array[String]] = sc.broadcast(womenUsers)
    //ratings:RDD[(userID, movieID, ratings)]
    println("women most like movie:")
    ratings.
      //过滤出女性用户
      filter(x => broadcastWomenUsers.value.contains(x._1)).
      //词频统计 key 电影ID
      map{ x => (x._2,1)}.
      reduceByKey(_ + _).
      map(x => (x._2,x._1)).
      //排序
      sortByKey(false).
      map(x => (x._2,x._1)).
      take(10).
      map(x => (movieID2Name.getOrElse(x._1,null),x._2)).
      foreach(println)

    val menUsers = usersRdd.map(_.split("::")).map { x =>
      (x(0), x(1))
    }.filter(_._2.equals(MAN)).map(_._1).collect()

    //所有男性用户
    val broadcastMenUsers: Broadcast[Array[String]] = sc.broadcast(menUsers)
    //ratings:RDD[(userID, movieID, ratings)]
    println("men most like movie:")
    ratings.
      //过滤出男性用户
      filter(x => broadcastMenUsers.value.contains(x._1)).
      //词频统计 key 电影ID
      map{ x => (x._2,1)}.
      reduceByKey(_ + _).
      map(x => (x._2,x._1)).
      //排序
      sortByKey(false).
      map(x => (x._2,x._1)).
      take(10).
      map(x => (movieID2Name.getOrElse(x._1,null),x._2)).
      foreach(println)

    sc.stop()
  }
}
