package com.lh.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("D:\\tmp\\test")

        val rdd: RDD[String] = sc.textFile("D:\\workLv\\learn\\proj\\hadoop-code\\bigDataSolve\\spark-core\\src\\main" +
          "\\java\\com\\lh\\core\\wc\\Spark01_WordCount.scala")

        // 建议对checkpoint()的RDD 使用Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存中读取数据即可，否则需要再从头计算一次RDD。
        val rddCache: rdd.type = rdd.cache()
        rddCache.checkpoint()

        val rdd1: RDD[(String, Int)] = rddCache.map((_, 1))
        val rdd2: RDD[(String, Int)] = rddCache.map((_, 2))

//        rdd1.collect()
        rdd2.collect()

        println(rdd1.toDebugString)
        println(rdd2.toDebugString)




        sc.stop()

    }
}
