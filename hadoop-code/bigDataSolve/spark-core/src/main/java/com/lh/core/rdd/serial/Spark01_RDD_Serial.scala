package com.lh.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf()
          .setMaster("local")
          .setAppName("WordCount")// 替换默认的序列化机制
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          // 注册需要使用 kryo 序列化的自定义类
          .registerKryoClasses(Array(classOf[Search]))
        val sc = new SparkContext(sparConf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "lh"), 2)
        println(rdd.partitioner)
        val value: RDD[(Int, Int)] = sc.makeRDD(List((1, 2), (2, 3)),2)
        println(value.partitioner)

        val search = new Search("h")

        search.getMatch1(rdd).collect().foreach(println)
//        search.getMatch2(rdd).collect().foreach(println)

        sc.stop()
    }
    // 查询对象
    // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
    case class Search(query:String){

        def isMatch(s: String): Boolean = {
            s.contains(this.query)
        }

        // 函数序列化案例
        def getMatch1 (rdd: RDD[String]): RDD[String] = {
//            rdd.filter(isMatch)
            rdd.filter(x => this.isMatch(x))
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            val s = query
            rdd.filter(x => x.contains(s))
        }
    }
}
