package com.lh.core.rdd.operator.transform

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object Spark23_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)

        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("a", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 1), ("c", 2),("a", 3)
        ))

        // cogroup : connect + group (分组，连接)
//        val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
//
//        cgRDD.collect().foreach(println)
//        rdd1.join(rdd2).collect().foreach(println)
/*
    (a,(1,1))
    (a,(3,1))
 */

        val bc: Broadcast[Array[(String, Int)]] = sc.broadcast(rdd2.collect())
        val rdd2_val = bc.value
        val value: RDD[(String,(Int, Int))] = rdd1.flatMap{ case (x, y) => {
            var arr:Array[(String,(Int, Int))] = Array();
            rdd2_val.foreach{ case (x1, y1) => {
                if (x1.equals(x)) {
                    arr = Array.concat(arr, Array((x, (y, y1))))
                }}
            }
            arr;
        }}
        value.collect().foreach(println)


        sc.stop()

    }
}
