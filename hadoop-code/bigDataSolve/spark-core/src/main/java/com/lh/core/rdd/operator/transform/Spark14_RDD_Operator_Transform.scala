package com.lh.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        val mapRDD:RDD[(Int, Int)] = rdd.map((_,1))
        println(mapRDD.partitioner)  // None
        // RDD => PairRDDFunctions
        // 隐式转换（二次编译）
        mapRDD.saveAsTextFile("output1")   // part-00000 (1,1) (2,1)  part-00001 (3,1) (4,1)

        // partitionBy根据指定的分区规则对数据进行重分区
        val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
        newRDD.partitionBy(new HashPartitioner(2))
        println(newRDD.partitioner)  // Some(org.apache.spark.HashPartitioner@2)

        newRDD.saveAsTextFile("output2")  // // part-00000 (2,1) (4,1)  part-00001 (1,1) (3,1)

        sc.stop()

    }
}
