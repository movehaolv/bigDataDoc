package com.lh.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("c", 3),
            ("b", 4), ("c", 5), ("c", 6)
        ),2)


        // aggregateByKey存在函数柯里化，有两个参数列表
        // 第一个参数列表,需要传递一个参数，表示为初始值
        //       主要用于当碰见第一个key的时候，和value进行分区内计算
        // 第二个参数列表需要传递2个参数
        //      第一个参数表示分区内计算规则
        //      第二个参数表示分区间计算规则

        // math.min(x, y)
        // math.max(x, y)
        rdd.aggregateByKey(10)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        ).collect.foreach(println) //  (b,10) (a,10)  (c,20)


        rdd.aggregateByKey(5)(
            (x, y) => x + y,
            (x, y) => x + y
        ).collect.foreach(println)

        rdd.aggregateByKey(0)(_+_, _+_).collect.foreach(println)





        sc.stop()

    }
}
