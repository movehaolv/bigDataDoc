package com.lh.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // textFile(path: String, minPartitions: Int = defaultMinPartitions)可以将文件作为数据处理的数据源，默认也可以设定分区。
        //     minPartitions : 最小分区数量
        //     math.min(defaultParallelism, 2)  // defaultParallelism 使用了local则为本机的cpu数量，这里的2是源码中的值
        // 为什么切片为2，但是最后确生成了3个分区文件呢？
        //val rdd = sc.textFile("datas/1.txt"， 2) // 第二个参数不写默认为2。
        /**
         * 1.txt的数据内容
         * 1CRLF
         * 2CRLF
         * 3
         */
        //
        // Spark读取文件，底层其实使用的就是Hadoop的读取方式 org\apache\hadoop\mapred\FileInputFormat.java/ getSplits
        // 分区数量的计算方式：
        //    totalSize = 7
        //    goalSize =  7 / 2 = 3（byte）  // 每个分区有3个字节

        //    7 / 3 = 2...1 (余下的1个字节大于分区的0.1倍[即剩余部分4个字节大于1.1倍]，故要加一个分区变成3个分区) + 1 = 3(分区)

        //

        val rdd = sc.textFile("D:\\workLv\\learn\\proj\\hadoop-code\\bigDataSolve\\spark-core\\src\\main\\java\\com" +
          "\\lh\\datas\\1.txt")

        rdd.saveAsTextFile("output21")

        // TODO 关闭环境
        sc.stop()
    }
}
