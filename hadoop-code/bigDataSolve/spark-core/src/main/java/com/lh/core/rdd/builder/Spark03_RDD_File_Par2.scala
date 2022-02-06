package com.lh.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD

        // 14byte / 2 = 7byte
        // 14 / 7 = 2(分区)

        /*
        1234567@@  => 012345678
        89@@       => 9101112
        0          => 13

        [0, 7]   => 1234567
        [7, 14]  => 890

         */

        // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区
        /*
        * 4个文件长度分别为100 100 100 1400字节,默认最小分区为2
            首先计算全部文件总长度totalSize=100+100+100+1400=1700B
            goalSize=totalSize/最小分区数即2 =850B
            blockSize=128M换算成字节为134217728B
            minSize=1B
            goalSize与blockSize取最小 值为850
            850 与minSize取最大 值为850
            即splitSize为850
            然后 每个文件长度除以850 判断是否大于1.1
            文件1,2,3都是100所以各生成1个分区,
            文件4位1400,除以850>1.1 切分一个分区,剩余
            (1400-850)/850 >1.1不再成立 又生成一个分区.
            所以举例中的四个文件 共生成5个分区
        * */

        val rdd = sc.textFile("output2", 2)

        rdd.saveAsTextFile("output")


        // TODO 关闭环境
        sc.stop()
    }
}
