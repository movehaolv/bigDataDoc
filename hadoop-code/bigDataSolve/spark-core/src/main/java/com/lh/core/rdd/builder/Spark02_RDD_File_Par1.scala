package com.lh.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par1 {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // TODO 数据分区的分配 紧接Spark02_RDD_File_Par
        // 1. 数据以行为单位进行读取
        //    spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
        // 2. 数据读取时以偏移量为单位,偏移量不会被重复读取
        /* 数据   => 偏移量     （其中@@为特殊字符，如windows下的换行符 crlf）
           1@@   => 012
           2@@   => 345
           3     => 6

         */
        // 3. 数据分区的偏移量范围的计算（结合Spark02_RDD_File_Par，数据[1@@,2@@,3]会被hadoop的
        //FileInputFormat.getSplits切分成3个分区
        // 0 => [0, 3]  => 12  第一个分区起始位置0，偏移量+3，则为[0,3]，注意为前闭后闭区间 , 所以读 1@@2，但是数据又以行为读取，则第一个分区读前2行
        // 1 => [3, 6]  => 3   第二个分区起始位置0+3, 偏移量+3，则为[3,6]，因为第二行已经读过，不会重复读取，故第二个分区读偏移量为6的值，所以读到3
        // 2 => [6, 7]  =>     第三个分区0+2*3, 共6个偏移量则[6,7]，这里不必纠结7取不取得到的问题了，反正都能最终读取完，因为前2个分区读取完，故该分区文件为空

        // 【1,2】，【3】，【】
        val rdd = sc.textFile("datas/1.txt", 2)

        rdd.saveAsTextFile("output")


        // TODO 关闭环境
        sc.stop()
    }
}
