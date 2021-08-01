package com.shulan.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zh
 * @create 2021-03-15
 */
object createRDD {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("createRDD").setMaster("local[*]")
//		创建sparkContext上下文环境对象
		val sc = new SparkContext(conf)
//      读取外部文件创建RDD
		val data: RDD[String] = sc.textFile(args(0))
//		根据集合创建RDD
		val data1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)
//      查看分区
		val size: Int = data.partitions.size

		data1.collect().foreach(println)
//		释放资源
		sc.stop()
	}
}
