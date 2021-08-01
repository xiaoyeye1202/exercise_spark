package com.shulan.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zh
 * @create 2021-03-15
 */
object WordCount {
	def main(args: Array[String]): Unit = {
//		创建sparkConf配置对象
		val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
//		创建sparkContext上下文环境对象
		val sc = new SparkContext(conf)
//		读取外部文件
		val dataRDD: RDD[String] = sc.textFile(args(0))
//      扁平化处理
		val flatMapRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
//      map处理
		val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
//      reduceByKey处理
		val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
//		数据落地
		reduceByKeyRDD.saveAsTextFile(args(1))
//		释放资源
		sc.stop()
		println("demo")
//		一行代码解决
//		sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
	}
}
