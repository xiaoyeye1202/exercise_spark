package com.shulan.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zh
 * @create 2021-03-18
 */
object sparkTopN {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTopN")
		val sc = new SparkContext(conf)
		val dataRDD: RDD[String] = sc.textFile("hdfs://qianfeng01:8020/input")


		//2.对读取到的数据，进行结构转换  (省份id-广告id,1)
		val mapRDD: RDD[(String, Int)] = dataRDD.map {
			line => {
				//2.1  用空格对读取的一行字符串进行切分
				val fields: Array[String] = line.split(" ")
				//2.2 封装为元组结构返回
				(fields(1) + "-" + fields(4), 1)
			}
		}

		//3.对当前省份的每一个广告点击次数进行聚合  (省份A-广告A,1000)  (省份A-广告B,800)
		val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey( _ + _ )

		//4.再次对结构进行转换，将省份作为key   (省份A,(广告A,1000))  (省份A,(广告B,800))
		val map1RDD: RDD[(String, (String, Int))] = reduceRDD.map {
			case (proAndAd, clickCount) => {
				val proAndAdArr: Array[String] = proAndAd.split("-")
				(proAndAdArr(0), (proAndAdArr(1), clickCount))
			}
		}

		//5.按照省份对数据进行分组    (省份, Iterable[(广告A, 80),(广告B, 100),(广告C, 90),(广告D, 200)....])
		val groupRDD: RDD[(String, Iterable[(String, Int)])] = map1RDD.groupByKey()

		//6.对每一个省份中的广告点击次数进行降序排序并取前3名
		val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
			itr => {
				//itr.toList.sortBy(_._2).reverse.take(3)
				itr.toList.sortWith {
					(left, right) => {
						left._2 > right._2
					}
				}.take(3)
			}
		)
		resRDD.collect().foreach(println)

		// 关闭连接
		sc.stop()

	}
}
