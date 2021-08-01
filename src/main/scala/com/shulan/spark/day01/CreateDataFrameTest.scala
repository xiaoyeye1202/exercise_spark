package com.shulan.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author zh
 * @create 2021-04-09
 */
object CreateDataFrameTest {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("creatDataTest").setMaster("local[*]")
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
		val dataRDD: RDD[String] = spark.sparkContext.textFile("E:\\第四阶段资料\\spark课件\\SparkSQL\\sql\\people.txt")
		val rdd: RDD[people] = dataRDD.map(x => {
			val arr: Array[String] = x.split(",")
			people(arr(0), arr(1).toInt)
		})
		import spark.implicits._
		val dataFrame: DataFrame = rdd.toDF()
		dataFrame.createTempView("people")
		val sql =
			"""
			  |select
			  |*
			  |from people
			  |""".stripMargin
		spark.sql(sql).show()
		
	}
}
case class people(str: String, i: Int)