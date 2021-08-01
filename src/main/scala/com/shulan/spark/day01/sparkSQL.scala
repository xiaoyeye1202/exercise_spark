package com.shulan.spark.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * @author zh
 * @create 2021-04-07
 */
object sparkSQL {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
		val df: DataFrame = spark.read.json("E:\\第四阶段资料\\spark课件\\SparkSQL\\sql\\people.json")
//		df.schema.printTreeString()
//		df.show(5,false)
//		import spark.implicits._
//		df.select($"name",($"age"+10).as("new age")).show()
		df.createTempView("people")
		val sql =
			"""
			  |select
			  |province,
			  |count(*) as counts
			  |from people
			  |group by province
			  |""".stripMargin
		spark.sql(sql).show()
		spark.stop()
	}
}
