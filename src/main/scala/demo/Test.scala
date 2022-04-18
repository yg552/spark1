package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 * 2022/4/14 0014
 */
object Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val str: String = sc.getConf.get("spark.network.timeout")
    println("--------------------------")
    println(str)
    println("--------------------------")




  }
}

