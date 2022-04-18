package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 杨广
 *         map算子
 */
object map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3))
    val list1RDD: RDD[Int] = listRDD.map(_ * 2)
    val resRDD: Array[Int] = list1RDD.collect()
    println(resRDD.mkString(","))

  }

}
