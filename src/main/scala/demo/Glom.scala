package demo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


import scala.io.StdIn

/**
 * @author 杨广
 *         golm  将同一个分区里的元素合并到一个array里
 *
 */
object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("glom")
    val sc = new SparkContext(conf)


    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD: RDD[Int] = sc.makeRDD(list,2)

    val array: RDD[Array[Int]] = listRDD.glom()
    val d: Double = array.map(arr => arr.max).sum()
    println(d)
    sc.stop()

  }

}
