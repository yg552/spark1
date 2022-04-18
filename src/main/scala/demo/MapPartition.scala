package demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author 杨广
 *         MapPartition
 *         分区，高效但存在oom  内存溢出风险
 */
object MapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3),2)
    val list1RDD: RDD[Int] = listRDD.mapPartitions(x=> x)
    val resRDD: Array[Int] = list1RDD.collect()


  }
}
