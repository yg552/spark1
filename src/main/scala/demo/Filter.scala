package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 杨广
 *         Filter
 */
object Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("Filter")
    val sc = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD: RDD[Int] = sc.makeRDD(list)


    val filterRDD: RDD[Int] = listRDD.filter(x => x > 3)
    val resRDD: Array[Int] = filterRDD.collect()
    println(resRDD.mkString(","))
    sc.stop()

  }
}
