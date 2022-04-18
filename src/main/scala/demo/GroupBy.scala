package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 杨广
 *         groupBy
 */
object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("groupBy")
    val sc = new SparkContext(conf)
    val list: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val groupByRDD: RDD[(Boolean, Iterable[Int])] = list.groupBy(x => x > 3)
    val tuples: Array[(Boolean, Iterable[Int])] = groupByRDD.collect()
    for (elem <- tuples) {
      println(elem)
    }
  }
}
