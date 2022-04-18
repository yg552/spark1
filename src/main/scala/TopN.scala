import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.breakable

/**
 * author 杨广
 */
object TopN {
  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
      .setAppName("TopN")

    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[(String, String, Int)] = sc.makeRDD(List(("H", "a", 1), ("H", "b", 2), ("B", "c", 3), ("B", "a", 1), ("B", "b", 2), ("H", "c", 3)))
    rdd1.map(x => (x._1, (x._2, x._3))).groupByKey().mapValues(x => {
      val list = new ArrayBuffer[(String, Int)]()
      val iterator: Iterator[(String, Int)] = x.iterator
      for (x <- iterator) {
        list.append(x)
      }
      val tuples: ArrayBuffer[(String, Int)] = list.sortBy(x => x._2).reverse.take(2)
      tuples
    }).foreach(println)
    sc.stop()
  }
}

