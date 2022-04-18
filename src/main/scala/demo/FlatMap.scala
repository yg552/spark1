package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 杨广
 *  flatmap
 */
object FlatMap {
  def main(args: Array[String]): Unit = {

    //构建sc对象
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("flatmap")
    val sc: SparkContext = new SparkContext(conf)
    val ListRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(4, 5)))
    //flatmap扁平化
    val FlatMapRDD: RDD[Int] = ListRDD.flatMap[Int]((data: List[Int]) => data).map(x=>x*2)

    FlatMapRDD.collect().foreach(println)

  }
}
