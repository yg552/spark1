package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 * join:两个RDD对象中的元素的key要有相同的才返回，相当于相当于两个RDD的交集
 */
object Join {
  def main(args: Array[String]): Unit = {
    //获取sc对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ACC")
    val sc = new SparkContext(conf)
    val listRDD1 = sc.makeRDD(List(("a", 1), ("B", 2), ("c", 3)))
    val listRDD2 = sc.makeRDD(List(("d", 2), ("a", 3)))
    val joinRDD: RDD[(String, (Int, Int))] = listRDD1.join(listRDD2)
    val tuples: Array[(String, (Int, Int))] = joinRDD.collect()
    println(tuples.mkString(","))
    println(listRDD1.leftOuterJoin(listRDD2).collect().mkString(","))
    println(listRDD1.rightOuterJoin(listRDD2).collect().mkString(","))
    sc.stop()
  }
}
