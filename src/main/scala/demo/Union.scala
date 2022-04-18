package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 */
object Union {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("Union")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Array[Int]] = sc.makeRDD(List(Array(1, 2, 3), Array(4, 5, 6)))
    val rdd2: RDD[Array[Int]] = sc.makeRDD(List(Array(1, 2), Array(4, 5), Array(7, 8)))
    val rdd3: RDD[Array[Int]] = rdd1.union(rdd2)
    val rdd4: RDD[Array[Int]] = rdd1.union(rdd1)
    val array: Array[Array[Int]] = rdd3.collect()
    array.foreach(x=>print(x.mkString(",")+" "))
    println()
    rdd3.foreachPartition(
      rdd => {
        val a: String = "o";
        rdd.foreach{
          r=>println(r.mkString("Array(", ", ", ")")+a)
        }
      }
    )
//    rdd4.collect().foreach(x=>print(x.mkString(" ")+" "))

    sc.stop()
  }

}
