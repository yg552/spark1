package demo

import demo.CoGroup.conf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 * 测试map(x=>x)
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]").setAppName("mapDemo")
    val sc = new SparkContext(conf)
    val value: RDD[Array[Int]] = sc.makeRDD(List(Array(1, 2, 3, 4, 5, 6)))
    val value1: RDD[Array[Int]] = value.map(x => x)
    value.foreach {
      x => {
        println(x.mkString("Array(", ",", ")"))
      }
    }
    value1.foreach {
      x => {
        println(x.mkString("Array(", ",", ")"))
      }
    }
    sc.stop()

  }

}
