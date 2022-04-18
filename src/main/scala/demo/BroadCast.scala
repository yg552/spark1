package demo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 * 广播变量的使用 broadcast
 * 在executor中只能读不能写，即不能在除Driver的位置修改
 */
object BroadCast {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadCast")
    val sc = new SparkContext(conf)
    //创建广播变量
    val bc: Broadcast[List[Int]] = sc.broadcast(List(1, 2, 3, 4))
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    listRDD.map(
      elem => {
        println(bc.value) //打印分区的bc变量值
      }
    ).collect()
    sc.stop()
  }
}
