package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * author 杨广
 * aggregateByKey()()
 * 第一个括号是初始值
 * 第二个括号的第一个参数为分区内的操作
 * 第二个括号的第二个参数为分区间的操作
 */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    val r1: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("a", 1), ("a", 3), ("b", 1), ("b", 6), ("b", 8)), 2)
    r1.aggregateByKey[Int](0)(
      //分区内的操作
      (x, y) => math.max(x, y),
      //分区间的操作
      (x, y) => x + y
    ).collect().foreach(println)

    //求平均值
    //初始值的(0,0)表示（字符后面的值，字符出现的次数）
println("*"*20)
    val agg: RDD[(String, (Int, Int))] = r1.aggregateByKey[(Int, Int)]((0, 0))(

      //第一个参数表示分区内的操作
      //t表示初始值时定义的tuple2，v表示r1中的值即("a", 2)中的2

      (t, v) => (t._1 + v, t._2 + 1),

      //第二个参数表示分区间的操作
      //不同分区的（字符后面的值，字符出现的次数），计算出总次数
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)


    )
    agg.mapValues(
      v => v._1 / v._2
    ).collect() foreach (println)


  }

}
