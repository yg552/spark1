package demo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

/**
 * author 杨广
 * spark 累加器（共享变量）
 * 累加器在executor中只能进行写操作
 * 创建自定义累加器对象并注册
 */
object Acc {
  def main(args: Array[String]): Unit = {
    //获取sc对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ACC")
    val sc = new SparkContext(conf)
    //求List(1,2,3,4,5)的和
    val listRDD: RDD[Double] = sc.makeRDD(List[Double](1, 2, 3, 4, 5))
    listRDD
    //1.如果不适用累加器
    //定义接收变量
    var sum: Double = 0
    listRDD.foreach(
      elem => {
        sum += elem
      }
    )
    println(sum) //输出结果为 0，原因：算子在executor计算后并不能传回Driver汇总，println(sum)的值并没有改变

    //2.使用累加器
    var acc = sc.doubleAccumulator
    listRDD.foreach(
      elem => {
        acc.add(elem)
      }
    )
    println("acc--sum=" + acc.value) //输出结果：sum=15.0

    //2.自定义累加器
    /**
     * 继承AccumulatorV2
     * 定义输入输出类型
     * 重新AccumulatorV2的方法
     */

    class MyAcc extends AccumulatorV2[Int, Int] {
      //判断是否为初值
      var sum: Int = 0

      override def isZero: Boolean = {
        sum == 0
      }

      override def copy(): AccumulatorV2[Int, Int] = {
        val acc2 = new MyAcc()
        acc2.sum = sum
        acc2
      }

      //重置
      override def reset(): Unit = sum = 0

      //添加元素
      override def add(v: Int): Unit = sum += v

      //整合所有分区的数据
      override def merge(other: AccumulatorV2[Int, Int]): Unit =
        sum +=other.value

      //返回累加器的值
      override def value: Int = sum
    }

    val listRDD1: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4, 5))
    val acc1 = new MyAcc
    sc.register(acc1, "MyAcc")
    listRDD1.foreach(
      elem => {
        acc1.add(elem)
      }
    )
    println("acc1--sum=" + acc1.value) //输出结果：sum=15.0

    sc.stop()
  }
}
