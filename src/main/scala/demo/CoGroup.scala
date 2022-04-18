package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 * cogroup   connect + group:返回两个RDD中元素的所有key的连接，相当于两个RDD的并集
 */
object CoGroup extends App {
  //获取sc对象
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ACC")
  val sc = new SparkContext(conf)
  val listRDD1 = sc.makeRDD(List(("a", 1), ("B", 2), ("c", 3),("a",5)))
  val listRDD2 = sc.makeRDD(List(("d", 2), ("a", 3)))
  val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = listRDD1.cogroup(listRDD2)
  private val value: RDD[(String, (Int, Int))] = listRDD1.join(listRDD2)
  value.foreach(println)
  //使用偏函数匹配，必须使用{}
  cgRDD.mapValues{
    case (x,y)=>{
      var v1=0
      var v2=0
      val xiter: Iterator[Int] =x.iterator
      if(xiter.hasNext){
        v1=xiter.next()
      }
      val yiter: Iterator[Int] =y.iterator
      if(yiter.hasNext){
        v2=yiter.next()
      }
      (v1,v2)
    }
  }.foreach(println)

  cgRDD.foreach(println)
  sc.stop()
}
