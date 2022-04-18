package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 杨广
 *spark wordCount
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)
    //    读取文件
    val lines: RDD[String] = sc.textFile("input")
    //扁平化
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //构架（hadoop，1）的形式
    val word: RDD[(String, Int)] = words.map((_, 1))
    //统计
    val result: RDD[(String, Int)] = word.reduceByKey(_ + _)
    //收集结果
    val res_tuples: Array[(String, Int)] = result.collect()
    println(res_tuples.mkString(","))
    sc.stop()

  }

}
