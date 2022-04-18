package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 * rdd写入文件
 * 分区问题
 */
object WriteToFile {
  def main(args: Array[String]): Unit = {
    //创建sc对象
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("WriteToFile")
    val sc = new SparkContext(conf)
    val accumulator = new LongAccumulator
    sc.setCheckpointDir(" demo")
    //读取input/file1.txt文件内容
    val fileValues: RDD[String] = sc.textFile("input/file1.txt",1)
    fileValues.cache()
    fileValues.checkpoint()
    fileValues.foreach(println)
    //把fileValues的内容写入到output/file3.txt
    fileValues.saveAsTextFile("output/file1_out")
    sc.stop()

  }
}
