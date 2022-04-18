package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * author 杨广
 * 2022/2/17 0017
 */
object TestJson {
  def main(args:Array[String]):Unit={
    var conf=new SparkConf()
    var spark=SparkSession.builder()
      .master("local[*]")
      .appName("jsonTest")
      .config(conf)
      .getOrCreate()

    var dataFrame=spark.read.json("E:\\Idea\\geospark_demo\\MyTest")
    dataFrame.createOrReplaceTempView("data")
    spark.sql("select properties.UserData from data").show()

    spark.stop()
  }

}
