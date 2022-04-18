package sparkSql
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


/**
 * author 杨广
 * 2021/12/17 0017
 */
object ReadOrc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.sql.orc.impl","native")
    conf.set("spark.debug.maxToStringFields", "200")
    val session: SparkSession = SparkSession
      .builder().config(conf).appName("readOrc").master("local[*]").getOrCreate()
    val data: DataFrame = session.read.orc("E:\\玖舆博泓文件\\KafkaDmoeData\\5GSA-104-N3HTTP-HW-0_1639729347130.orc")
    data.printSchema()
    println(data.count())
    session.stop()
  }

}
