package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * author 杨广
 * 不能连接虚拟机上的spark暂时搁置
 */
object fConHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("spark://liunx:7077").setAppName("ConHive")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
    println(spark)
  }

}
