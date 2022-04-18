package sparkSql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}



/**
 * author 杨广
 * sparkSql知识点复习
 */
object SqlDemo1 {
  def main(args:Array[String]): Unit ={
    var conf=new SparkConf()
    conf.setMaster("local[*]").setAppName("sparkSqlDemo")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //加入隐式转换
    import spark._

    //读取mysql的demo表
    var prop:Properties=new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val table: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/hah?serverTimezone=UTC", "demo", prop)
    table.createOrReplaceTempView("demo")
    val str: String = table.columns.mkString(",")
    spark.sql(s"select concat_ws('_',$str) as allColumn from demo").show()
    spark.stop()
  }

}
