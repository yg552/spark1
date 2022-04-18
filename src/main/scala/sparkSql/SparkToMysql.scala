package sparkSql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * author 杨广
 * spark+mysql
 * 使用session.read.jdbc()
 *
 * username=root
 * password=123456
 * url=jdbc:mysql://localhost:3306/hah?serverTimezone=UTC
 * driver=com.mysql.cj.jdbc.Driver
 */
object SparkToMysql {

  case class people(rid: Short, name: String, zw: String)

  def apply(): SparkToMysql = new SparkToMysql()

  def writeMysql(session: SparkSession, url: String, table: String, prop: Properties): Unit = {
    import session.implicits._
    //通过构建dataframe写入mysql数据库
    //使用toDF方法(不要忘记  import session.implicits._)
    //1.构造需要写入的数据，比如，7，Tom,a;8,Mark,b
    val values: RDD[(Short, String, String)] = session.sparkContext.makeRDD(Array((7: Short, "Tom", "a"), (8: Short, "Mark", "b")))
    val frame: DataFrame = values.map((x: (Short, String, String)) => people(x._1, x._2, x._3)).toDF()
    //2.开始写入数据库，可以选择模式，默认是覆盖，现在使用append模式
    frame.write.mode("append").jdbc(url, table, prop)
    val value: Dataset[Row] = frame.as("aa")
    println("it's ok!")
  }

  def main(args: Array[String]): Unit = {
    //查看mysql hah数据库的role表

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    prop.put("driver", "com.mysql.cj.jdbc.Driver")
    val url: String = "jdbc:mysql://localhost:3306/hah?serverTimezone=UTC"
    val session: SparkSession = SparkSession.builder().master("local").appName("SparkToMysql").getOrCreate()
    val mysql: SparkToMysql = SparkToMysql()

    //SparkSession对mysql的读取
    //mysql.readMysql(session, url, "role", prop)

    //SparkSession对mysql的写入
    writeMysql(session, url, "role", prop)
    mysql.close(session)
  }
}

class SparkToMysql {

  /*
  读取mysql的表文件
   */
  def readMysql(session: SparkSession, url: String, table: String, prop: Properties): Unit = {
    //4.连接mysql并读取数据
    val frame: DataFrame = session.read.jdbc(url, table, prop)
    frame.show()
  }

  /*
  关闭SparkSession对象
   */
  def close(session: SparkSession): Unit = {
    session.stop()
  }
}