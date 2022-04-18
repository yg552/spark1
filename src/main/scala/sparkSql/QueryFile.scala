package sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * author 杨广
 * 直接读取file文件
 */
object QueryFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("QueryFile")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //隐式转换
    import spark.implicits._
    //读取文件

    spark.sql("select * from csv. `./input/a.csv`").show()

    val data: DataFrame = spark.read.text("./input/file1.txt")
//    saveAsTable 在spark内嵌的hive中创建表,option("path","/")
    /*
    df.write.option("path", "/some/path").saveAsTable("t"). When the table is dropped,
    the custom table path will not be removed and the table data is still there.
    If no custom table path is specified, Spark will write data to a default
    table path under the warehouse directory.
    When the table is dropped, the default table path will be removed too.
     */
    data.write.saveAsTable("file1")
    spark.sql("show tables").show()
    spark.sql("drop table file1")
    spark.sql("show tables").show()


    spark.stop()

  }

}
