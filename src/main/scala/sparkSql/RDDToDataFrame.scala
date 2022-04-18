package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ShortType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * author 杨广
 * RDD转为DataFrame
 * 方式一：使用toDF,(case class)
 * 方式二：使用createDataFrame   (StructType+Row)
 */
object RDDToDataFrame {

  //样例类
  case class people(name: String, age: Short)

  //结构类型
  private val structType: StructType = StructType(
    Seq(
      StructField("name", StringType, true),
      StructField("age", ShortType, true)
    )
  )

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//    conf.set("spark.sql.orc.impl", "native")
//    conf.set("spark.debug.maxToStringFields", "200")
    //创建sparkSession对象
    val session: SparkSession = SparkSession.builder().master("local[*]").appName("test").config(conf).getOrCreate()
    import session.implicits._
    //    val value: RDD[String] = session.sparkContext.textFile("input/people.txt", 1)
    //    //方式一
    //    //需要使用样列类，设定dataframe的形状
    //    val df1: DataFrame = value.map(x => x.split(",")).map(x => people(x(0), x(1).trim.toShort)).toDF
    //
    //    val value1: Dataset[people] = df1.as[people]
    //    value1.show()
    //
    //
    //    //方式二
    //    //需要定义一个结构类型（StructType），设定dataframe的形状
    //    val value2: RDD[Row] = value.map(x => x.split(",")).map(x => Row(x(0), x(1).trim.toShort))
    //    val df2: DataFrame = session.createDataFrame(value2, structType)
    //    df2.show()
    val frame: DataFrame = session.read.orc("E:\\玖舆博泓文件\\KafkaDmoeData\\CQ_BB_MOBILE_CNOS_HUAWEI_CXDR_RNC017_0010_20211112173931_S1UDNS-443_0_0\\targetFileName.orc")
    frame.printSchema()

    session.stop()


  }

}
