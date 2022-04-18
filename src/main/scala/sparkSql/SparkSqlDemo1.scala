package sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
 * author 杨广
 * sparkSql
 * 1.自定义udf函数 一进一出类型
 * 2.自定义udaf聚合函数 多进一出类型  （又分为强  弱 类型）
 */
object SparkSqlDemo1 {

  case class user(name: String, age: Long)

  case class buff(var sum: Long, var count: Long)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("SparkSqlDemo1")
    //创建SparkSession对象
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //session隐式转换，即把rdd隐式转化为dataframe
    import session.implicits._
    //读取people.json
    val df: DataFrame = session.read.json("input/people.json")
    df.createOrReplaceTempView("user")



    //    session.udf.register("add2", (x: Int) => x + 2)
    //    session.sql("select add2(age) from user").show()


    //    //使用自定义弱聚合函数求出平均值
    //    session.udf.register("myAvg", new Rudf)
    //    session.sql("select myAvg(age) as avg from user").show()

    //使用自定义强聚合函数求出平均值(较新的版本写法)
    //    session.udf.register("avg2",functions.udaf(new QUDAF))
    //老版本的写法
    val ds: Dataset[user] = df.as[user]
    val column: TypedColumn[user, Double] = new QUDAF().toColumn.name("qAvg")
    ds.select(column).show()

    session.stop()

  }

  //强聚合函数
  class QUDAF extends Aggregator[user, buff, Double] {

    //初始化缓冲区的数据
    override def zero: buff = {
      buff(0, 0)
    }

    //更新缓冲区的数据
    override def reduce(b: buff, a: user): buff = {
      b.sum += a.age
      b.count += 1
      b

    }

    //合并缓冲区
    override def merge(b1: buff, b2: buff): buff = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    //得出最后的的值
    override def finish(buff: buff): Double = {
      (buff.sum / buff.count).toDouble
    }

    //固定编码格式
    override def bufferEncoder: Encoder[buff] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }


  //自定义聚合函数
  //弱类型(求平均)
  class Rudf extends UserDefinedAggregateFunction {
    //输入参数的结构
    override def inputSchema: StructType = {
      StructType(List(StructField("age", IntegerType)))
    }

    //缓冲区的数据类型
    override def bufferSchema: StructType = {
      StructType(List(StructField("sum", IntegerType), StructField("count", IntegerType)))
    }

    //最终输出的数据类型
    override def dataType: DataType = DoubleType

    //函数是否稳定
    override def deterministic: Boolean = true

    //初始化缓冲区
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
      buffer(1) = 0
    }

    //更新buffer
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getInt(0) + input.getInt(0)
      buffer(1) = buffer.getInt(1) + 1
    }

    //合并分区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
      buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }

    //最终的计算
    override def evaluate(buffer: Row): Any = {
      (buffer.getInt(0) / buffer.getInt(1)).toDouble
    }
  }

}

