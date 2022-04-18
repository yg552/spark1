package StruStreaming
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{count, expr, sum, window}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 * author 杨广
 * spark 结构化流 基于sparkSql引擎，无界表
 */
object Pra_1 {

  case class people (name: String, age: String)
  def main(args: Array[String]): Unit = {
    System.setProperty("file.encoding", "UTF-8")


    //创建sparkSql环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("pra_1")
      .getOrCreate()
    import spark.implicits._


    val inputTable: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 8888)
      .load()

    inputTable.as[people].groupByKey{
      x=>
        val a: Int =x.age match{
          case "yg" =>1
          case "lh"=>2
          case _ =>0
        }
        a
    }

    val data: Dataset[people] = inputTable.as[String].map(
      x => {
        var y = new String(x.getBytes)
        println(y)
        var z = y.split(" ")
        people(z(0), z(1))
      }
    )


    val outputTable: StreamingQuery = data.writeStream
      .format("console")
      .outputMode("append")

      .start()


    outputTable.awaitTermination()
  }

}
