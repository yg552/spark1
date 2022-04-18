package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * author 杨广
 * 监听9999端口 统计词频
 */
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkStreaming对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SocketWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    val dData: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val value: DStream[(String, Int)] = dData.flatMap(x => x.split(" ")).map((_, 1))

    val value1: DStream[(String, (Int, Option[Int]))] = value.leftOuterJoin(value)
    value1.mapValues(x=>x._2.getOrElse(0))

    val wordCount=value.reduceByKey(_ + _)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
