package sparkstreaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * author 杨广
 */
object Demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiver")
    val ssc = new StreamingContext(conf, Seconds(2))
    val message: ReceiverInputDStream[String] = ssc.receiverStream[String](new MyReceiver)
    message.print()
    ssc.start()
    ssc.awaitTermination()


  }

}
