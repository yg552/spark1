package sparkstreaming


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

/**
 * author 杨广
 */
object Window {

  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
    conf.setMaster("local[*]").setAppName("window")
    val sc=new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.remember(Minutes(5))
    ssc.checkpoint("./checkpoint")
    val dataSource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888,StorageLevel.MEMORY_ONLY)
//    dataSource.foreachRDD(
//      //是不能追加的
//      rdd=>rdd.saveAsTextFile("window/window.txt")
//    )
    println("start")

    dataSource.window(Seconds(30)).saveAsTextFiles("window","txt")

    ssc.start()
    ssc.awaitTermination()

  }

}
