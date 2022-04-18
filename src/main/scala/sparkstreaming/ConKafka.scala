package sparkstreaming

import java.util
import java.util.Collections

import com.google.common.eventbus.Subscribe
import com.mysql.cj.x.protobuf.MysqlxCrud.Collection
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable


/**
 * author 杨广
 * sparkStreaming +Kafka（消费数据）
 */
object ConKafka {
  def main(args: Array[String]): Unit = {
    //创建kafka消费者的配置信息
    val kafka: mutable.Map[String, String] = mutable.Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.167:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.GROUP_ID_CONFIG, "demo1")
    )
    //创建ssc对象
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("ConKafka")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //使用kafkaUtils接收kafka生产者生产的数据
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      //通常使用,executor之间均匀地分布分区(consumer会在executor上缓冲，而不是为每批数据重新创建)
      PreferConsistent,
      //当executor和kafka的leader在同一节点时可以使用
      //PreferBrokers
      //分区之间的负载有明显的偏差，请使用PreferFixed
      //PreferFixed()
      ConsumerStrategies.Subscribe[String, String](List("test"), kafka)
    )

    kafkaData.foreachRDD{
          //driver
      rdd=>
        val offset: Array[OffsetRange] =rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //executor
        rdd.foreachPartition{
          _: Iterator[ConsumerRecord[String, String]] =>
          val a = offset(TaskContext.getPartitionId())
            println(s"${a.topic},${a.partition},${a.fromOffset},${a.untilOffset}")
      }
    }



    //    kafkaData.foreachRDD(
    //      /*
    //      ，只有在对createDirectStream的结果调用的第一个方法中，而不是在随后的方法链中，对HasOffsetRanges的类型转换才会成功。
    //       获取kafka consumer的偏移量
    //       */
    //      rdd=>{
    //        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //        rdd.foreachPartition(
    //          iter=>{
    //            val range: OffsetRange = ranges(TaskContext.get().partitionId())
    //          }
    //        )
    //      }
    //    )

    ssc.start()
    ssc.awaitTermination()
  }

}
