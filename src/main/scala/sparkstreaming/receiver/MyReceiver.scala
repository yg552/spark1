package sparkstreaming.receiver

import java.util.Random

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


/**
 * author 杨广
 * 自定义采集器
 */
class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  private var flag: Boolean = true

  //采集器开始数据采集
  override def onStart(): Unit = {
    //开启线程采集需要的数据
    new Thread(new Runnable {
      override def run(): Unit = {
        //数据采集核心代码
        //模拟数据的生产并采集
        while (flag) {
          var data = new Random().nextInt(10)
          var res:String="已采集到数据:aa--" + data
          store(res)
          Thread.sleep(500)
        }
      }
    }).start()

  }

  //停止采集器
  override def onStop(): Unit = {
    flag = false

  }
}
