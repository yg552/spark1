import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * author 杨广
 */
object WordCont2 {
  def main(args:Array[String]): Unit ={
    //创建sparkcontext
    val conf:SparkConf=new SparkConf()
    conf.setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val inputRDD: RDD[String] = sc.textFile("input2")
    val resultRDD: RDD[(String, Int)] = inputRDD.flatMap((_: String).split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2,false)
      .repartition(1)
    resultRDD.saveAsTextFile("output2")

    sc.stop()
  }

}
