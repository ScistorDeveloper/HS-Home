
import Util.{Func, UrlCheck}

import scala.collection.mutable.Map
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import kafka.serializer.StringDecoder

import collection.JavaConversions._
import collection.mutable._
/**
  * @author litaoxiao
  *
  */
object SSScala extends Serializable {
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster("spark://HS:7077")
      .set("spark.local.dir", "/lyt/tmp")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create direct kafka stream with brokers and topics
    //定义test为kafka中数据流
    val topicsSet = "HStest".split(",").toSet
    val brokerlist = "172.16.18.228:9092,172.16.18.229:9092,172.16.18.234:9092"
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> brokerlist, "auto.offset.reset" -> "smallest",
    "group.id" -> "testGroup","fetch.message.max.bytes"->"20971520")
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")

    kafkaDirectStream.cache

    //提取Host操作
    val lineSplit = kafkaDirectStream.map(rdd=>rdd._2.split("\\|\\|"))
    val hosts = lineSplit.map(array=>(array(0),1))
    val wordCount = hosts.reduceByKey(_+_)
    wordCount.print()

    //查询Redis中是否存在当前host
    wordCount.foreachRDD(rdd=>{
      rdd.foreach(host=> {
        val h = host._1
        val result: java.util.List[String] = UrlCheck.getCheck.checkUrl(h)
        if (result != null) {
          if (result.get(0) == null) {
            val m: java.util.Map[String, String] = HashMap("COUNT" -> "1", "STATUS" -> "0")
            UrlCheck.getCheck.put(h, m)
          } else {
            val status = UrlCheck.getCheck.get(h, "STATUS")
            var count = UrlCheck.getCheck.get(h, "COUNT").get(0).toInt
            val newcount = host._2
            count = newcount + count
            val m1: java.util.Map[String, String] = HashMap("COUNT" -> count.toString(), "STATUS" -> status.get(0))
            UrlCheck.getCheck.put(h, m1)
          }
        }else{
          val m: java.util.Map[String, String] = HashMap("COUNT" -> "1", "STATUS" -> "0")
          UrlCheck.getCheck.put(h, m)
        }
      })

    })
    kafkaDirectStream.saveAsTextFiles("hdfs://HS:9000//output//streaming","txt") //        valueDStream.dstream().repartition(1).saveAsTextFiles("hdfs://test", "txt");

    //更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }


  def main(args: Array[String]) {
    log.setLevel(Level.WARN)
    val ssc = functionToCreateContext()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}