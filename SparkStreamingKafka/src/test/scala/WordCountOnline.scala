import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCountOnline {
  def main(args: Array[String]): Unit = {

    /**
      * 创建SparkConf
      */
    val conf =  new SparkConf().setMaster("spark://172.16.18.234:7077").setAppName("wordcountonline")
      .set("spark.local.dir", "/lyt/tmp")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    val ssc  = new StreamingContext(conf,Seconds(1))
    /**
      * 数据来源 ，可以来自kafka
      */
    val lines = ssc.socketTextStream("Master", 9999, StorageLevel.MEMORY_AND_DISK)
    /**
      * 按空格分割
      */
    val words = lines.flatMap { line => line.split(" ") }
    /**
      * 把单个的word变成tuple
      */
    val wordCount  = words.map { word => (word,1) }
    /**
      * (key1,1) (key1,1)
      * key相同的累加。
      */
    wordCount.reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
    ssc.stop(true)

  }
}