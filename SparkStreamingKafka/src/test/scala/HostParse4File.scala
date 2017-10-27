import Util.UrlCheck
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import collection.JavaConversions._
import collection.mutable._

object HostParse4File  extends Serializable {
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster("local[1]")
    val ssc = new SparkContext(sparkConf)

    val data = ssc.textFile("e:\\tmp\\upload.txt")

    //提取Host操作
    val lineSplit = data.map(rdd=>rdd.toString.split("\\|\\|"))
    val hosts = lineSplit.map(array=>(array(0),1))
    val wordCount = hosts.reduceByKey(_+_)
    wordCount.foreach(map => println(map._1 +  " " + map._2))
    //查询Redis中是否存在当前host
    wordCount.foreach(rdd=>{
        val h = rdd._1
        val result:java.util.List[String] = UrlCheck.getCheck.checkUrl(h)
        if (result == null) {
          val m: java.util.Map[String, String] = HashMap("COUNT" -> "0", "STATUS" -> "0")
          UrlCheck.getCheck.put(h,m)
        }else{
          val status = UrlCheck.getCheck.get(h,"STATUS")
          var count = UrlCheck.getCheck.get(h,"COUNT").get(0).toInt
          val newcount = rdd._2
          count = newcount + count
          val m1: java.util.Map[String, String] = HashMap("COUNT" -> count.toString(), "STATUS" -> status.get(0))
          UrlCheck.getCheck.put(h,m1)
        }
    })
    println("check for keys in RDD")
    val lists : java.util.TreeSet[String] = UrlCheck.getCheck.getAll("*")
    lists.foreach(f=>{
      if(f.length()>1) {
        println(f + UrlCheck.getCheck.get(f, "COUNT") + " " + UrlCheck.getCheck.get(f, "STATUS"))
      }
    })

    ssc
  }


  def main(args: Array[String]) {
    log.setLevel(Level.WARN)
    val ssc = functionToCreateContext()
    // Start the computation
//    ssc
//    ssc.awaitTermination()
  }
}
