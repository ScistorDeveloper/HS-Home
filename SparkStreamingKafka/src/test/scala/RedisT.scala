import Util.UrlCheck
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random
import collection.JavaConversions._
import collection.mutable._

object RedisT extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[1]")
    val spark = new SparkContext(conf)
    println("reids test " + UrlCheck.getCheck.checkUrl("www.baidu.com"))
    val m :java.util.Map[String, String] = HashMap("STATUS"->"1","COUNT"->"1")
    println("reids inset test " + UrlCheck.getCheck.put("www.baidu.com",m))
    println("redis get test " + UrlCheck.getCheck.get("www.baidu.com", "COUNT"))
    val status = UrlCheck.getCheck.get("www.baidu.com","STATUS")
    val count : java.util.List[String] = UrlCheck.getCheck.get("www.baidu.com","COUNT")
    println("count :" + count.get(0))
//    val c = count.get(0)
//    count = toInt(c)
    val curr = 32423
    val newcount = curr + count.get(0)
    println(newcount)
    val m1 :java.util.Map[String, String] = HashMap("STATUS"->"1","COUNT"->newcount)
    println("fresh count " + UrlCheck.getCheck.put("www.baidu.com",m1))
    spark.stop()
  }
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }
}
