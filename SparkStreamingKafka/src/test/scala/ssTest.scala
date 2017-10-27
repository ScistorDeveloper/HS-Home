import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


@SerialVersionUID(6732270565076291202L)//使用注解来制定序列化id
object ssTest extends Serializable {
  def main(args: Array[String]) {
    // Create a StreamingContext with a local master
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster("spark://172.16.18.234:7077")
      .set("spark.local.dir", "/lyt/tmp")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create a DStream that will connect to serverIP:serverPort, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print a few of the counts to the console
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}