import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val topics = "kafka_tutorial"

    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreming")
    SparkConf.set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(SparkConf, Seconds(3))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    val line = messages.map(_.value)
    val words = line.flatMap(_.split(" "))
    val wordsCount = words.map(x => (x, 1L)).reduceByKey(_+_)

    wordsCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
