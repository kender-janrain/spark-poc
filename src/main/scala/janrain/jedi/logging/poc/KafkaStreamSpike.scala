package janrain.jedi.logging.poc

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel

object KafkaStreamSpike extends StreamSpike {
  override def stream(streamingContext: StreamingContext) = {
    val kafkaConfig = Map(
      "zookeeper.connect" → "localhost:2181/kafka",
      "group.id" → "jedi-logging-poc",
      "consumer.id" → "jedi-logging-poc"
    )

    val kafkaTopics = Map(
      "jedi-kender" → 1
    )

    val records = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      streamingContext, kafkaConfig, kafkaTopics, StorageLevel.MEMORY_ONLY_SER)

    records map { case (_, value) ⇒ value }
  }
}
