package janrain.jedi.logging.poc

import scala.util.Try
import spray.json.{JsObject, JsonParser}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.SparkContext

trait StreamSpike {
  def stream(streamingContext: StreamingContext): DStream[String]

  def apply(sparkContext: SparkContext, checkpointLocation: String) {
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    streamingContext.checkpoint(checkpointLocation)
//    streamingContext.checkpoint("./target/spark/checkpoint")

    stream(streamingContext) map { value ⇒
      Try { JsonParser(value).asJsObject } getOrElse JsObject()
    } filter { js ⇒
      js has ("capture-app-id", "webhook-callback", "webhook-response-code")
    } map { js ⇒
      (js string "capture-app-id", js string "webhook-callback", js string "webhook-response-code")
    } checkpoint Seconds(60) countByValueAndWindow(Seconds(10), Seconds(5)) foreachRDD { rdd ⇒
      println(s"--- ${new java.util.Date} ---")
      rdd foreach println
      println()
    }


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
