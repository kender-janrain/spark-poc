package janrain.jedi.logging.poc

import scala.util.Try
import spray.json.{JsObject, JsonParser}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.SparkContext
import spray.json.DefaultJsonProtocol._
import java.net.URL

trait StreamSpike {
  def stream(streamingContext: StreamingContext): DStream[String]

  def apply(sparkContext: SparkContext, checkpointLocation: String) {
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    streamingContext.checkpoint(checkpointLocation)
//    streamingContext.checkpoint("./target/spark/checkpoint")

    stream(streamingContext) map { value ⇒
      Try { JsonParser(value).asJsObject } getOrElse JsObject()
    } filter { js ⇒
      js has ("request-uri", "timer-elapsed")
    } map { js ⇒
      (js string "request-uri", js.fields("timer-elapsed").convertTo[String].toLong )
    } map { case (uri, timer_elapsed) =>
      (new URL(uri).getHost, timer_elapsed)
    } checkpoint Seconds(60) window(Seconds(10)) foreachRDD { rdd ⇒
      rdd.groupBy(_._1) map { case (host, times) ⇒
        (host, times.length, times.map(_._2).sum / times.length)
      } foreach {
        case ("ecic--dev1.cs18.my.salesforce.com", num_timers, avg) ⇒ println(s"$num_timers, $avg")
        case (host, num_timers, avg) ⇒
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
