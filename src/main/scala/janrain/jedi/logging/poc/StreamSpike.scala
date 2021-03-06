package janrain.jedi.logging.poc

import java.io.FileWriter

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
    } checkpoint Seconds(60) window Seconds(10) foreachRDD { rdd ⇒
      rdd.groupBy(_._1) map { case (host, times) ⇒
        (host, times.size, times.map(_._2).sum / times.size)
      } foreach { case (host, num_timers, avg) ⇒
        val output = new FileWriter("./target/spark.out")
        try {
          output.write(s"$host: $num_timers, $avg\n")
        } finally {
          output.close()
        }
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
