package janrain.jedi.logging.poc

import org.apache.spark.SparkContext
import spray.json._

object BatchSpike {
  import DefaultJsonProtocol._
  def parse(line: String): JsObject = {
    JsonParser(line.substring(line.indexOf("{"))).asJsObject
  }

  def apply(sparkContext: SparkContext) {
    val job = sparkContext.textFile("s3n://janrain-logs/*.app.janraincapture.com/apid/2014-04-28/*.log") collect {
      case value if value.length > 0 ⇒ parse(value)
    } collect {
      case json: JsObject if json.has("duration", "application", "api_name") ⇒ json
    } groupBy { json ⇒
      (json string "application", json string "api_name")
    } map { case (k, vs) ⇒
      val durations = vs map { _.fields("duration").convertTo[Float] }
      val long = durations count { _ > 250 }
      k → (long.toFloat / durations.size.toFloat, durations.size)
    }

    println(job.collect().mkString("\n"))
  }
}
