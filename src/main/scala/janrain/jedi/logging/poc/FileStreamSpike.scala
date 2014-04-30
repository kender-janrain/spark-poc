package janrain.jedi.logging.poc

import org.apache.spark.streaming.StreamingContext

class FileStreamSpike extends StreamSpike {
  override def stream(streamingContext: StreamingContext) = {
    streamingContext.textFileStream(".")
  }
}
