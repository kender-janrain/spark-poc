package janrain.jedi.logging.poc

import com.amazonaws.auth.AWSCredentials
import akka.actor.Props
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

object KinesisStreamSpike extends StreamSpike {
  def stream(streamingContext: StreamingContext) = {
    val records = {
      val awsSecretAccessKey = streamingContext.sparkContext.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")
      val awsAccessKeyId = streamingContext.sparkContext.hadoopConfiguration.get("fs.s3n.awsAccessKeyId")
      case object Credentials extends AWSCredentials {
        override def getAWSSecretKey = {
          awsSecretAccessKey
        }
        override def getAWSAccessKeyId = {
          awsAccessKeyId
        }
      }
      val props = Props(classOf[KinesisReceiver], Credentials, "jedi-staging-logs")
      streamingContext.actorStream[Array[Byte]](props, "kinesis-actor", StorageLevel.MEMORY_ONLY_SER)
    }

    records map { bytes ⇒
      new String(bytes, "UTF-8")
    }
  }
}
