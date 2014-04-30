package janrain.jedi.logging.poc

import java.util
import scala.beans.BeanProperty
import akka.actor._
import org.apache.spark.streaming.receivers.Receiver
import com.amazonaws.auth.{AWSCredentialsProvider, AWSCredentials}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessorCheckpointer, IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import java.util.concurrent.Executors

object KinesisReceiver {
  sealed trait State
  private case object Stopped extends State
  private case object Running extends State

  sealed trait Data
  private case object InitializingData extends Data
  private case class RunningData(worker: Worker) extends Data

  private case object Start
  private case class KinesisData(data: Array[Byte])

  def createWorker(awsCredentials: AWSCredentials, streamName: String)(onRecord: (Record) ⇒ Unit) = {
    val recordProcessorFactory = new IRecordProcessorFactory {
      def createProcessor() = new IRecordProcessor {
        import scala.collection.JavaConverters._
        println("kinesis processor created")
        def initialize(shardId: String) {
          println(s"kinesis: initializing(shardId=$shardId)")
        }

        def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) {
          if (reason == ShutdownReason.TERMINATE) checkpointer.checkpoint()
        }

        def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer) {
          println(s"kinesis: ${records.size} records")
          records.asScala foreach { record ⇒
            onRecord(record)
          }
          checkpointer.checkpoint()
        }
      }
    }

    val config = new KinesisClientLibConfiguration(
      "kender-logging-poc",
      streamName,
      new AWSCredentialsProvider {
        @BeanProperty val credentials = awsCredentials
        def refresh() {}
      },
      "kender-local")

    new Worker(recordProcessorFactory, config)
  }
}

class KinesisReceiver(awsCredentials: AWSCredentials, streamName: String)
  extends Actor
  with ActorLogging
  with Receiver
  with FSM[KinesisReceiver.State, KinesisReceiver.Data]
  with LoggingFSM[KinesisReceiver.State, KinesisReceiver.Data] {
  import KinesisReceiver._

  when(Stopped) {
    case Event(Start, _) ⇒
      val receiver = self
      val worker = createWorker(awsCredentials, streamName) { record ⇒
        receiver ! KinesisData(record.getData.array())
      }
      Executors.newFixedThreadPool(1).submit(worker)
      goto(Running) using RunningData(worker)
  }

  when(Running) {
    case Event(KinesisData(data), _) ⇒
      println(s"kinesis: ${data.length} bytes")
      pushBlock(data)
      stay()
  }

  startWith(Stopped, InitializingData)
  initialize()

  override def preStart() {
    println("KinesisReceiver.preStart()")
    self ! Start
  }
}
