package janrain.jedi.logging.poc

import org.apache.spark.SparkConf

object Main extends App {
  import org.apache.spark.SparkContext

  def accessKeyId = Option(System.getenv("AWS_ACCESS_KEY_ID")) getOrElse {sys.error("AWS_ACCESS_KEY_ID missing")}
  def secretAccessKey = Option(System.getenv("AWS_SECRET_ACCESS_KEY")) getOrElse {sys.error("AWS_SECRET_ACCESS_KEY missing")}

  object SpikeTypes extends Enumeration {
    type SpikeType = Value
    val batch, streamKafka, streamKinesis = Value
  }

  val Array(master, checkpointLocation) = args

  val sparkConfig = new SparkConf()
    .setMaster(master)
//    .setMaster("spark://ec2-174-129-110-220.compute-1.amazonaws.com:7077")
//    .setMaster("local[8]")
//    .setSparkHome(System.getenv("SPARK_HOME"))
    .setAppName("jedi-logging-poc")
    .setJars(SparkContext.jarOfObject(Main))
    .setAll(List(
      "spark.cleaner.ttl" → "240",
//      "spark.local.dir" → "./target/spark",
      "akka.loglevel" → "DEBUG",
      "akka.log-dead-letters" → "100",
      "akka.actor.debug.receive" → "on",
      "akka.actor.debug.fsm" → "on",
      "spark.cores.max" → "12"))

  val sparkContext = new SparkContext(sparkConfig)
//  sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
//  sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
  val spikeType = SpikeTypes.streamKinesis


  spikeType match {
    case SpikeTypes.batch ⇒
      println("running batch")
      BatchSpike(sparkContext)

    case SpikeTypes.streamKafka ⇒
      println("running kafka")
      KafkaStreamSpike(sparkContext, checkpointLocation)

    case SpikeTypes.streamKinesis ⇒
      println("running kinesis")
      KinesisStreamSpike(sparkContext, checkpointLocation)
  }
}

