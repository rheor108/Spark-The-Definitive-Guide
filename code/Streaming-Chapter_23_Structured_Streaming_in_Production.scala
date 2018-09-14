%dep
z.load("org.apache.kafka:kafka-clients:0.10.2.2")

// COMMAND ----------

// in Scala
val static = spark.read.json("/data/activity-data")
val streaming = (spark
  .readStream
  .schema(static.schema)
  .option("maxFilesPerTrigger", 10)
  .json("/data/activity-data")
  .groupBy("gt")
  .count())
val query = (streaming
  .writeStream
  .outputMode("complete")
  .option("checkpointLocation", "/some/location/")
  .queryName("test_stream")
  .format("memory")
  .start())


// COMMAND ----------

query.status


// COMMAND ----------

query.recentProgress


// COMMAND ----------

//val spark: SparkSession = ...

spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
    }
    override def onQueryTerminated(
      queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
    }
}


// COMMAND ----------

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.streaming.StreamingQueryListener

class KafkaMetrics(servers: String) extends org.apache.spark.sql.streaming.StreamingQueryListener {
  val kafkaProperties = new java.util.Properties()
  kafkaProperties.put(
    "bootstrap.servers",
    servers)
  kafkaProperties.put(
    "key.serializer",
    "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put(
    "value.serializer",
    "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")

  val producer = new org.apache.kafka.clients.producer.KafkaProducer[String, String](kafkaProperties)

  import org.apache.spark.sql.streaming.StreamingQueryListener
  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerRecord

  override def onQueryProgress(event:
                               StreamingQueryListener.QueryProgressEvent): Unit = {
    producer.send(new ProducerRecord("streaming-metrics",
      event.progress.json))
  }
  override def onQueryStarted(event:
                              StreamingQueryListener.QueryStartedEvent): Unit = {}
  override def onQueryTerminated(event:
                                 StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}




// COMMAND ----------

