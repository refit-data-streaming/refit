package edu.cdl.iot.ingestion.application.dependencies

import edu.cdl.ingestion.scheduler.camel.dependencies.SchedulerDependencies
import edu.cdl.iot.common.config.RefitConfig
import edu.cdl.iot.common.yaml.KafkaTopic
import edu.cdl.iot.data.cassandra.CassandraRepository
import edu.cdl.iot.data.kafka.KafkaRepository
import edu.cdl.iot.data.minio.MinioRepository
import edu.cdl.iot.data.postgres.factory.JdbiFactory
import edu.cdl.iot.ingestion.notebook.camel.dependencies.NotebookDependencies
import edu.cdl.iot.integrations.scheduler.jdbi.mapper.TrainingJobMapper
import org.apache.camel.CamelContext
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.collection.JavaConverters._
import java.util.Properties

class IngestionDependencies(config: RefitConfig,
                            context: CamelContext) {

  // set up kafka topic
  val properties = new Properties()
  val kafka_host = config.getKafkaConfig().getHost()
  properties.setProperty("bootstrap.servers", kafka_host)
  val adminClient = AdminClient.create(properties)
  val kafka_topics = config.getKafkaConfig().getTopics()
  val data = new NewTopic(kafka_topics.data, 1, 1.toShort)
  val predictions = new NewTopic(kafka_topics.predictions, 1, 1.toShort)
  val modelPublished = new NewTopic(kafka_topics.modelPublished, 1, 1.toShort)
  val `import` = new NewTopic(kafka_topics.`import`, 1, 1.toShort)
  val trainingWindowImport = new NewTopic(kafka_topics.trainingWindowImport, 1, 1.toShort)
  val staticDataImport = new NewTopic(kafka_topics.staticDataImport, 1, 1.toShort)
  val rawSensorData = new NewTopic(kafka_topics.rawSensorData, 1, 1.toShort)
  val sensorData = new NewTopic(kafka_topics.sensorData, 1, 1.toShort)
  val trainingJobScheduled = new NewTopic(kafka_topics.trainingJobScheduled, 1, 1.toShort)
  val newTopic = List(data, predictions, modelPublished, `import`, trainingWindowImport, staticDataImport, rawSensorData, sensorData, trainingJobScheduled)
  adminClient.createTopics(newTopic.asJava)

  private val cassandraRepository = new CassandraRepository(config.getCassandraConfig())
  // kafka repository
  private val kafkaRepository = new KafkaRepository(config.getKafkaConfig(), "ingestion")
  private val minioRepository = new MinioRepository(config.getMinioConfig())

  private val jdbi = new JdbiFactory(config.getPostgresConfig())
    .jdbi


  val notebookDependencies = new NotebookDependencies(
    config = config,
    context = context,
    jdbi = jdbi,
    cassandraRepository = cassandraRepository,
    kafkaRepository = kafkaRepository,
    minioRepository = minioRepository
  )
  val schedulerDependencies = new SchedulerDependencies(
    jdbi = jdbi,
    context = context,
    config = config
  )
}
