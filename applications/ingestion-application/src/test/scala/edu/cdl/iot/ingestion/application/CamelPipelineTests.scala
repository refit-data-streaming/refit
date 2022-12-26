package edu.cdl.iot.ingestion.application

import edu.cdl.iot.common.factories.SchemaFactory
import edu.cdl.iot.data.kafka.KafkaRepository
import edu.cdl.iot.integrations.notebook.core.entity.DirectImport
import edu.cdl.iot.integrations.notebook.core.factory.SensorDataFactory
import edu.cdl.iot.common.yaml.{KafkaConfig, KafkaTopic}
import edu.cdl.iot.integrations.notebook.kafka.repository.NotebookKafkaSensorDataRepository
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.io.{File, FileInputStream}

class CamelPipelineTests extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll  {
  val schemaDirectory = s"${System.getProperty("user.dir")}/common/src/main/resources/schema"
  val schemaFileName = s"$schemaDirectory/demo.yaml"
  val input = new FileInputStream(new File(schemaFileName))
  val schema = SchemaFactory.parse(input)
  val sensorDataFactory = new SensorDataFactory(schema)
  val data = new DirectImport("1,2,timestamp,1.0,2.0,3.0")
  val sensorData = sensorDataFactory.fromCsv(data.getData)

  val kafkaConfig = new KafkaConfig(
    "refit-kafka:9092",
    new KafkaTopic(
      modelPublished = "refit.training.models",
      data = "refit.inference.data",
      predictions = "refit.inference.predictions",
      `import` = "refit.training.import",
      trainingWindowImport = "KAFKA_TRAINING_WINDOW_IMPORT_TOPIC",
      staticDataImport = "KAFKA_STATIC_DATA_IMPORT_TOPIC",
      rawSensorData = "refit.inference.raw.data",
      sensorData = "refit.inference.sensor.data",
      trainingJobScheduled = "KAFKA_TRAINING_JOB_SCHEDULED"
    ))

  // val kafkaRepository = new KafkaRepository(kafkaConfig, "ingestion")
  // val sensorDataRepository = new NotebookKafkaSensorDataRepository(kafkaRepository)
  // sensorDataRepository.createSensorData(sensorData)

  //  val importService = new NotebookImportService(
  //    minioConfig = null,
  //    fileRepository = null,
  //    projectRepository = null,
  //    sensorDataRepository = sensorDataRepository,
  //    trainingWindowRepository = null,
  //    staticDataRepository = null,
  //    importRepository = null,
  //    trainingWindowImportRepository = null,
  //    staticDataImportRepository = null
  //  )
  //importService.performDirectSensorDataImport(projectGuid, data)


}
