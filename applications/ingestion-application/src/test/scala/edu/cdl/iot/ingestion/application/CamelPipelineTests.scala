package edu.cdl.iot.ingestion.application

import edu.cdl.iot.common.factories.SchemaFactory
import edu.cdl.iot.data.kafka.KafkaRepository
import edu.cdl.iot.integrations.notebook.core.entity.DirectImport
import edu.cdl.iot.integrations.notebook.core.factory.SensorDataFactory
import edu.cdl.iot.common.yaml.{KafkaConfig, KafkaTopic}
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
  // SensorData(b6ee5bab-08dd-49b0-98b6-45cd0a28b12f,2,timestamp,
  // Map(temperature -> 1.0, pressure -> 2.0, wind -> 3.0),Map(),Map(),Map(),Map(),Map())
  // assert(sensorData == 1)
  val kafkaConfig = new KafkaConfig(
    "localhost:19092",
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

  val kafkaRepository = new KafkaRepository(kafkaConfig, "ingestion")
  kafkaRepository.send(kafkaRepository.topics.data, sensorData.toByteArray)
  Thread.sleep(3000)
  val records: Iterable[Array[Byte]] = kafkaRepository.receive(kafkaRepository.topics.data)
  Thread.sleep(3000)

  records should not be null

  "Test2" should "Convert from SensorData" in {
    // turn records into sensor data in string
    val receivedData = records.map(record => new String(record, "UTF-8"))
    receivedData should not be null
    assert(receivedData === "1,2,timestamp,1,2,3")
  }








//   val sensorDataRepository = new NotebookKafkaSensorDataRepository(kafkaRepository)
//   sensorDataRepository.createSensorData(sensorData)

//   val importService = new NotebookImportService(
//      minioConfig = null,
//      fileRepository = null,
//      projectRepository = null,
//      sensorDataRepository = sensorDataRepository,
//      trainingWindowRepository = null,
//      staticDataRepository = null,
//      importRepository = null,
//      trainingWindowImportRepository = null,
//      staticDataImportRepository = null
//    )
   // importService.performDirectSensorDataImport(projectGuid, data)


}
