package edu.cdl.iot.data.kafka.repository

import edu.cdl.iot.data.kafka.KafkaRepository
import edu.cdl.iot.protocol.SensorData.SensorData
import org.slf4j.LoggerFactory

class KafkaSensorDataRepository(kafkaRepository: KafkaRepository) {
  private val logger = LoggerFactory.getLogger(classOf[KafkaSensorDataRepository])
  def createSensorData(sensorData: SensorData): Unit = {
    logger.info("sensorData ready to send to kafka topics 'data': {}",sensorData)
    kafkaRepository.send(kafkaRepository.topics.data, sensorData.toByteArray)
  }
}
