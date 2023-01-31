package edu.cdl.iot.data.kafka

import java.util.concurrent.Future
import scala.reflect.internal.util.Collections
import edu.cdl.iot.common.yaml.{KafkaConfig, KafkaTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecords}
import java.time.Duration
import scala.collection.JavaConverters._

class KafkaRepository(config: KafkaConfig, groupIdentifier: String) {
  private val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](config.getProperties(groupIdentifier))
  private val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.getProperties(groupIdentifier))
  val topics: KafkaTopic = config.topics

  def send(topic: String, payload: Array[Byte]): Future[RecordMetadata] = kafkaProducer.send((new ProducerRecord[Array[Byte], Array[Byte]](
    topic,
    payload
  )))

  def receive(topic: String)   = {
    kafkaConsumer.subscribe(List(topic).asJavaCollection)
    val records: ConsumerRecords[Array[Byte], Array[Byte]] = kafkaConsumer.poll(Duration.ofMillis(1000))
    records.asScala.map(record => record.value()).toList
  }
//  def receive(topic: String): ConsumerRecords[Array[Byte], Array[Byte]] = {
//    kafkaConsumer.subscribe(List(topic).asJava)
//    kafkaConsumer.poll(10)
//  }

}
