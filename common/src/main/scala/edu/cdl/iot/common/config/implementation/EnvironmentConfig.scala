package edu.cdl.iot.common.config.implementation

import edu.cdl.iot.common.config.RefitConfig
import edu.cdl.iot.common.constants.EnvConstants
import edu.cdl.iot.common.yaml.{CassandraConfig, KafkaConfig, KafkaTopic, MinioBucket, MinioConfig, PostgresConfig}
import org.slf4j.LoggerFactory


class EnvironmentConfig extends RefitConfig with Serializable {

  private val logger = LoggerFactory.getLogger(classOf[EnvironmentConfig])

  override val getKafkaConfig: () => KafkaConfig = () => new KafkaConfig(
    sys.env(EnvConstants.KAFKA_HOST),
    new KafkaTopic(
      modelPublished = sys.env(EnvConstants.MODEL_PUBLISHED_TOPIC),
      data = sys.env(EnvConstants.DATA_TOPIC),
      predictions = sys.env(EnvConstants.PREDICTIONS_TOPIC),
      `import` = sys.env(EnvConstants.IMPORT_TOPIC),
      trainingWindowImport = sys.env(EnvConstants.TRAINING_WINDOW_IMPORT_TOPIC),
      staticDataImport = sys.env(EnvConstants.STATIC_DATA_IMPORT_TOPIC),
      rawSensorData = sys.env(EnvConstants.RAW_SENSOR_DATA_TOPIC),
      sensorData = sys.env(EnvConstants.SENSOR_DATA_TOPIC),
      trainingJobScheduled = sys.env(EnvConstants.TRAINING_JOB_SCHEDULED_TOPIC)
    )
  )

  override val getEncryptionKey: () => String = () => sys.env(EnvConstants.ENCRYPTION_KEY)


  override val getProject: () => String = () => sys.env(EnvConstants.PROJECT)
  override val getCassandraConfig: () => CassandraConfig = () => new CassandraConfig(
    keyspace = sys.env(EnvConstants.CASSANDRA_KEYSPACE),
    host = sys.env(EnvConstants.CASSANDRA_HOST),
    port = sys.env(EnvConstants.CASSANDRA_PORT).toInt,
    user = sys.env(EnvConstants.CASSANDRA_USER),
    password = sys.env(EnvConstants.CASSANDRA_PASSWORD)
  )
  override val runDemo: () => Boolean = () => sys.env(EnvConstants.DEMO).toBoolean
  override val getMinioConfig: () => MinioConfig = () => {
    val buckets = new MinioBucket(
      `import` = sys.env(EnvConstants.MINIO_BUCKET_IMPORT),
      models = sys.env(EnvConstants.MINIO_BUCKET_MODELS),
      schema = sys.env(EnvConstants.MINIO_BUCKET_SCHEMA)
    )

    logger.info("Minio buckets loaded from environment")
    logger.info(s"Import Bucket: ${buckets.`import`}")
    logger.info(s"Model Bucket: ${buckets.models}")
    logger.info(s"Schema Bucket: ${buckets.schema}")

    new MinioConfig(
      host = sys.env(EnvConstants.MINIO_HOST),
      accessKey = sys.env(EnvConstants.MINIO_ACCESS_KEY),
      secretKey = sys.env(EnvConstants.MINIO_SECRET_KEY),
      buckets = buckets
    )
  }
  /*
  override val getPostgresConfig: () => PostgresConfig = () => new PostgresConfig(
    schema = sys.env(EnvConstants.POSTGRES_SCHEMA),
    host = sys.env(EnvConstants.POSTGRES_HOST),
    username = sys.env(EnvConstants.POSTGRES_USERNAME),
    password = sys.env(EnvConstants.POSTGRES_PASSWORD),
    port = 5432
  )
*/

  //
    override val getPostgresConfig: () => PostgresConfig = () => {
    // Extracting environment variables
    val postgresSchema = sys.env(EnvConstants.POSTGRES_SCHEMA)
    val postgresHost = sys.env(EnvConstants.POSTGRES_HOST)
    val postgresUsername = sys.env(EnvConstants.POSTGRES_USERNAME)
    val postgresPassword = sys.env(EnvConstants.POSTGRES_PASSWORD) // You might not want to log this

    // Logging the information for debugging
    logger.info(s"Postgres Schema: $postgresSchema")
    logger.info(s"Postgres Host: $postgresHost")
    logger.info(s"Postgres Username: $postgresUsername")
    logger.info(s"Postgres Password is null or empty: $postgresPassword") 

    // Constructing the PostgresConfig object
    new PostgresConfig(
      schema = postgresSchema,
      host = postgresHost,
      username = postgresUsername,
      password = postgresPassword,
      port = 5432 // This is hardcoded, consider making it configurable if needed
    )
  }

  
}
