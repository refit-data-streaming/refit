lazy val `ingestion-cassandra` = project in file("../ingestion-cassandra")
lazy val `ingestion-kafka` = project in file("../ingestion-kafka")
lazy val `ingestion-core` = project in file("../ingestion-core")
lazy val `ingestion-minio` = project in file("../ingestion-minio")
lazy val `scheduler-ingestion` = project in file("../../scheduler/scheduler-camel/scheduler-ingestion")

name := "ingestion-application"
Settings.assembly

libraryDependencies ++= Dependencies.camel
assemblyJarName in assembly := "ingestion.jar"
mainClass in (run / assembly) := Some("edu.cdl.iot.ingestion.application.CamelMain")

dependsOn(
  `ingestion-core`,
  `ingestion-cassandra`,
  `ingestion-kafka`,
  `ingestion-minio`,
  `scheduler-ingestion`,
  Shared.protocol
)
