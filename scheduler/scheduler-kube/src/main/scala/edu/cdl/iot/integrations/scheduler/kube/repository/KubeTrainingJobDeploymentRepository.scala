package edu.cdl.iot.integrations.scheduler.kube.repository

import java.io.FileReader
import java.io.File
import java.nio.file.Paths

import edu.cdl.iot.common.config.RefitConfig
import edu.cdl.iot.common.constants.EnvConstants
import edu.cdl.iot.integrations.scheduler.core.entity.{KubernetesApiConflict, TrainingJob, TrainingJobDeployment, TrainingJobDeploymentStatus, TrainingJobError, TrainingJobNotComplete}
import edu.cdl.iot.integrations.scheduler.core.repository.TrainingJobDeploymentRepository
import edu.cdl.iot.integrations.scheduler.kube.config.SchedulerKubeConfig
import io.kubernetes.client.openapi.{ApiException, Configuration}
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.models.{V1DeleteOptionsBuilder, V1EnvVarBuilder, V1EnvVarSourceBuilder, V1Job, V1JobBuilder, V1SecretKeySelectorBuilder}
import io.kubernetes.client.util.{ClientBuilder, KubeConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.mapAsJavaMapConverter


class KubeTrainingJobDeploymentRepository(refitConfig: RefitConfig,
                                           config: SchedulerKubeConfig) extends TrainingJobDeploymentRepository {

  //private val userHome = System.getProperty("user.home")
  //private val kubeConfigPath = Paths.get(userHome, ".kube", "config").toString

  private val kubeConfigPath = "/.kube/config"
  private val minioConfig = refitConfig.getMinioConfig()
  private val logger = LoggerFactory.getLogger(classOf[KubeTrainingJobDeploymentRepository])

  // This change relies on the in-cluster configuration and doesn't expect a .kube/config file to be present. 
  // This requires necessary RBAC settings are in place for the pod's service account, so the application has 
  // the required permissions to work with the Kubernetes API
  
  private val client = try {
  logger.info("Using in-cluster configuration")
  ClientBuilder.standard().build()
} catch {
  case e: Exception =>
    logger.error("Could not create the Kubernetes client", e)
    throw e
}
/*
  private val client =
    if (new File(kubeConfigPath).exists() )   {
      logger.info("Kube config file /.kube/config found, using the configuration file")
      ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build()
    } else {
      logger.info("Kube config file /.kube/config not found, using the in cluster configuration")
      ClientBuilder.standard().build()
    }

  */
  /*
  private val kubeConfigFile = new File(kubeConfigPath)
  private val client =
     if (!kubeConfigFile.exists() ) { //|| !kubeConfigFile.canRead()) {
         logger.error(s"Kube config file at $kubeConfigPath either does not exist or is not readable.")
         // Consider throwing an exception or taking alternative action
     } else {
       logger.info("Kube config file /.kube/config found, using the configuration file")
       ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build()
      }*/

  logger.info(s"after config loading.")

  Configuration.setDefaultApiClient(client)
  val api = new BatchV1Api()

  logger.info(s"api instance created.")

  def buildPod(trainingJob: TrainingJob): V1Job =
    new V1JobBuilder()
      .withNewMetadata()
      .withName(s"refit-job-${trainingJob.jobName}")
      .withLabels(Map(
        "project" -> trainingJob.projectGuid.toString,
        "job" -> trainingJob.jobName,
      ).asJava)
      .endMetadata()
      .withApiVersion("batch/v1")
      .withKind("Job")
      .withNewSpec()
      .withTtlSecondsAfterFinished(60)
      .withNewTemplate()
      .withNewSpec()
      .addNewContainer()
      .withEnv(
        new V1EnvVarBuilder()
          .withName("SCRIPT_LOCATION")
          .withValue(s"${trainingJob.projectGuid}/jobs/${trainingJob.jobName}/")
          .build(),
        new V1EnvVarBuilder()
          .withName("SCRIPT_FILE")
          .withValue("index.py")
          .build(),
        new V1EnvVarBuilder()
          .withName("PROJECT_GUID")
          .withValue(trainingJob.projectGuid.toString)
          .build(),
        new V1EnvVarBuilder()
          .withName("MINIO_BUCKET")
          .withValue(config.minioBucket)
          .build(),
        new V1EnvVarBuilder()
          .withName("JOB_NAME")
          .withValue(trainingJob.jobName)
          .build(),
        new V1EnvVarBuilder()
          .withName("MINIO_HOST")
          .withValue(config.minioHost)
          .build(),
        new V1EnvVarBuilder()
          .withName("MINIO_ACCESS_KEY")
          .withValueFrom(new V1EnvVarSourceBuilder()
            .withSecretKeyRef(new V1SecretKeySelectorBuilder()
              .withName(s"${config.releasePrefix}-minio")
              .withKey("accesskey")
              .build())
            .build()
          ).build(),
        new V1EnvVarBuilder()
          .withName("MINIO_SECRET_KEY")
          .withValueFrom(new V1EnvVarSourceBuilder()
            .withSecretKeyRef(new V1SecretKeySelectorBuilder()
              .withName(s"${config.releasePrefix}-minio")
              .withKey("secretkey")
              .build())
            .build()
          ).build(),
        new V1EnvVarBuilder()
          .withName(EnvConstants.MINIO_BUCKET_IMPORT)
          .withValue(minioConfig.buckets.`import`)
          .build(),
        new V1EnvVarBuilder()
          .withName(EnvConstants.MINIO_BUCKET_MODELS)
          .withValue(minioConfig.buckets.models)
          .build(),
        new V1EnvVarBuilder()
          .withName(EnvConstants.MINIO_BUCKET_SCHEMA)
          .withValue(minioConfig.buckets.schema)
          .build(),
        new V1EnvVarBuilder()
          .withName("INTEGRATIONS_HOST")
          .withValue(s"${config.releasePrefix}-integrations")
          .build()
      )
      .withName("python-training")
      .withImage(s"cdliotprototype/cdl-refit-job:${config.refitVersion}")
      .endContainer()
      .withNodeSelector(
        Map("refit" -> "enabled").asJava
      )
      .withRestartPolicy("Never")
      .endSpec()
      .endTemplate()
      .endSpec()
      .build()

  private def availableToSchedule(trainingJob: TrainingJob) = {
    val jobName = s"refit-job-${trainingJob.jobName}"
    try {
      val response = api.readNamespacedJobStatus(jobName, config.namespace, "true")

      if (response != null) {
        val deleteOptions = new V1DeleteOptionsBuilder()
          .withOrphanDependents(false)
          .build()
        api.deleteNamespacedJob(jobName, config.namespace, "true", null, null, false, null, deleteOptions)
        true
      }
    }
    catch {
      case e: ApiException => {
        logger.info("Error deleting previous job", e)
      }
    }
  }

  override def create(trainingJob: TrainingJob): Either[TrainingJobDeployment, TrainingJobError] = {
    try {
      availableToSchedule(trainingJob)
      val pod = buildPod(trainingJob)
      api.createNamespacedJob(config.namespace, pod, "true", null, null)
      Left(
        TrainingJobDeployment(
          name = trainingJob.jobName,
          status = TrainingJobDeploymentStatus.UNKNOWN
        )
      )
    } catch {
      case e: Exception =>
        logger.info("Error scheduling job", e)
        Right(KubernetesApiConflict())
    }
  }
}
