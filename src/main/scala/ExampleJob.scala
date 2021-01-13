import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters.mapAsJavaMapConverter

object ExampleJob {

  val AppName = "ExampleApp"
  val LogLevel = "FATAL"
  val LocalMaster = "local"
  val LocalEnvironment = "local"

  val RequiredArgs: Array[String] = Array("JOB_NAME", "ENV")

  val SourceArg = "SOURCE"
  val TargetArg = "TARGET"
  val OptionalArgs: Map[String, String] = Map(
    SourceArg -> "target/scala-2.11/classes/test.csv",
    TargetArg -> "target/output/test"
  )

  def main(sysArgs: Array[String]): Unit = {
    val args = GlueArgParser.getResolvedOptions(sysArgs, RequiredArgs)
    val jobName = args("JOB_NAME")
    val sourcePath = args.getOrElse(SourceArg, OptionalArgs(SourceArg))
    val targetPath = args.getOrElse(TargetArg, OptionalArgs(TargetArg))

    val isLocal = args("ENV") == LocalEnvironment

    println("Initializing Spark and GlueContext")
    val glueContext = getGlueContext(isLocal)
    val sparkSession: SparkSession = glueContext.sparkSession

    // Job actions should only happen when executed by AWS Glue
    if (!isLocal) {
      Job.init(jobName, glueContext, args.asJava)
    }

    println(s"Reading from '$sourcePath'")
    val frame: DataFrame = sparkSession.read.csv(sourcePath)

    frame.printSchema()

    println(s"Writing to '$targetPath'")
    frame.write.mode(SaveMode.Overwrite).csv(targetPath)

    // Job actions should only happen when executed by AWS Glue
    if (!isLocal) {
      Job.commit()
    }
  }

  def getGlueContext(isLocal: Boolean): GlueContext = {
    val sc: SparkContext = if (isLocal) {
      // For testing, we need to use local execution
      val conf = new SparkConf().setAppName(AppName).setMaster(LocalMaster)

      new SparkContext(conf)
    } else {
      new SparkContext()
    }

    sc.setLogLevel(LogLevel) // this can be changed to INFO, ERROR, or WARN
    new GlueContext(sc)
  }
}
