package post.parthmistry.singlehostscd2

import org.apache.spark.sql.SparkSession
import post.parthmistry.singlehostscd2.service.{DataGeneratorService, SparkService}

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors

object GenerateTargetData {

  private val BATCH_SIZE = 1000000

  def main(args: Array[String]): Unit = {
    val iterations = args(0).toInt
    val targetDir = args(1)

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("target-data-generator").getOrCreate()

    val effStartDate = new Timestamp(Instant.now().minus(10, ChronoUnit.DAYS).toEpochMilli)

    val executor = Executors.newFixedThreadPool(4)

    val records = DataGeneratorService.generateRecords(1, BATCH_SIZE, effStartDate)
    SparkService.writeSparkData(spark, records, targetDir)

    (1 until iterations).map(i => {
      executor.submit(new Runnable {
        override def run(): Unit = {
          val records = DataGeneratorService.generateRecords(i * BATCH_SIZE + 1, BATCH_SIZE, effStartDate)
          SparkService.writeSparkData(spark, records, targetDir)
        }
      })
    }).foreach(_.get())

    executor.shutdown()
  }

}
