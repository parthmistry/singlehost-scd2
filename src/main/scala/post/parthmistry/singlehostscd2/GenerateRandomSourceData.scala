package post.parthmistry.singlehostscd2

import org.apache.spark.sql.SparkSession
import post.parthmistry.singlehostscd2.service.{DataGeneratorService, SparkService}

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable
import scala.util.Random

object GenerateRandomSourceData {

  def main(args: Array[String]): Unit = {
    val startId = args(0).toInt
    val targetDir = args(1)

    if (startId <= 1000000) {
      throw new RuntimeException("start id is too small")
    }

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("random-data-generator").getOrCreate()

    val effStartDate = new Timestamp(Instant.now().minus(10, ChronoUnit.DAYS).toEpochMilli)

    val newRecords = DataGeneratorService.generateRecords(startId, 250000, effStartDate)

    val random = new Random()
    val generatedIds = mutable.Set[Int]()

    while (generatedIds.size < 750000) {
      generatedIds += (random.nextInt(startId - 1) + 1)
    }

    val existingRecords = generatedIds.map(id => {
      DataGeneratorService.generateRecord(id, effStartDate)
    }).toList

    SparkService.writeSparkData(spark, existingRecords ++ newRecords, targetDir)
  }

}
