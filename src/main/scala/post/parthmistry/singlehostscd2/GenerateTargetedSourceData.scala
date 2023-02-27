package post.parthmistry.singlehostscd2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import post.parthmistry.singlehostscd2.service.{DataGeneratorService, SparkService}

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

object GenerateTargetedSourceData {

  def main(args: Array[String]): Unit = {
    val startId = args(0).toInt
    val sourceDir = args(1)
    val targetDir = args(2)
    val targetPercent = args(3).toInt

    if (startId <= 1000000) {
      throw new RuntimeException("start id is too small")
    }

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("targeted-data-generator").getOrCreate()

    val effStartDate = new Timestamp(Instant.now().minus(10, ChronoUnit.DAYS).toEpochMilli)

    val newRecords = DataGeneratorService.generateRecords(startId, 250000, effStartDate)

    val existingRecordsDF = spark.read.format("delta").load(sourceDir)
    val inputFiles = existingRecordsDF.withColumn("input_file", input_file_name())
      .select("input_file")
      .distinct()
      .collect()
      .map(row => row.getString(0))
      .toSet

    val targetFileCount = (inputFiles.size * targetPercent / 100.0).toInt
    val perFileUpdateCount = (750000.0 / targetFileCount).ceil.toInt

    val targetedFiles = inputFiles.take(targetFileCount)

    val generatedIds = targetedFiles.par.flatMap(targetedFile => {
      spark.read.format("parquet").load(targetedFile)
        .select("name")
        .distinct()
        .take(perFileUpdateCount)
        .map(row => row.getString(0))
    }).take(750000).map(productNameString => productNameString.replace("name_", "").toInt)

    val existingRecords = generatedIds.map(id => {
      DataGeneratorService.generateRecord(id, effStartDate)
    }).toList

    SparkService.writeSparkData(spark, existingRecords ++ newRecords, targetDir)

    println(s"total input files: ${inputFiles.size}")
    println(s"total targeted files: $targetFileCount")
    println(s"per file update count: $perFileUpdateCount")
  }

}
