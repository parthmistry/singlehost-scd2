package post.parthmistry.singlehostscd2

import org.apache.spark.sql.{SaveMode, SparkSession}

object CopyData {

  def main(args: Array[String]): Unit = {
    val sourceDir = args(0)
    val targetDir = args(1)

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("copy-data").getOrCreate()

    val dataDF = spark.read.format("delta").load(sourceDir)

    dataDF.write.format("delta").mode(SaveMode.Append).save(targetDir)
  }

}
