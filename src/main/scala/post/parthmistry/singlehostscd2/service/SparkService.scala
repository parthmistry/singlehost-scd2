package post.parthmistry.singlehostscd2.service

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{SaveMode, SparkSession}
import post.parthmistry.singlehostscd2.data.ProductDim

object SparkService {

  def writeSparkData(spark: SparkSession, records: List[ProductDim], targetDir: String): Unit = {
    import spark.implicits._

    val df = records.toDF
      .repartition(10, col("id"))
      .withColumn("price", col("price").cast(DecimalType(10, 2)))

    this.synchronized {
      df.write
        .format("delta")
        .mode(SaveMode.Append)
        .save(targetDir)
    }
  }

}
