package post.parthmistry.singlehostscd2

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object OptimizeTable {

  def main(args: Array[String]): Unit = {
    val optimizeDir = args(0)

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("optimize-table").getOrCreate()

    val deltaTable = DeltaTable.forPath(spark, optimizeDir)
    deltaTable.optimize().executeZOrderBy("id")
    deltaTable.vacuum(0)
  }

}
