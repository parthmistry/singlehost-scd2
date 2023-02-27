package post.parthmistry.singlehostscd2

import org.apache.spark.sql.SparkSession

object MergeJob {

  def main(args: Array[String]): Unit = {
    val sourceDir = args(0)
    val targetDir = args(1)

    val start = System.currentTimeMillis()
    println("merge job start")

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .appName("merge-job").getOrCreate()

    val sourceDF = spark.read.format("delta").load(sourceDir)
    sourceDF.createOrReplaceTempView("product_updates")

    val effStartDate = sourceDF.select("effStartDate").head().getTimestamp(0)

    val targetDF = spark.read.format("delta").load(targetDir)
    targetDF.createOrReplaceTempView("products")

    spark.sql(
      s"""
         |merge into products d using (
         |  select
         |  s.id as key,
         |  s.*
         |  from product_updates s
         |  union all
         |  select
         |  null as key,
         |  s.*
         |  from product_updates s
         |  join products d
         |  on s.id = d.id
         |  and d.isCurrent = true
         |  where not (
         |  s.name = d.name and
         |  s.brand = d.brand and
         |  s.description = d.description and
         |  s.category = d.category and
         |  s.price = d.price and
         |  s.color = d.color and
         |  s.weight = d.weight and
         |  s.imageUrl = d.imageUrl and
         |  s.customAttribute = d.customAttribute
         |  )
         |) u on d.id = u.key
         |when matched and d.isCurrent = true and not (
         |  u.name = d.name and
         |  u.brand = d.brand and
         |  u.description = d.description and
         |  u.category = d.category and
         |  u.price = d.price and
         |  u.color = d.color and
         |  u.weight = d.weight and
         |  u.imageUrl = d.imageUrl and
         |  u.customAttribute = d.customAttribute
         |  ) then update set
         |  d.effEndDate = '$effStartDate',
         |  d.isCurrent = false
         |when not matched then
         |  insert *
         |""".stripMargin)

    val end = System.currentTimeMillis()
    println("merge job complete - " + (end - start))
  }

}
