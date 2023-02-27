# singlehost-scd2

## To generate products table with 1 billion records
```bash
spark-submit \
  --master "local[*, 4]" \
  --driver-memory 12g \
  --executor-memory 12g \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.2.0 \
  --conf spark.local.dir=/mnt-temp/spark \
  --class post.parthmistry.singlehostscd2.GenerateTargetData \
  /home/ubuntu/spark-job.jar <iterations> <generate_path>
```
**iterations:** number of iterations for generating products table records. Each iteration generates 1 million records  
**generate_path:** path of products table where data will be generated


## To optimize and vacuum products table
```bash
spark-submit \
  --master "local[*, 4]" \
  --driver-memory 12g \
  --executor-memory 12g \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.2.0 \
  --conf spark.local.dir=/mnt-temp/spark \
  --conf spark.databricks.delta.retentionDurationCheck.enabled=false \
  --conf spark.sql.files.maxRecordsPerFile=2000000 \
  --class post.parthmistry.singlehostscd2.OptimizeTable \
  /home/ubuntu/spark-job.jar <table_path>
```
**table_path:** path of products table to be optimized


## To generate product_updates table records with random identifiers
```bash
spark-submit \
  --master "local[*, 4]" \
  --driver-memory 12g \
  --executor-memory 12g \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.2.0 \
  --conf spark.local.dir=/mnt-temp/spark \
  --class post.parthmistry.singlehostscd2.GenerateRandomSourceData \
  /home/ubuntu/spark-job.jar <start_id> <generate_path>
```
**start_id:** first identifier for range of new records  
**generate_path:** path of product_updates table where data will be generated


## To generate product_updates table records with only new records
```bash
spark-submit \
  --master "local[*, 4]" \
  --driver-memory 12g \
  --executor-memory 12g \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.2.0 \
  --conf spark.local.dir=/mnt-temp/spark \
  --class post.parthmistry.singlehostscd2.GenerateNewSourceData \
  /home/ubuntu/spark-job.jar <start_id> <generate_path>
```
**start_id:** first identifier for range of new records  
**generate_path:** path of product_updates table where data will be generated


## To generate product_updates table records by picking product ids from targeted files
```bash
spark-submit \
  --master "local[*, 4]" \
  --driver-memory 12g \
  --executor-memory 12g \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.2.0 \
  --conf spark.local.dir=/mnt-temp/spark \
  --class post.parthmistry.singlehostscd2.GenerateTargetedSourceData \
  /home/ubuntu/spark-job.jar <start_id> <target_path> <generate_path> <target_percentage>
```
**start_id:** first identifier for range of new records  
**target_path:** path of products table from which data files will be targeted  
**generate_path:** path of product_updates table where data will be generated  
**target_percentage:** percentage of target number of files to target

