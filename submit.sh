#hdfs dfs -put ./target/scala-2.11/SimilarPubmed-assembly-1.0-deps.jar hdfs://hpc2:9000/jars/ 
hdfs dfs -rm hdfs://soldier1:9000/jars/similarpubmed_2.11-1.0.jar 
hdfs dfs -put ./target/scala-2.11/similarpubmed_2.11-1.0.jar hdfs://soldier1:9000/jars/ 
spark-submit \
  --class topic.Main \
  --deploy-mode cluster \
  --master spark://soldier1:6066 \
  --executor-memory 10g \
  --driver-memory 4g \
  --conf "spark.driver.extraClassPath=/home/gcbi/jars/SimilarPubmed-assembly-1.0-deps.jar" \
  --conf "spark.driver.maxResultSize=0" \
  hdfs://soldier1:9000/jars/similarpubmed_2.11-1.0.jar \
  "hdfs://soldier1:9000/data/svd/us.bin" \
  "hdfs://soldier1:9000/raw/pmid_range_may10.txt" \
  "hdfs://soldier1:9000/data/svd/topResult_1w"
