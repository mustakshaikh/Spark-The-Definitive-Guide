# This runs the mac command to find the exact path to Java 17
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import window, column, desc, col
java17_path = os.popen("/usr/libexec/java_home -v 17").read().strip()

if java17_path:
    os.environ["JAVA_HOME"] = java17_path
    print(f"✅ JAVA_HOME set to: {java17_path}")
else:
    print("❌ Error: Java 17 not found. Please install it or check path.")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Streaming") \
    .getOrCreate()

from pyspark.sql.functions import *

lines = (spark
  .readStream.format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load())

words = lines.select(explode(split(col("value"), "\\s")).alias("word"))
counts = words.groupBy("word").count()
checkpointDir = "./checkpointdir/"
streamingQuery = (counts
  .writeStream
  .format("console")
  .outputMode("complete")
  .trigger(processingTime="1 second")
  .option("checkpointLocation", checkpointDir)
  .start())
#try:
    # Block the thread here
streamingQuery.awaitTermination()
# except KeyboardInterrupt:
#     # This block runs when you hit Ctrl+C
#     print("Stopping stream...")
#     streamingQuery.stop()
#     print("Stream stopped gracefully!")