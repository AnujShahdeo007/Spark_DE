from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("MacVSCodeSparkUI")
    .master("local[2]")
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

rdd = sc.parallelize([1,2,3,4], 2)
print("Spark Version:", spark.version)
print("Partitions:", rdd.getNumPartitions())
print("Collect:", rdd.collect())

print("Spark UI: http://localhost:4040 (try 4041/4042 if busy)")
print("Keeping app alive for 180 seconds...")
time.sleep(180)

spark.stop()
PY
