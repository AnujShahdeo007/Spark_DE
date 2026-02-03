"""
Spark Application – COMPLETE INTERNAL WORKING (with explanations)

This script explains:
- Spark Application lifecycle
- Driver, Executors
- Lazy evaluation
- Transformations vs Actions
- Narrow vs Wide transformations
- Jobs, Stages, Tasks
- Shuffle
- Cache / Storage
- Repartition vs Coalesce
- Broadcast variables
- DataFrame vs RDD
- Spark UI (Jobs / Stages / DAG / Storage / Executors)

Use this script for LIVE teaching with Spark UI.
"""

from pyspark.sql import SparkSession
import time


def pause():
    input("\nPress ENTER after checking Spark UI...\n")


# -------------------------------------------------------------------
# STEP 0: START SPARK APPLICATION
# -------------------------------------------------------------------
print("\nSTEP 0: STARTING SPARK APPLICATION")
print("TOPIC: Spark Application Lifecycle | Driver & Executors")
print("""
WHAT IS HAPPENING?
- A Spark APPLICATION is being created.
- Spark DRIVER (this Python process) starts first.
- Spark creates EXECUTORS (JVM threads in local mode).

WHY?
- Driver coordinates execution.
- Executors execute tasks on data partitions.

UI CHECK:
- Open Spark UI link printed below
- Go to 'Executors' tab
""")

spark = (
    SparkSession.builder
    .appName("Spark-UI-Full-Explained")
    .master("local[2]")  # 2 cores → max 2 tasks in parallel
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"\nSpark Version: {spark.version}")
print(f"Spark UI URL: {sc.uiWebUrl}")
pause()


# -------------------------------------------------------------------
# STEP 1: CREATE RDD (TRANSFORMATION – LAZY)
# -------------------------------------------------------------------
print("\nSTEP 1: CREATING RDD USING parallelize()")
print("TOPIC: RDD Creation | Lazy Evaluation")

print("""
WHAT IS HAPPENING?
- We create an RDD with 4 partitions.
- NO computation happens yet.

WHY?
- Spark uses LAZY EVALUATION.
- Transformations only build a LOGICAL PLAN.

UI CHECK:
- Jobs tab should show NOTHING yet.
""")

rdd = sc.parallelize(range(1, 101), 4)
print("RDD partitions:", rdd.getNumPartitions())
pause()


# -------------------------------------------------------------------
# STEP 2: ACTION → JOB CREATION
# -------------------------------------------------------------------
print("\nSTEP 2: ACTION – count()")
print("TOPIC: Actions | Jobs | Tasks")

print("""
WHAT IS HAPPENING?
- count() is an ACTION.
- This triggers actual execution.
- Spark creates:
    → 1 JOB
    → 1 or more STAGES
    → Multiple TASKS (one per partition)

WHY?
- Actions require a final result at Driver.

UI CHECK:
- Jobs tab → new completed job
- Click job → Stages → Tasks
""")

print("RDD count:", rdd.count())
pause()


# -------------------------------------------------------------------
# STEP 3: NARROW TRANSFORMATIONS
# -------------------------------------------------------------------
print("\nSTEP 3: map() and filter()")
print("TOPIC: Narrow Transformations")

print("""
WHAT IS HAPPENING?
- map() and filter() are NARROW transformations.
- Each output partition depends on ONE input partition.

WHY?
- No data movement across partitions.
- Spark can PIPELINE operations in same stage.

UI CHECK:
- DAG shows a straight pipeline
- No shuffle
""")

mapped = rdd.map(lambda x: x * 10)
filtered = mapped.filter(lambda x: x % 3 == 0)

print("Sample output:", filtered.take(5))
pause()


# -------------------------------------------------------------------
# STEP 4: WIDE TRANSFORMATION (SHUFFLE)
# -------------------------------------------------------------------
print("\nSTEP 4: reduceByKey() – SHUFFLE")
print("TOPIC: Wide Transformations | Shuffle | Stages")

print("""
WHAT IS HAPPENING?
- reduceByKey() is a WIDE transformation.
- Data with same key must move to same partition.

WHY?
- Requires SHUFFLE (network + disk IO).
- Spark splits execution into MULTIPLE STAGES.

UI CHECK:
- Jobs → latest job
- You will see 2 stages
- Shuffle Read / Shuffle Write visible
""")

pairs = sc.parallelize(range(1, 1001), 6).map(lambda x: (x % 10, 1))
reduced = pairs.reduceByKey(lambda a, b: a + b)

print("reduceByKey result:", reduced.collect())
pause()


# -------------------------------------------------------------------
# STEP 5: repartition vs coalesce
# -------------------------------------------------------------------
print("\nSTEP 5: repartition() vs coalesce()")
print("TOPIC: Partition Management")

print("""
WHAT IS HAPPENING?
- repartition() ALWAYS causes shuffle.
- coalesce() tries to AVOID shuffle.

WHY?
- repartition balances data evenly.
- coalesce reduces partitions efficiently.

UI CHECK:
- repartition → shuffle visible
- coalesce → simpler DAG
""")

base = sc.parallelize(range(1, 41), 2)

rep = base.repartition(6)
print("repartition output:", rep.glom().collect())
pause()

coal = rep.coalesce(2)
print("coalesce count:", coal.count())
pause()


# -------------------------------------------------------------------
# STEP 6: CACHE / STORAGE
# -------------------------------------------------------------------
print("\nSTEP 6: cache()")
print("TOPIC: Persistence | Memory Optimization")

print("""
WHAT IS HAPPENING?
- cache() stores RDD in memory.
- First action computes & stores data.
- Next actions reuse cached data.

WHY?
- Avoid recomputation.
- Improves performance.

UI CHECK:
- Storage tab → cached RDD visible
""")

big = sc.parallelize(range(1, 500000), 8).map(lambda x: x * 2).cache()

print("First action (fills cache):", big.count())
pause()

print("Second action (uses cache):", big.sum())
pause()


# -------------------------------------------------------------------
# STEP 7: BROADCAST VARIABLE
# -------------------------------------------------------------------
print("\nSTEP 7: Broadcast Variable")
print("TOPIC: Shared Variables")

print("""
WHAT IS HAPPENING?
- Small lookup table broadcasted to executors.

WHY?
- Avoid sending data with every task.
- Used heavily in joins.

UI NOTE:
- In local mode broadcast impact is small.
""")

lookup = {i: f"val_{i}" for i in range(10)}
b = sc.broadcast(lookup)

rdd_lookup = sc.parallelize(range(20), 4).map(lambda x: (x, b.value[x % 10]))
print("Broadcast result:", rdd_lookup.take(5))
pause()


# -------------------------------------------------------------------
# STEP 8: DATAFRAME / SQL
# -------------------------------------------------------------------
print("\nSTEP 8: DATAFRAME API")
print("TOPIC: SparkSession | SQL Engine")

print("""
WHAT IS HAPPENING?
- DataFrame operations use Spark SQL engine.
- Optimized via Catalyst & Tungsten.

UI CHECK:
- SQL tab appears
- Logical & Physical plans visible
""")

df = spark.range(0, 100000).repartition(4)
df2 = df.where("id % 7 = 0").groupBy().count()

print("DataFrame result:", df2.collect())
pause()


# -------------------------------------------------------------------
# FINAL STEP
# -------------------------------------------------------------------
print("\nFINAL STEP: STOPPING SPARK")
print("""
WHAT IS HAPPENING?
- SparkContext stops.
- Executors shut down.
- Spark UI disappears.

WHY?
- Application lifecycle ends.
""")

spark.stop()
print("Spark stopped.")
