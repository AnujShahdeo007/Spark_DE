"""
Spark UI Full Walkthrough Script (local mode)
Run: python spark_ui_full_walkthrough.py

What you will learn in Spark UI:
- Spark application start (Driver + Executors)
- Jobs created by actions (count, collect, take)
- Stages and tasks
- DAG visualization
- Shuffle (wide transformation) vs no shuffle (narrow)
- Cache / Storage tab
- Broadcast variable / SQL tab (optional)
"""

from pyspark.sql import SparkSession
import time


def pause(msg: str):
    print("\n" + "=" * 80)
    print(msg)
    print("=" * 80)
    input("Press Enter to continue...\n")


def main():
    # 1) Start Spark (forces UI binding to localhost)
    spark = (
        SparkSession.builder
        .appName("Spark-UI-Full-Walkthrough")
        .master("local[2]")  # 2 threads = 2 tasks at a time
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")  # make shuffle visible
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print(f"Spark Version: {spark.version}")
    print(f"Spark UI: {sc.uiWebUrl}")
    print("Open this URL in your browser NOW.\n")

    pause(
        "STEP 0: Spark started.\n"
        "UI tabs to check now:\n"
        "- 'Executors' (driver info)\n"
        "- 'Environment' (Spark configs)\n"
        "Then come back and press Enter."
    )

    # 2) Create an RDD with multiple partitions
    rdd = sc.parallelize(range(1, 101), 4)  # 100 numbers, 4 partitions
    print("RDD created with partitions:", rdd.getNumPartitions())

    pause(
        "STEP 1: No job yet (because transformations are lazy).\n"
        "UI check:\n"
        "- 'Jobs' tab should still be mostly empty.\n"
        "Now we will run an ACTION to trigger a job."
    )

    # 3) ACTION: count() triggers a Job + Stages + Tasks
    print("Action 1: count() -> triggers Job")
    cnt = rdd.count()
    print("count =", cnt)

    pause(
        "STEP 2: After count() job.\n"
        "UI check:\n"
        "- Jobs tab: you will see a completed job\n"
        "- Click Job -> Stages -> see tasks & partitions\n"
        "- Click 'DAG Visualization' for the job"
    )

    # 4) Narrow transformations (map/filter) -> usually no shuffle
    mapped = rdd.map(lambda x: x * 10)
    filtered = mapped.filter(lambda x: x % 3 == 0)

    pause(
        "STEP 3: map/filter created (still no job, lazy).\n"
        "Now we trigger an action to execute them."
    )

    print("Action 2: take(10) -> triggers Job")
    print("take(10) =", filtered.take(10))

    pause(
        "STEP 4: Check the DAG for map/filter.\n"
        "UI check:\n"
        "- Jobs -> latest job -> DAG Visualization\n"
        "Narrow transformations usually create a simple pipeline (no shuffle)."
    )

    # 5) Wide transformation (reduceByKey / groupBy) -> SHUFFLE
    # Create (key, value) pairs
    pairs = sc.parallelize(range(1, 1001), 6).map(lambda x: (x % 10, 1))  # keys 0..9

    pause(
        "STEP 5: We will now create a SHUFFLE using reduceByKey.\n"
        "Shuffle means data moves across partitions.\n"
        "In UI you'll see multiple stages (map stage + reduce stage)."
    )

    reduced = pairs.reduceByKey(lambda a, b: a + b)  # shuffle happens here (wide)
    print("Action 3: collect() on reduceByKey -> triggers shuffle job")
    print("reduceByKey result =", reduced.collect())

    pause(
        "STEP 6: Shuffle job completed.\n"
        "UI check:\n"
        "- Jobs -> open latest job\n"
        "- You should see 2 stages (shuffle boundary)\n"
        "- Click a stage -> look at 'Shuffle Read/Write'\n"
        "- DAG Visualization will show the shuffle"
    )

    # 6) Repartition vs Coalesce (repartition shuffles)
    base = sc.parallelize(range(1, 41), 2)

    pause(
        "STEP 7: repartition(6) will SHUFFLE.\n"
        "coalesce(2) typically avoids shuffle.\n"
        "We will compare both."
    )

    rep = base.repartition(6)   # shuffle
    print("Action 4: rep.glom().collect() to show partitions")
    print(rep.glom().collect())

    pause(
        "STEP 8: Check UI for repartition job.\n"
        "UI check:\n"
        "- Jobs -> latest -> DAG Visualization\n"
        "- Look for shuffle\n"
    )

    coal = rep.coalesce(2)  # usually no shuffle
    print("Action 5: coal.count()")
    print("count =", coal.count())

    pause(
        "STEP 9: Check UI for coalesce job.\n"
        "It should be simpler than repartition (often no shuffle).\n"
    )

    # 7) Cache/Persist (Storage tab)
    pause(
        "STEP 10: Now we cache an RDD so it appears in 'Storage' tab.\n"
        "First action computes it, second action reuses cached data."
    )

    big = sc.parallelize(range(1, 5_000_00), 8).map(lambda x: x * 2).cache()

    print("Action 6: big.count() -> fills cache")
    t0 = time.time()
    print("count =", big.count(), "| took", round(time.time() - t0, 3), "sec")

    pause(
        "STEP 11: Now check UI Storage tab.\n"
        "You should see cached RDD with memory usage.\n"
        "Next action should be faster (reused from cache)."
    )

    print("Action 7: big.sum() -> should reuse cache")
    t1 = time.time()
    print("sum =", big.sum(), "| took", round(time.time() - t1, 3), "sec")

    pause(
        "STEP 12: Check UI again.\n"
        "- Jobs: one more job\n"
        "- Storage: cached RDD still present"
    )

    # 8) Broadcast variable (for learning)
    pause(
        "STEP 13: Broadcast demo (small but important concept).\n"
        "Broadcast sends a read-only variable efficiently to executors."
    )

    lookup = {i: f"val_{i}" for i in range(10)}
    b = sc.broadcast(lookup)

    rdd_lookup = sc.parallelize(range(20), 4).map(lambda x: (x, b.value[x % 10]))
    print("Action 8: collect() on broadcast usage")
    print(rdd_lookup.collect()[:10])

    pause(
        "STEP 14: Broadcast doesn't always show as a big UI change in local mode,\n"
        "but it's a core concept for joins later."
    )

    # 9) Optional: SQL/DataFrame to see SQL tab
    pause(
        "STEP 15: DataFrame action to show SQL tab / query plan.\n"
        "This helps you teach SparkSession vs SparkContext differences."
    )

    df = spark.range(0, 1_000_00).repartition(4)
    df2 = df.where("id % 7 = 0").groupBy().count()
    print("Action 9: df2.collect()")
    print(df2.collect())

    pause(
        "FINAL STEP: Go to Spark UI and check:\n"
        "- SQL tab (if visible)\n"
        "- Jobs / Stages / DAG\n"
        "- Executors\n"
        "When done, press Enter to stop Spark."
    )

    spark.stop()
    print("Spark stopped. UI will disappear now.")


if __name__ == "__main__":
    main()
