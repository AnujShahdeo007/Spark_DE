# Notes for Core Concepts

- What is Spark?
- Why Spark?
  Problem without spark -
    - Single machine 
    - Limited CPU and RAM 
    - Slow Processing 
    - Not scalble 
        - 100 GB log process -> 1 Laptop  -> Takes hours or crashes 
 Solution with Spark - 
    - Usese Multiple Machine 
    - Split data 
    - Process in parallel 
    - Very fast 
        - 100 GB log file -> 10 Machines -> FInishes in minutes 

What kind of data does spark handle?
    Type                 Examples 
    Structure           Tables,CSV 
    Semi- structure     JSON, Parquet 
    Unstructure         Logs,Text 
    Streaming           Kafka,events 

Spark ecosystem

    Spark Application 
-------------------------------------------

- Spark SQL     - Streaming     - Mllib 
- Graph         - Structred  Streaming 
-------------------------------------------
    Spark Core 
    -----------
Cluster manager  (YARn/k8s)

Cluster vs Local mode

- Is spark local mode distributed?
- Spark local mode uses spark API's but does not provide true distribution across machines. it runs on single node.

- Can spark run without a cluster?
- Yes, Spark can wun in local mode for developemnt and testing, but production workloads require cluster mode.

- Main spark APIs?
    1. RDD API
    2. Dataframe 
    3. Dataset 
    4. Spark SQL 

Spark architecture

        You submit spark job 
                |
                |
                |
              Driver 
                |
                |
            Cluster Manager
                |
                |
             Executors 


Driver : 

1. What is Driver?
    - The driver is the main program that: 
        - Runs your spark code.
        - Creates SparkContext/SparkSession 
        - Builds Execution plan 
        - Coordinates everything 
    - What driver does internally?
        - Reads your code 
        - Creates DAG (Directed Acylic Graph)
        - Split DAG -> Stages 
        - Split Stages-> Tasks 
        - Send Tasks -> Executor

        If Driver Dies ---> Job Fail 

Executors:

2. What are execuotrs?
    - Execute task 
    - Process data 
    - Store data in Memory 
    - Send the result to Driver back

    - One Spark application - Many Executors 
    - Executors stay alive for entire job
    - Each Executors:
        - CPU cores 
        - Memory 

    Executors:
        - Task 1 
        - Task 2 
        - Task 3 

    One core = One task at a time 

3. Cluster manager: 

What is CM ?

- Cluster manager Allocates Machiens, CPU, Memory to spark 
Spark supports:
    - YARN 
    - Kuberntes 
    - Standalone 
What CM does:
    - Start execuotrs 
    - Monitor Health 
    - Restart Failed executors 


Job 
    - Stages 
        - Task 1
        - Task 2 
    - Stages 
        - Task 3
        - Task 4


Distributed computing: 

- One big problem is divided into smaller problems. 
- Multiple Machines solve them togather 

    - Many machines working in parallel 

1. Why do we need Distruted Computing?

    - Problem with single machine:
        - Limited CPU 
        - Limited RAM 
        - Disk Boottleneck
        - Crashes on big data 
        - 1 TB -> 1 Laptop -> Too slow or crashes 

    - Distributed Solution 
        - 1 TB -> 10 Machine -> Each Processes 100 GB --> Fast + Scalable 

2. Parallel and Distributed Computing 

    Parallel Computing 
    ------------------
        - Multiple CPUs 
        - Same Machine 
    Distributed Computing 
    ---------------------
        - Multiple machine 
        - Network Involved 


SparkSession & SparkContext
-----------------------------
    
What is SparkContext
---------------------
SparkContext is a gateway to the spark cluster.

    - Connect your program to cluster manager 
    - Allocates executotrs 
    - Taslk to workers 
    - Mange RDD execution

    Without SparkContext ---> Spark cannot run anything 


    Responsibilty             Explanation 
    Cluster connection        Connect to YARN/standalone/kubernetes 

    Excutor allocation        Request CPU & Memory 

    Task schdulling           Decide which executor runs which task 

    RDD                       Creates,track and execute RDD 

    Fault tolenrece           Recomputes lost partitions 

Problems with this approch:

    - NO SQL support 
    - No Dataframe API 
    - No streaming/ML 

Key sparkcontext Objects 
-----------------------
    - sc.parallelize()      Create RDD 
    - sc.textFile()         Read file as RDD
    - sc.broadcast()        Broadcast Variable 
    - sc.accumulator()      Shared Counters 
    - sc.stop()             Stop spark      
 



Spark optimization techniques :
    1. - Read less data 
        - Read only required columns (Column Pruning)
        - Filter Early (Predicate pushdown)
        - Use correct file formate 
        - Partitioned data + Filter on partition data 

df= (spark.read.parquet(S3://bucket/sales.parqut")
    . select ("dt","country","amount") # column pruning
    . filter("dt>=2026-01-01)) # Filter early 

    2. Avoid Shuffle (Shuffle expensive)
        - Shuffle happens in :
            - groupBy,distinct,join,orderby,repartition 
        - Prefer reduceBykey style/ Pre-aggregation
        - Use broadcast join when one table is small 
        - Use partitionBy on write for future queries 
        - Avoid unnessary repartition() ( it forces shuffle)

    3. Partition tuning (Paralleism)
        - Too few partitons --> slow (not enough parallism)
        - too many partition --> (Overhead ( too many small task))

    4. Cache only when it will be reused
        - Cache helps when- 
            - Same DF used multiple times (2+actions)
    5. AQE (Dapative query execution)
        - AQE can automatically:
            - Switch tobroadcast join
            - Coalese shuffle partitions 
            - Handle skew joins 

    

Airflow failure scenario 
    Common failure senarios ;

    - upstream data not arrived (Most Common)
        EX: S3 file not present, Hive table partition missing 
    - Task fails dure transient issues 
        - Network glitch 
        - Temporary DB lock 
        - API timeout 

        FIX: Retries + expontial backoff 

    3. Duplicate loads 
        - Task partially succeeded, Failed 
        - Retry loads same data again - Duplicaate 

        FIX: Make pipeline idempotent 
            - Write to a temp path then rename 
            - Use overwirite by partition 
            - Watermark 
        
    4. DAG stuck due to resource limits 
            - too many parallel task 
            - executor/memory storage 




Hive sql to pyspark step migration 

    Step 1 : Identify SQL pattern 
        Common Hive SQl pattern :
            - SELECT + WHERE 
            - JOIN
            - GROUP By
            - Windown fn 
            - INSERT OVERWRITE PARTITION 

        Step 2: Convert each part into Dataframe API 

            HIVE SQL 
            --------
            INSERT OVERWRITE TABLE sales PARTITION (dt)
            SELECT dt,country.SUM(amount) as total_sum
            FROM sales 
            Where dt>= ""
            GROUP By dt,country


            PYSPARK 
            --------
            
            from pyspark.sql.functions import sum as _sum 
            sales= spark.table("sales")
            df_summary= (sales .filter(dt >= )
                         .group by ("dt,"country"))


        Step 3: validate result 

            - row count 
            - distinct keys 
            - aggregates (sum,count)

Data skew issue

What is skew?
     When one key has huge records, one partition becomes massive - One executor runs forever while others finish 

     EX:

     - country ="INDIA" ----> has 80% rows 
     - join/group by on country ---> One partition overloaded 

     FIX 1:
        - Brodcaste join 
        - Salting 
        - AQE skew join handling 



Write code in filter data overwrite to parquet

    CASE A: Overwrite full folder 
         df= spark.read.parquet(s3 path)
         filtered= df.filter("status=ACTIVE")

         filteres.write.mode("overwrite").parquet(s3 path)
    CASE B : Overwrite only affected data 
        If your data is partitined by dt, overwrite only those partition 

                 df= spark.read.parquet(s3 path)
         filtered= df.filter("status=ACTIVE")

         filteres.write.mode("overwrite").formate("parquet").partitionBy("dt").save(s3 path)

   
    

9:45 - 

5:30 - 