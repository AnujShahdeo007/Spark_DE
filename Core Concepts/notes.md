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

  -----------------------------------------------------------------------------------------------------------------------------------
  RDD Fundamental 
  ---------------

  1. What is RDD? (Resilient Disrtributed system) 

    - Resilient : Fault tolerent 
        - If one machine crashes --> Spark can rebuild data using lineage.
    
    Example:
            Excel file lost -> gone forever 
            RDD lost -> Rebuild from history 

    - Distributed: Data is split across multiple executors 

        - Dataset - 1 million records 
            - Partition 1-> Executor A
            - Partition 2-> Executor B 
            - Partition 3-> Executor C 
    - Dataset 
        Colelction of data 
            - list 
            - file
            - databses 
            - log records 
2. Why RDD was created?
    - Before Dataframe existed.
        - Spark worked using RDD only 

    RDD gives:
        - Full control 
        - Functional Programming style 
        - Low-level transformation 

IMP: Creating RDDs 

1. parallelize() 
    rdd=sc.paralleize([10,20,30,40])

2. textFile() 
    rdd=sc.textFile("data.txt")

- Transformation:
    - Transformation -> Return a New RDD 
    - Spark builds a DAG 
    - Does not execute immediatly 

Narrow Transformation 
--------------------
    - Each output partition depends on only input partition.
    - No shuffle (No network data transfer)
    - Fast 

    1. map(lambda x:x*2) - Applies function to each element (Narrow Transformation) and return new RDD.

    2. flatMap() - 1 input -> multiple output (multiple output for each element)
        - flatmap() takes one input and can return multiple output elements and then flatten them into single list.
            - If does two things 
                - 1. Applies a function 
                - 2. Flatten the result 

        rdd=sc.paralleize([1,2,3])
        result=rdd.flatMap(lambda x:[x,x*10])
        output: [1,10,2,20,3,30]
    
        rdd=sc.parallelize("hello   Spark","Big     Data")
        result=rdd.map(lambda x:x.split(""))
        result=rdd.flatMap(lambda x:[w for w in x.split(" ") if w != ""])
        print(result.collect())


        rdd=sc.parallelize(["Hello world","spark rdd"]) -> ["Hello", "world","spark", "rdd"]
        result=rdd.flatMap(lambda x:x.split(" "))
    3. filter() - Keep only matching elements 
        filter() is a transformation used to keep only those elements that satisfy a condition.

        syntax:
            rdd.filter(lambda x : condition)

            nums=sc.parallelize([10,20,30,40,50])
            result=nums.filter(lambda x:x%2==0)
            print(result.collect())

            logs=sc.parallelize([
                "INFO job started",
                "ERROR connection failed",
                "INFO completed"
            ])

            errors=logs.filter(lambda x:"ERROR" in x)
            print(errors.collect())

rdd = sc.parallelize([
    "user1,login,success",
    "user2,logout,failed",
    "",
    "user3,login,sucess",
    "    ",
    "use4,login,failed"
])

1. Remove empty rows 
2. Split each row into column 
3. Keep only login events 
4. Convert usernames to upercase 

OUTPUT: ['USER1','USER3','USER4']

rdd = sc.parallelize([
    "user1,login,success",
    "user2,logout,failed"])


['user1,login','success','user2,logout','failed']




            
            nums=sc.parallelize([1,2,3,4,5,6,7,8,9])
            result=nums.filter(lambda x:x>25)
            print(result.collect())


        rdd=sc.paralleize([10,20,30,40,50])
        result=rdd.filter(lambda x:x>20)
    4. mapPartition()
    5. mapPartitionsWithIndex() 
    7. union()
    

Wide transformation
--------------------
    - Output partition may depends on many input partitions 
    - Shuffle happens ( network transfer + disk spill)
    - Slower ( Grouping/joining/distinct/repartition)

    6. distinct()
    8. Intersection()
    9. subtract()
    10.cartesian()
    11. reduceBykey()
    12. groupBykey() 
    13. sortBykey()
    14. join()
    15. combineByKey()
    16. aggregateByKey()
    17. PartitionBy()
    

    18. coalsece()
    19. repartition()



    



