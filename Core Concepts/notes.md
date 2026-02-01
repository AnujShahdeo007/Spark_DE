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



    

