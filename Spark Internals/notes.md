# Notes for Spark Internals

Spark Internals
----------------
RDD - Resilient Ditributed Dataset 
-----------------------------------
    - Resilient - Fault- tolrent 

Resilient:
    - Means fault torlance.
    - If a partition (piece of data) is lost (executor crash) Spark can rebuild it using    the lineage (history of transformation)
Ditributed:
    - Means - Data is split into partition and stored/processed across multiple executors (machine/core)
Dataset:
    - A collection of records (number,rows,tuple,strings)

RDD is spark low level distributed data structure that supports parallel processing + fault tolerence.

1. Why RDD exists? ( When to use RDD )

    - We need low level control (partitons,custom logic)
    - Data is unstructured (logs,row text)
    - If you want use map/filter/reduce style like hadoop.
    - custom transformation 

    Dataframe.dataset: 
        - Dataframe/dataset is preferred for SQl- like operations (faster catalyst optimizer)
        - RDD is the foundation of eveything 

2. Internal structure of an RDD (How spark store it)

    - Partition 
        - RDD is divides into partition 
        - Each partition is processed by 1 task 
        - More partition -> More paraalissm (but too many partition also overhead)

    - Lineage
        - RDD does not store "how it was computed" as code text, bit store a logical dependency chain.
    - Dependicies 
        - Narrow Dependicies - Each partition depends on 1 partiton (No shuffle)
        - wide dependencies - partition depends on many partitions(shuffle happeds)

How to see no of partitions

    - rdd.getNumPartitions()

rdd.glom().collect() 

glom() - takes element of each partition and pack them into list 
    - output RDD[]
collect() - Bring eveything to driver 

Transformation & Action 
----------------------
Transformation 
-------------
Creates a new RDD, but does not execute immediately.

Examples: map,filer,flatmap,distinct

Action
------
Triggers execution (spark creates job/stages/tasks)

example: collect(),count,take,saveASTextFile

ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:289 []

DAG
Stages
Tasks
Shuffle
Partitioning
Catalyst optimizer
Tungsten engine






