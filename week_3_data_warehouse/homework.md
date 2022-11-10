## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)

> 1262546970

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**
Can run a distinct query on the table from question 1

> 914

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

> Partition by dropoff_datetime & cluster by dispatching_base_num.

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

> Estimated data processed: 11.72 GB
> Actual data processed: 1.76GB
> Count: 796740

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Partitioning cannot be created on all data types.

> Clustering on dispatching_base_num and SR_Flag as composite cluster key.

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

> Partitioning allows individual partitions to be managed independently.
> Clustering keeps data in sorted order. BigQuery is able to optimise by skipping ORDER BY clauses for already sorted data. BigQuery is also able to perform binary search for WHERE predicates when the data is in sorted order.

## (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

> Capacitor, Google's Internal columar format. Data is stored by columns instead of rows typical of OLTP databases.
