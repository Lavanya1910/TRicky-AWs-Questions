DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Scenario-Based Tricky Questions — AWS Data Engineering Services 
Studio
These questions are intentionally written to create confusion at first read. Read the question carefully, think about it, then read the answer. That gap between confusion and clarity is where real learning happens. 
Table of Contents 
# 
Topic
Q1 
ect
Redshift cluster is running but queries are getting slower every day 
Q2 
t
Glue job succeeds but the data in S3 is wrong — or is it? 
Q3 
i
You added more nodes to EMR but the job got slower 
Q4 
Redshift Spectrum vs Redshift — which one is "faster"?
Q5 ch
Your Spot Instance was terminated and your Glue job failed — whose fault is it?
Q6 r
Reserved Instance vs Savings Plan — you paid upfront but still got a huge bill
Q7 A
TooManyRequestsException on DynamoDB — you are only doing 100 reads per second
Q8 
Your Glue job runs fine in dev but throttles in production
Q9 ta
Redshift node went down — your queries are still running. How?
Q10 a
On-Demand vs Spot vs Reserved — which one should you use right now?
Q11  D
You increased Glue DPUs from 5 to 50 but the job did not get 10x faster
Q12 
Redshift COPY command is taking 3 hours for 500 GB — what is wrong?
Q13 
Your EMR cluster auto-scaled but costs tripled — you did nothing wrong
Q14 
Kinesis is dropping records even though you have enough shards
Q15 
You enabled Redshift Auto Vacuum but table scans are still slow



©
Page 1 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
© DataArchitectStudio
Q1 — Redshift Cluster is Running But Queries Are Getting Slower Every Day 
The Question 
Your Redshift cluster has 4 ra3.4xlarge nodes. No new data is being loaded. No new queries were added. Your team did not change anything. But every day for the past 2 weeks, query performance has been degrading — what you used to run in 30 seconds now takes 4 minutes. 
You might think: Hardware is degrading? Network issue? Someone running heavy queries? All wrong. 
The Answer 
The real culprit is table bloat and unsorted/unskewed data caused by missing VACUUM and ANALYZE operations. 
Here is what happens under the hood: 
1. DELETE and UPDATE operations do not physically remove rows in Redshift. 
When you delete or update rows, Redshift marks those rows as "tombstoned" (soft delete) but the disk space is not freed. These dead rows still get scanned during every query. Over time, as more updates and deletes accumulate, every scan reads more dead rows than live ones. 
2. SORT KEY order degrades over time. 
Redshift stores data in sorted blocks on disk. When new rows arrive, they may not be inserted in sort key order. Over time, the "percent of unsorted rows" climbs. When it is high, Redshift cannot use zone maps (block-level min/max statistics) to skip blocks, so it scans everything. 
3. Statistics go stale. 
Redshift's query planner uses table statistics to choose the best execution plan. If statistics are not refreshed (via ANALYZE), the planner makes wrong decisions — like choosing a full table scan instead of a more targeted approach. 
Root Cause Summary: 
• Dead rows from DELETE/UPDATE piling up → more data scanned 
• Unsorted rows → zone maps become useless → no block skipping 
• Stale statistics → bad query plans 
Page 2 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Fix:
```sql
-- Step 1: Reclaim space from dead rows and re-sort data
VACUUM FULL your_schema.your_table;

-- Step 2: Refresh statistics so the query planner makes smart decisions
ANALYZE your_schema.your_table;

-- Step 3: Check which tables need vacuum most urgently
SELECT
    "table",
    unsorted,
    stats_off,
    tbl_rows,
    estimated_visible_rows
FROM svv_table_info
WHERE unsorted > 10 OR stats_off > 10
ORDER BY unsorted DESC;
``` 
Production Tip: 
At production scale (hundreds of millions of rows), running VACUUM FULL on a busy table blocks writes. Use VACUUM SORT ONLY first (faster, no locking), then VACUUM DELETE ONLY during a maintenance window. 
Why it caught people off guard: 
Nobody changed anything. The degradation was gradual. The cluster looked healthy in CloudWatch — CPU normal, connections normal. The problem was entirely inside the data blocks, invisible without querying svv_table_info. 
Key Low-Level Details 
• Redshift uses columnar storage — each column is stored as a series of 1 MB blocks 
• Each block has a zone map — min and max value stored in metadata 
• If sort order is intact, Redshift reads zone maps and skips entire blocks that do not match your WHERE clause 
• If sort order is broken, zone maps are useless — Redshift scans every block 
• A table with 10% unsorted rows can still have terrible performance if those unsorted rows are spread across every block 
Q2 — Glue Job Succeeds But the Data in S3 is Wrong — Or Is It? 
Page 3 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
The Question 
Your AWS Glue ETL job runs successfully. Green checkmark. No errors in CloudWatch. But when your analyst 
queries the Athena table on top of the S3 output, they see duplicate rows — sometimes 2x, sometimes 3x the © DataArchitectStudio
expected count. 
You might think: Bug in the transformation logic? Glue wrote duplicates? Wrong. The job is actually correct. 
The Answer 
The problem is almost certainly Glue job bookmarks not being configured correctly combined with S3 output not being cleared before rerun — causing multiple job runs to write overlapping data to the same S3 prefix. 
Here is what actually happens: 
Scenario A — No Job Bookmark, Manual Rerun: 
Glue reads from the source (say, an RDS table or S3 input folder). On the first run, it writes 10 million rows to s3://bucket/output/. Someone manually reruns the job (maybe they thought it failed, or they changed a config). The job reads the same source again and writes another 10 million rows to the same output path — but S3 does not delete old files automatically. Now the Athena table sees 20 million rows. 
Scenario B — Job Bookmark Misconfigured: 
Glue Job Bookmarks track which S3 files or JDBC offsets have already been processed. If bookmarks are enabled but the output is an overwrite target (not an append target), you get a mismatch — Glue skips the source data correctly, but the previous output files still exist, leading Athena to read stale data alongside new data. 
Scenario C — Glue Dynamic Frames and Partial Writes: 
If a Glue job crashes mid-write and is retried, partial Parquet files may already exist in S3. On retry, new files are written alongside the partial ones. Athena reads all files in the prefix — including the incomplete ones. 
Fix:
```python
# Option 1: Always clear the output prefix before writing
import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('your-bucket')
bucket.objects.filter(Prefix='output/your_table/').delete()

# Option 2: Use partitioned output with overwrite mode per partition
datasink = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://your-bucket/output/your_table/",
        "partitionKeys": ["year", "month", "day"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

# Option 3: Enable Job Bookmarks correctly
# In AWS Console → Glue Job → Edit → Job Bookmark = Enable
# This ensures the job only processes NEW files since last successful run
``` 
Why it catches people off guard: 
The Glue job shows SUCCESS. No error. The problem is entirely in what was already in S3 from a previous run. Analysts see wrong counts and immediately blame the transformation logic — which is actually fine. 
Key Low-Level Details 
• S3 is a key-value store — it does not have a concept of "overwrite a folder." Writing to the same prefix adds new objects alongside old ones 
• Athena reads ALL objects under a prefix when you query a table — it has no way to know which files are "current" 
• Glue Job Bookmarks work by storing a checkpoint of the last successfully processed partition or file in AWS metadata — they work well for append scenarios but not for full-refresh scenarios 
• For full-refresh pipelines, the safest pattern is: delete output prefix → run Glue job → verify row count 
Q3 — You Added More Nodes to EMR But the Job Got Slower 
The Question 
You have an EMR Spark cluster with 5 core nodes (r5.2xlarge). Your job processes 2 TB of data and takes 45 minutes. You scale up to 15 core nodes expecting it to run 3x faster. Instead, it now takes 55 minutes. You literally made it slower by adding more machines. 
You might think: AWS bug? Wrong instance type? No. This is a fundamental Spark behavior that surprises almost everyone. 
The Answer 
Page 5 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
The problem is data skew — one or more partitions of your data are dramatically larger than others. Adding more nodes gave Spark more workers, but the skewed partition still ends up on one worker. That one worker takes 50 minutes. All other 14 workers finish in 5 minutes and sit idle waiting. The job cannot complete until the last task finishes. 
© DataArchitectStudio
Additionally, adding more nodes increases shuffle overhead. Shuffle is when Spark redistributes data across nodes (during joins, groupBy, etc.). With 15 nodes, there are more network transfers required to move data between nodes during shuffle — and if your job is shuffle-heavy, this extra coordination cost outweighs the parallelism gain. 
How to detect skew: 
In Spark UI, look at the Stages tab. If most tasks in a stage finish quickly but 1-2 tasks take significantly longer, that is skew. The "Max" task duration will be far above the "Median." 
```pyspark
# Check partition sizes to detect skew
from pyspark.sql.functions import spark_partition_id, count

df.groupBy(spark_partition_id()).agg(count("*").alias("row_count")) \
    .orderBy("row_count", ascending=False) \
    .show(20)

# Fix Option 1: Repartition evenly
df_balanced = df.repartition(200)

# Fix Option 2: Salting — for skewed JOIN keys
# If joining on 'customer_id' and one customer_id has 80% of rows:
from pyspark.sql.functions import concat, lit, floor, rand

# Add a random salt to the skewed key in the large table
df_large = df_large.withColumn(
    "salted_key",
    concat(df_large["customer_id"], lit("_"), (floor(rand() * 10)).cast("string"))
)

# Explode the small table to match all salt values
from pyspark.sql.functions import explode, array

df_small = df_small.withColumn("salt", explode(array([lit(str(i)) for i in range(10)])))
df_small = df_small.withColumn(
    "salted_key",
    concat(df_small["customer_id"], lit("_"), df_small["salt"])
)

# Now join on salted_key
df_result = df_large.join(df_small, "salted_key")

# Fix Option 3: Increase shuffle partitions for large datasets
spark.conf.set("spark.sql.shuffle.partitions", "800")
``` 
Page 6 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
# Default is 200 — too low for TB-scale data 
Why it catches people off guard: 
The instinct is always "more resources = faster." That is true only if work is distributed evenly. Skew makes scaling © DataArchitectStudio
useless or counterproductive. This is one of the most common production EMR performance issues. 
Key Low-Level Details 
• Spark divides data into partitions — each partition is processed by one task on one executor 
• Default partition count after shuffle is controlled by spark.sql.shuffle.partitions (default: 200) • A skewed partition means one task does 80% of the work while others do 1-2% each 
• Optimal partition size is typically 128 MB to 256 MB per partition 
• For 2 TB of data: 2000 GB / 200 MB = ~10,000 partitions is a reasonable target 
• Shuffle write amplification: with 15 nodes, each shuffle produces 15 × 15 = 225 shuffle files per stage 
Q4 — Redshift Spectrum vs Redshift Tables — Which One is "Faster"? 
The Question 
Your manager asks: "We have 10 TB of historical data in S3 and 500 GB of recent data in Redshift. For a report that joins both, should we move all 10 TB into Redshift or use Redshift Spectrum to query S3 directly? Spectrum is slower than native Redshift tables, right? So we should move everything in?" 
The trap: "Spectrum is slower" is not always true. And "move everything in" is not always the right answer. 
The Answer 
Redshift Spectrum is NOT always slower than native Redshift. The answer depends entirely on the query pattern, data format, partitioning, and how much data is being scanned. 
When Spectrum is slower than native Redshift: 
• Querying non-partitioned data in S3 — Spectrum has to scan every file 
• Small, frequent queries — the overhead of invoking Spectrum's external processing layer (thousands of EC2 nodes in AWS's infrastructure) adds latency of 1-3 seconds even for tiny scans 
• Uncompressed or row-based formats (CSV, JSON) — Spectrum reads the entire file even for a single column query 
Page 7 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
When Spectrum is faster than or equal to native Redshift: 
• Columnar, compressed formats (Parquet, ORC) — Spectrum reads only the columns needed 
• Well-partitioned data — WHERE date = '2024-01-01' on a date-partitioned S3 prefix means Spectrum only reads that day's files 
compute. But Spectrum's massive parallelism and columnar pruning on well-formatted data often compensates — © DataArchitectStudio especially for large historical scans where a small Redshift cluster would struggle.
• Large historical cold data — you are not paying for Redshift node storage for 10 TB that is rarely queried • Spectrum uses thousands of parallel nodes owned by AWS — for very large scans it can outperform a modest Redshift cluster 
The actual recommendation for this scenario: 
Keep the 10 TB in S3 as Parquet, partitioned by date. Keep the 500 GB hot data in Redshift native tables. Use Spectrum for the join — the query crosses both. This is the standard hot-cold architecture and it is cost-optimal. 
```sql
-- Create external schema pointing to S3 (Glue Data Catalog)
CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'your_glue_database'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Query joining native Redshift table with Spectrum S3 table
SELECT
    r.customer_id,
    r.recent_purchase_amount,
    s.historical_lifetime_value
FROM
    public.recent_orders r -- Native Redshift table
JOIN spectrum_schema.historical_orders s -- S3 via Spectrum
ON r.customer_id = s.customer_id
WHERE
    s.order_year >= 2020 -- Partition pruning
AND r.order_date >= CURRENT_DATE - 30;
``` 
Cost comparison: 
• Storing 10 TB in Redshift ra3 nodes: ~$1,600/month 
• Storing 10 TB in S3 Standard: ~$230/month 
• Spectrum query cost: $5 per TB scanned — with Parquet and good partitioning, you might scan only 50 GB for a typical report = $0.25 per query 
Why it catches people off guard: 
People assume "native = always faster." Native tables have sorted blocks, zone maps, and are co-located with 
Page 8 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Q5 — Your Spot Instance Was Terminated and Your Glue Job Failed — Whose Fault Is It? 
The Question 
© DataArchitectStudio
You ran a Glue ETL job using a Spot Instance worker type (G.1X workers with Spot pricing). The job was 70% complete. AWS terminated the Spot Instance. Your job failed, and you lost 3 hours of work. Your colleague says "You should have used On-Demand." Is your colleague right? 
The trap: The termination was expected. The failure was not. They are two different problems. 
The Answer 
Your colleague is partially right but misses the real issue. Spot Instance termination is normal and expected — AWS can reclaim Spot capacity with a 2-minute warning. The real problem is that your Glue job was not designed to handle it. 
What should have happened: 
Glue supports Job Bookmarks — a checkpointing mechanism. If your job reads from S3 and bookmarks are enabled, Glue records which input files have been successfully processed. On retry, it skips already-processed files and only processes what remains. The job resumes from roughly where it left off — not from zero. 
Additionally, Spark (which Glue uses under the hood) supports checkpointing — saving the state of a streaming or batch computation to S3 at regular intervals. If a node is lost, Spark can recover from the last checkpoint rather than restarting the entire stage. 
What you should do: 
```python
# 1. Enable Job Bookmarks — in Glue Console or via boto3
# This handles input file tracking across retries

# 2. Enable Spark checkpointing for long-running jobs
spark.sparkContext.setCheckpointDir("s3://your-bucket/checkpoints/")

# 3. Use Glue's built-in retry with max retries
# In Glue Job settings: Max Retries = 2, Timeout = 480 minutes

# 4. Save intermediate results to S3 at logical stages
# Instead of one giant job, break into stages:
# Stage 1: Raw → Cleaned (save to S3)
# Stage 2: Cleaned → Aggregated (save to S3)
# Stage 3: Aggregated → Final Output
# If Stage 2 fails, retry only Stage 2

# 5. For cost-aware design — mix On-Demand and Spot
# Use 1 On-Demand driver + Spot workers
# If workers are terminated, the driver survives and can retry tasks
``` 
When to use Spot vs On-Demand for Glue: 
© DataArchitectStudio
• Spot (G.1X or G.2X Flex): Best for jobs that are idempotent (safe to rerun), have checkpointing enabled, and run on a flexible schedule 
• On-Demand: Best for time-sensitive jobs (before a business report deadline), jobs without checkpointing, or critical production pipelines where failure cost exceeds the Spot discount 
Cost reality: 
Spot workers cost about 30-70% less than On-Demand Glue workers. For a job that runs 4 hours daily, that saving is significant. But if termination causes a full restart of a 3-hour job, you may end up paying more total than just using On-Demand. 
Why it catches people off guard: 
Most people think "Spot = risky, On-Demand = safe." The real lesson is: design for failure. With proper checkpointing and bookmarks, Spot termination becomes a minor inconvenience, not a disaster. 
Q6 — Reserved Instance vs Savings Plan — You Paid Upfront But Still Got a Huge Bill 
The Question 
Your company bought 3-year Reserved Instances for 10 r5.2xlarge instances, paying the full upfront amount. Six months later, your data team migrated from EC2-based processing to AWS Glue (serverless). The EC2 instances are now sitting idle. Your AWS bill is still the same as before. You already paid — why are you still being charged? 
You might think: Reserved Instances should cover your costs. They do — just not for what you are using now. 
The Answer 
Reserved Instances are a billing discount applied to running EC2 instances that match the reservation's specifications. They are not a "turn off billing" mechanism. Here is the critical misunderstanding: 
• You paid upfront for 10 r5.2xlarge Reserved Instances 
• Reserved Instances apply a discount to the running cost of matching EC2 instances 
• If those instances are idle (stopped), AWS does not charge the running rate — but you already paid the upfront cost, which is non-refundable 
Page 10 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
• Your Glue jobs run on AWS-managed serverless infrastructure — they are not EC2 instances — so your Reserved Instance discount does not apply to them 
• You are now paying for Glue (per DPU-hour) on top of having already paid for unused Reserved Instances 
What you should have used instead: 
rchitectStudio 
Compute Savings Plans are the modern, more flexible alternative: 
• Cover EC2, Fargate, and Lambda 
• Apply automatically to any instance family, size, OS, or region 
• Do not lock you to a specific instance type 
But even Compute Savings Plans do not cover Glue — Glue has its own separate pricing model (DPU-hours). Mitigation options: 
1. Sell unused Reserved Instances on the AWS Reserved Instance Marketplace 
— Standard RIs (not Convertible) can be listed for sale 
— Convertible RIs cannot be sold but can be exchanged for different instance types 
2. Convert to Convertible Reserved Instances 
— Exchange for different instance families (e.g., r5 → m5) 
— Use them for other workloads in your account (RDS, other EC2 uses) 
3. Apply Reserved Instances to other teams' EC2 usage 
— RI discounts apply account-wide (or across an AWS Organization) 
— If another team in your org is running r5.2xlarge instances, your RIs 
automatically discount their usage 
Reserved Instance vs Savings Plan — Quick Decision Guide: 
Factor A
Reserved Instance 
Compute Savings Plan
Commitment ta
1 or 3 years 
1 or 3 years
Flexibility 
Locked to instance type/region 
Any EC2, Fargate, Lambda
Discount depth a
Up to 72% vs On-Demand 
Up to 66% vs On-Demand
Can be sold 
Standard RIs: Yes 
No
Covers Glue  D
No 
No
Best for 
Known, stable EC2 workloads 
Mixed or evolving workloads



discount — not for the service itself. If you stop using matching instances, the discount has nothing to apply to and ©
Why it catches people off guard: 
Reserved Instances feel like prepaying for a service and then not being charged. In reality, you prepaid for a the upfront cost is simply lost.
Page 11 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Q7 — TooManyRequestsException on DynamoDB — You Are © DataArchitectStudio
Only Doing 100 Reads Per Second 
The Question 
Your DynamoDB table is provisioned with 1,000 Read Capacity Units (RCUs). 1 RCU = 1 strongly consistent read per second for items up to 4 KB. So you should be able to do 1,000 reads per second. Your application is only doing 100 reads per second. But you are seeing ProvisionedThroughputExceededException errors. How? 
You might think: 100 < 1,000, so you are well within limits. Wrong — and the reason is not what you expect. 
The Answer 
The problem is partition-level throughput limits, not table-level limits. 
DynamoDB distributes data across internal partitions based on the partition key. Each partition has its own throughput limit — approximately 3,000 RCUs per partition. But the allocated table capacity (1,000 RCUs) is split evenly across all partitions. 
If your table has 10 partitions, each partition gets 1,000 / 10 = 100 RCUs. Now if 90 out of your 100 reads per second all happen to hit the same partition (because of a "hot partition" — a partition key that most requests use), that single partition receives 90 reads/second against its limit of 100 reads/second. You are at 90% of that partition's capacity while the table as a whole is at 9% utilization. Small spikes will immediately cause throttling. 
Detecting hot partitions: 
In CloudWatch, check: 
- ConsumedReadCapacityUnits — if close to provisioned, overall table is hot 
- ThrottledRequests — requests being throttled 
- SystemErrors — internal errors 
In DynamoDB contributor insights (enable separately): 
- Shows top partition keys consuming the most throughput 
- Identifies which specific partition keys are causing hot spots 
Common causes of hot partitions: 
• Using a low-cardinality partition key (e.g., status = 'active' — most items are active, so that partition is hammered) 
• Using a date as a partition key (e.g., date = '2024-01-07' — today's data gets all writes) 
• A single popular item (celebrity product, trending content) receiving disproportionate reads 
Fixes: 
Page 12 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
```python
# Fix 1: Use Write Sharding — add a random suffix to partition key
import random

def write_item(user_id, data):
    shard = random.randint(0, 9)  # 10 shards
    sharded_key = f"{user_id}_{shard}"
    table.put_item(Item={"pk": sharded_key, **data})

# Fix 2: Enable DynamoDB Adaptive Capacity (automatic, on by default)
# DynamoDB automatically redistributes capacity to hot partitions
# But this only helps up to the table's total provisioned throughput

# Fix 3: Switch to On-Demand mode if traffic is unpredictable
# table = dynamodb.create_table(
#     BillingMode='PAY_PER_REQUEST'  # No capacity planning needed
# )

# Fix 4: Add DAX (DynamoDB Accelerator) for read-heavy hot keys
# DAX is an in-memory cache in front of DynamoDB
# Reads hit DAX first — if cached, DynamoDB is never reached
# Read latency drops from milliseconds to microseconds
``` 
Why it catches people off guard: 
The math looks perfectly fine at the table level. 100 reads vs 1,000 RCU capacity — should be fine. But DynamoDB is not one big table, it is a collection of partitions. Uneven access patterns create hot spots that violate per-partition limits while the table-level utilization looks healthy. 
Q8 — Your Glue Job Runs Fine in Dev But Throttles in 
Production 
The Question 
Your Glue job runs perfectly in the dev environment — processes 10 GB in 8 minutes. You deploy it to production with the same code, same Glue version, same number of DPUs. In production, the job takes 45 minutes and CloudWatch shows ThrottlingException errors on the Glue API. You did not change a single line of code. 
You might think: It is a code bug triggered by production data. Or a network issue. Neither is correct.
Page 13 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
The Answer 
The problem is AWS Glue API rate limits being hit because production runs multiple Glue jobs concurrently, while dev runs jobs one at a time. 
© DataArchitectStudio
When a Glue job runs, it makes API calls to various AWS services — most commonly to the Glue Data Catalog (to fetch table schemas, partition metadata) and to S3 (to list objects, read data). These API calls have rate limits: 
• Glue Data Catalog: GetPartitions API — 75 requests per second per account, with burst up to 1,500 • S3 ListObjectsV2: 5,500 requests per second per prefix per account (shared across all S3 activity) 
• Glue GetTable: 5 requests per second (low limit!) 
In production, you likely have 10-20 Glue jobs running simultaneously. Each job fetches partition metadata at startup. If your table has 10,000 partitions across a year of daily data, each job calls GetPartitions hundreds of times. 10 concurrent jobs × hundreds of calls each = thousands of API calls per second, far exceeding the limit. 
Fix 1 — Push Down Predicates to reduce partition fetches:
```python
# Without push down — Glue fetches ALL partitions, then filters
df = glueContext.create_dynamic_frame.from_catalog(
    database="your_db",
    table_name="your_table"
)

# WITH push down — Glue only fetches matching partitions
df = glueContext.create_dynamic_frame.from_catalog(
    database="your_db",
    table_name="your_table",
    push_down_predicate="(year == '2024' and month == '01')"  # Only fetch Jan 2024 partitions
)
# This reduces GetPartitions API calls by 90%+ for date-filtered jobs
``` 
Fix 2 — Use boto3 retry configuration:
```python
import boto3
from botocore.config import Config

config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'adaptive'  # Adaptive mode uses exponential backoff
    }
)
glue_client = boto3.client('glue', config=config)
``` 
Fix 3 — Stagger job start times: 
In production, if 10 jobs all start at 00:00:00, they all hit the catalog simultaneously. Stagger them: Job1 at 00:00:00, Job2 at 00:02:00, Job3 at 00:04:00 
This simple change eliminates 80% of throttling with no code changes. 
Page 14 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Why it catches people off guard: 
Same code, same DPUs, different environment — "it must be a data problem." But the issue has nothing to do with the data. It is a concurrency problem invisible in dev (one job at a time) and very visible in production (many jobs at © DataArchitectStudio
a time). 
Q9 — A Redshift Node Went Down — Your Queries Are Still Running. How? 
The Question 
Your Redshift cluster has 8 ra3.4xlarge compute nodes. AWS notified you that one node experienced a hardware failure. You expected queries to fail or the cluster to go offline. But users are still running queries successfully. Is this a glitch in the notification? Or are your queries actually running on a broken node? 
You might think: Either the notification was wrong, or queries should be failing. Both assumptions are wrong. 
The Answer 
Redshift automatically handles single node failures through its built-in fault tolerance — and the mechanism depends on whether you are using Multi-AZ or Single-AZ deployment. 
How Redshift stores data (key to understanding this): 
Redshift slices data across all compute nodes. Each node has multiple slices (virtual sub-units of processing). Data blocks are distributed across slices using the distribution style (EVEN, KEY, or ALL). Crucially, Redshift also maintains mirror copies of data blocks. Each node holds a mirror of another node's data. 
When a node fails: 
– AWS detects the failure automatically (usually within 60 seconds) 
– A replacement node is provisioned from AWS's infrastructure pool 
– While the replacement is being set up, other nodes serve queries using the mirror data — the data that failed node held is also on another node as a mirror 
– The cluster continues running in a degraded state (slightly slower, as surviving nodes pick up extra work) – The replacement node is added and data is redistributed — this happens in the background without taking the cluster offline 
For Redshift Multi-AZ (available on ra3 node types): 
• Active and standby clusters run in different Availability Zones 
Page 15 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
• If the active cluster fails entirely, the standby takes over within 60-120 seconds 
• Queries in flight at the moment of failover are lost (clients need to retry), but the cluster itself does not go offline 
What you WILL notice during a node failure: 
Who is right? Is there a correct answer? © DataArchitectStudio 
• Some queries may run slightly slower (surviving nodes handle extra data) 
• The Redshift console will show a node in "impaired" state 
• stv_nodes system table will show the unhealthy node 
```sql
-- Check node health in Redshift
SELECT node, used_mb, capacity_mb, elapsed_ms
FROM stv_nodes
ORDER BY node;

-- Check if any queries were affected
SELECT pid, query, starttime, endtime, aborted
FROM stl_query
WHERE aborted = 1
AND starttime > GETDATE() - INTERVAL '1 hour'
ORDER BY starttime DESC;
``` 
Why it catches people off guard: 
Most databases (especially single-node ones) go down when the server has a hardware failure. Redshift's distributed, mirrored architecture is designed to treat node failure as a routine event — not an emergency. The cluster is designed to survive it silently. 
Q10 — On-Demand vs Spot vs Reserved — Which One Should You Use Right Now? 
The Question 
You are starting a new data pipeline project. Your manager asks you to choose the cheapest instance pricing model for EMR. You say "Spot — it's 70% cheaper!" Your manager says "But what if it gets terminated?" You say "Reserved then — we'll commit for 3 years." Your manager says "But we don't know if this pipeline will still exist in 3 years." 
The trap: You are both partially right. The correct answer depends on a framework, not a single rule.
Page 16 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
The Answer 
All three options are correct in different scenarios. Here is the decision framework: 
© DataArchitectStudio
On-Demand: 
• No commitment, full price, no interruption risk 
• Use when: Job must complete on time (SLA-bound), business-critical, not safe to rerun, or you are 
testing/experimenting and do not know the usage pattern yet 
• Example: "This pipeline feeds the CFO's daily morning report at 7 AM. If it fails, she calls the CTO." 
Spot: 
• Up to 90% cheaper, can be interrupted with 2-minute warning 
• Use when: Job is fault-tolerant (checkpointed, idempotent), flexible deadline, batch workloads, dev/test environments 
• Example: "This weekly historical backfill job runs on Saturday night. If it gets interrupted, it retries Sunday morning. Nobody cares." 
Reserved: 
• Commit 1 or 3 years, up to 72% discount, guaranteed capacity 
• Use when: You have a steady-state, always-on or predictable workload that will definitely run for the commitment period 
• Example: "Our Redshift cluster and an EMR cluster run 24/7/365 for real-time reporting. We have used them daily for 2 years already." 
The right answer for a new project: 
Start with On-Demand (no risk while figuring out the workload). Once usage is stable and predictable (after 2-3 months), purchase a Savings Plan or Reserved Instance for the baseline. Use Spot for any elastic/variable portion. 
Production Pattern — "Base + Burst": 
[Reserved/Savings Plan] → Covers the always-running baseline (e.g., 5 nodes always on) [Spot] → Covers peak processing bursts (e.g., 20 extra nodes during heavy load) 
[On-Demand] → Fallback if Spot capacity unavailable (avoids job failure) 
In EMR, this is configured as: 
- Instance Fleet with weighted capacities 
- Primary fleet: On-Demand (driver node — must not be interrupted) 
- Core fleet: Reserved/On-Demand (data nodes — interruption causes data loss) 
- Task fleet: Spot (compute-only nodes — safe to interrupt, no data stored) 
Scenario 
Best Choice 
Why
New project, unknown usage 
On-Demand 
No commitment until you know the pattern
Nightly batch, flexible SLA 
Spot 
Safe to retry, huge cost saving
Critical morning report 
On-Demand or Reserved 
Cannot risk interruption



Page 17 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Scenario 
Best Choice 
Why
24/7 always-on cluster 
Reserved or Savings Plan 
Predictable, long-term commitment pays off
Burst processing (irregular) 
Spot 
Variable and fault-tolerant
EMR driver/master node 
On-Demand always 
Never use Spot for master — losing it kills the job



© DataArchitectStudio
Q11 — You Increased Glue DPUs From 5 to 50 But the Job Did Not Get 10x Faster 
The Question 
Your Glue job uses 5 DPUs (Data Processing Units) and takes 60 minutes to process 500 GB. You increase DPUs to 50. You expect it to run in 6 minutes (10x more resources = 10x faster). It finishes in 40 minutes. You get about 1.5x improvement for a 10x increase in resources — and a 10x higher bill. 
Where did the 8.5x speed improvement go? 
The Answer 
This is Amdahl's Law in practice — the speedup of a parallel program is limited by the portion of the program that cannot be parallelized. 
In your Glue job, several parts cannot be parallelized regardless of how many DPUs you add: 
1. The Serial Portion — Driver overhead: 
Each Glue job has one driver (master) node that coordinates tasks, manages the DAG (Directed Acyclic Graph of operations), collects results, and writes output metadata. The driver runs on one DPU. Going from 5 to 50 DPUs gave you 49 workers instead of 4 — but the driver's coordination work also increased (it now manages 10x more tasks). 
2. Small input file problem: 
If your 500 GB is stored as 10,000 small files (50 MB each), each file becomes one task. With 5 DPUs (8 executors), you process ~1,250 files per executor. With 50 DPUs (80 executors), each executor handles fewer files — but the file open/close overhead per executor stays constant. Worse: if each file is 50 MB and Spark's default partition size is 128 MB, Spark cannot combine files across partition boundaries efficiently. 
3. Shuffle stages are bottlenecks regardless of parallelism: 
Page 18 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
If your job has a groupBy or join, all data must be reshuffled across nodes. This network transfer is bounded by network bandwidth, not DPU count. 
Fix:
```python
# 1. Combine small files before processing (S3 groupFiles option)
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://your-bucket/input/"],
        "groupFiles": "inPartition",  # Combine small files into larger splits
        "groupSize": "134217728"  # 128 MB target size per split
    },
    format="parquet"
)

# 2. Set optimal parallelism — not too high, not too low
# Rule of thumb: number of partitions = 2-4x number of executor cores
# With 50 DPUs: ~400 executor cores
# So set shuffle partitions to 800-1600
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# 3. Check the Glue job metrics — is the bottleneck CPU, memory, or I/O?
# CloudWatch metrics to watch:
# - glue.driver.ExecutorAllocationManager.executors.numberAllExecutors
# - glue.ALL.s3.filesystem.read_bytes (I/O bound?)
# - glue.ALL.jvm.heap.usage (memory pressure?)
``` 
Practical DPU sizing guide: 
Data Size aA
Recommended DPUs 
Notes
< 10 GB t
2-5 DPUs 
More DPUs waste money
10 GB - 100 GB a
5-10 DPUs 
Standard workloads
100 GB - 1 TB 
10-25 DPUs 
Watch for skew
1 TB - 10 TB D
25-100 DPUs 
Profile before scaling
> 10 TB  
Consider EMR 
Glue has max DPU limits



©
Q12 — Redshift COPY Command Is Taking 3 Hours for 500 GB — What Is Wrong? 
Page 19 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
The Question 
You are loading 500 GB of Parquet files from S3 into Redshift using the COPY command. You expect it to be fast 
— COPY is Redshift's optimized bulk load mechanism. But it has been running for 3 hours with no sign of finishing. © DataArchitectStudio
A colleague loaded a similar dataset last week in 12 minutes. Same table, same cluster, same data volume. What is the colleague doing differently that you are not? 
The Answer 
There are four likely causes — and your colleague is probably doing all four correctly: 
1. Number of files vs number of slices: 
The most common COPY performance mistake. Redshift COPY is parallelized — each compute node slice reads a separate file simultaneously. If you have 8 nodes × 4 slices each = 32 slices, the optimal input is 32 files or a multiple of 32. If your 500 GB is in a single large file, only one slice is loading data and 31 slices sit idle. 
2. File compression: 
COPY is network-bound from S3 to Redshift. Uncompressed Parquet at 500 GB takes 5x longer to transfer than gzip-compressed Parquet at ~100 GB. Always use compressed files. 
3. Sort key conflicts during load: 
If you are loading data that is NOT in sort key order into a table that has many existing rows, Redshift cannot insert it cleanly — it writes unsorted data and marks sort key order as violated. This triggers background merge operations that compete with the COPY for I/O. 
4. Distribution key hotspot: 
If your table uses KEY distribution on a low-cardinality column (e.g., country — only 5 values), all rows with country = 'US' go to one node. That node receives 80% of the data while others receive 5% each. The load is IO-bound on one node. 
```sql
-- OPTIMAL COPY command with all best practices
-- Step 1: Check your cluster's slice count
SELECT COUNT(*) AS total_slices FROM stv_slices;

-- Result: 32 → your S3 input should have 32, 64, 96, 128... files

-- Step 2: Split your input file if needed (use AWS CLI or Glue)
-- Each file should be 100-250 MB compressed

-- Step 3: Run COPY with proper options
COPY your_schema.your_table
FROM 's3://your-bucket/input-data/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftCopyRole'
FORMAT AS PARQUET
COMPUPDATE OFF  -- Skip automatic compression analysis (saves time if you know your compression)
STATUPDATE OFF  -- Skip auto statistics update (run ANALYZE manually after load)
MAXERROR 0;  -- Fail on any error (or set a tolerance)

-- Step 4: After load, run ANALYZE once
ANALYZE your_schema.your_table;

-- Check COPY load performance after it completes
SELECT
    query,
    trim(filename) AS file,
    lines_scanned,
    bytes_scanned,
    transfer_microsecs
FROM stl_load_commits
WHERE query = pg_last_copy_id()
ORDER BY transfer_microsecs DESC;
``` 
Why it catches people off guard: 
COPY is described as "the fastest way to load data into Redshift" — which is true. But fast is relative. A single-file COPY with no parallelism is slower than a naive INSERT loop with parallelism. The speed of COPY comes entirely from how well you exploit parallelism through file count matching slice count. 
Q13 — Your EMR Cluster Auto-Scaled But Costs Tripled — You Did Nothing Wrong 
The Question 
You set up EMR with Auto Scaling — a great cost-saving feature, right? The cluster scales up when CPU is high and scales down when CPU is low. You go on vacation for a week. You come back to an AWS bill 3x higher than expected. CloudWatch shows the cluster scaled up to 50 nodes multiple times throughout the week. Your Spark jobs themselves are normal — same jobs, same data. What happened? 
Auto Scaling is supposed to save money. How did it triple your costs? 
The Answer 
Page 21 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
Auto Scaling scaled up correctly in response to high CPU — but the root cause of that high CPU was not more data. It was inefficient Spark jobs creating excessive GC (Garbage Collection) pressure, which looks like high CPU to the scaler. 
Here is what actually happened: 
© DataArchitectStudio
1. Memory misconfiguration → GC pressure → high CPU → auto-scale: 
Spark executors were configured with insufficient memory (e.g., 4 GB heap for processing 10 GB partitions). When executors cannot fit data in memory, they spill to disk AND trigger frequent JVM garbage collection. GC is CPU-intensive. CloudWatch sees CPU at 90% and triggers scale-up. More nodes join. But more nodes do not fix a memory problem — they split the data into smaller partitions, which actually increases GC frequency per executor. Auto Scaling keeps adding nodes trying to fix a CPU metric that is caused by memory, not computation. 
2. Termination delay: 
EMR Auto Scaling has a default scale-down cooldown period and a termination delay (instances cannot be terminated if they have active tasks). If tasks keep starting on new nodes before old ones are released, the cluster stays large even when overall load is low. 
3. Auto Scaling target was wrong metric: 
Scaling on YARNMemoryAvailablePercentage (memory) is more appropriate for Spark than scaling on CPU. CPU can spike for reasons unrelated to needing more compute capacity. 
Fix: 
1. Fix the root cause — increase executor memory 
spark.executor.memory = 8g (instead of 4g) 
spark.executor.memoryOverhead = 2g (for off-heap, native operations) 
2. Use better Auto Scaling metrics 
Scale UP trigger: YARNMemoryAvailablePercentage < 15% 
Scale DOWN trigger: YARNMemoryAvailablePercentage > 75% for 5 minutes 
3. Set a maximum node count ceiling 
Max instances = 20 (not unlimited) 
This caps your worst-case bill even if something goes wrong 
4. Enable EMR Managed Scaling (newer, smarter than Auto Scaling policies) 
AWS manages scale decisions using internal metrics 
More accurate than CloudWatch-based rules 
Q14 — Kinesis Is Dropping Records Even Though You Have Enough Shards 
Page 22 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
The Question 
Your Kinesis Data Stream has 10 shards. Each shard can handle 1,000 records/second or 1 MB/second 
(whichever is lower). Your application sends 5,000 records/second at 100 bytes each — well within the 10,000 © DataArchitectStudio
records/second and 10 MB/second limit. Yet records are being dropped with ProvisionedThroughputExceededException. You have headroom. How are records being dropped? 
This looks mathematically impossible. But it happens constantly in production. 
The Answer 
The same hot partition problem from DynamoDB applies to Kinesis. The 10-shard limit of 10,000 records/second is a total capacity — but each shard independently handles its own 1,000 records/second. If your partition key distribution is uneven, some shards get hammered while others are idle. 
How Kinesis routes records: 
When you call PutRecord, you specify a PartitionKey. Kinesis hashes the partition key using MD5 and maps it to a shard based on the hash range each shard owns. If 70% of your records have the same partition key (or partition keys that hash to the same shard's range), one shard gets 3,500 records/second against its 1,000 records/second limit. 
Common bad partition key choices: 
• partition_key = "user_type" with values like "premium" or "free" — low cardinality, all 
premiums hash to same shard 
• partition_key = datetime.now().strftime("%H") — all records in the same hour use the same key 
• partition_key = "default" — a fixed string used by developers who did not think about it 
Fix: 
import hashlib 
```python
import uuid

# BAD — all records go to same shard
kinesis.put_record(
    StreamName='your-stream',
    Data=record_data,
    PartitionKey='user_events'  # Fixed key → hot shard
)

# GOOD Option 1 — use a high-cardinality field
kinesis.put_record(
    StreamName='your-stream',
    Data=record_data,
    PartitionKey=str(user_id)  # Unique per user → even distribution
)

# GOOD Option 2 — random partition key (if ordering does not matter)
kinesis.put_record(
    StreamName='your-stream',
    Data=record_data,
    PartitionKey=str(uuid.uuid4())  # Random → perfectly even distribution
)

# GOOD Option 3 — PutRecords batch with varied partition keys
records = [
    {'Data': record, 'PartitionKey': str(i % 1000)}  # Cycle through 1000 keys
    for i, record in enumerate(batch)
]
kinesis.put_records(StreamName='your-stream', Records=records)
``` 
Additional check — Enhanced Fan-Out vs Standard consumers: 
Standard Kinesis consumers share the 2 MB/second READ limit per shard across all consumers. If you have 5 consumers reading the same shard, each gets only 400 KB/second. Use Enhanced Fan-Out to give each consumer its own dedicated 2 MB/second per shard. 
Q15 — You Enabled Redshift Auto Vacuum But Table Scans Are Still Slow 
The Question 
You read about Redshift's automatic VACUUM feature — it runs in the background and automatically reclaims space from deleted rows and re-sorts data. You enabled it. You waited a week. But your table scans are still slow and svv_table_info shows unsorted is still at 40%. You expected Auto Vacuum to fix this. Why hasn't it? 
Auto Vacuum is running. The table is still unsorted. Contradiction? 
The Answer 
Auto Vacuum has a lower priority than user queries and throttles itself deliberately to avoid impacting query performance. In a busy production cluster, it may never get enough uncontested time to fully sort a large table. 
Here is what actually happens: 
Auto Vacuum's priority system: 
Page 24 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
• Vacuum runs at lower system priority than user queries 
• When query load increases, Vacuum pauses to give resources to queries 
• On a cluster that runs queries 24/7, Auto Vacuum may get only small windows of a few minutes before being paused again 
© DataArchitectStudio
• A large table with billions of rows may require hours of uninterrupted vacuum time to fully sort — which a busy cluster never provides 
The threshold behavior: 
Auto Vacuum only prioritizes a table for sorting when unsorted_rows / total_rows > 5%. But even when triggered, it processes the unsorted portion incrementally — it may sort 10% of the unsorted rows before being paused, then pause for hours, then sort another 10%, etc. Meanwhile, new INSERT operations keep adding unsorted rows faster than vacuum can sort them. 
What actually works: 
-- Option 1: Run VACUUM manually during a low-traffic window 
-- Schedule for Sunday 2 AM when query load is minimal 
VACUUM SORT ONLY your_schema.your_table 
TO 95 PERCENT; -- Sort until 95% of rows are in sorted order 
-- (does not need to be 100% to be effective) 
-- Option 2: Check vacuum progress 
SELECT * FROM svv_vacuum_progress; 
-- Option 3: For very large tables — vacuum by partition incrementally 
-- Use WHERE clause to vacuum one time partition at a time 
VACUUM your_schema.your_table 
TO 95 PERCENT BOOST; -- BOOST uses more resources, runs faster 
-- Use only during maintenance windows 
-- Option 4: Redesign the ingestion pattern 
-- Instead of inserting rows directly (which always land unsorted), 
-- use a staging table + INSERT INTO SELECT + DROP staging 
CREATE TABLE your_schema.your_table_staging (LIKE your_schema.your_table); 
-- Load new data into staging (it is small, sorting is fast) 
COPY your_schema.your_table_staging FROM 's3://...' IAM_ROLE '...'; 
-- Merge: delete old versions of updated rows, insert new ones 
BEGIN; 
DELETE FROM your_schema.your_table 
USING your_schema.your_table_staging 
WHERE your_table.id = your_table_staging.id; 
INSERT INTO your_schema.your_table 
SELECT * FROM your_schema.your_table_staging; 
Page 25 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 

COMMIT; 
DROP TABLE your_schema.your_table_staging; -- Now run ANALYZE 
ANALYZE your_schema.your_table; 
Why it catches people off guard: 
ctStudio

"Automatic" implies it just works. And it does work — for lightly loaded clusters with moderate data churn. But in heavy production environments, "automatic" means "best effort, lower priority." For truly large tables with continuous inserts, manual vacuum scheduling during off-peak hours is the only reliable option. 
Summary — The Core Lessons 
# 
Lesson
Q1 
ite
Redshift performance degrades silently over time without VACUUM and ANALYZE 
Q2 h
A green job status means the code ran — not that the output is correct
Q3 
More nodes do not fix skew — they can make it worse
Q4 rc
Spectrum on well-formatted, partitioned Parquet can match native Redshift performance
Q5 A
Spot termination is expected — design for failure with checkpoints and bookmarks
Q6 ta
Reserved Instances are a discount commitment, not a service payment
Q7 a
Table-level capacity means nothing if partition keys are uneven
Q8 D
Glue throttling in production is usually an API concurrency problem, not a code problem
Q9  
Redshift survives single node failures silently through mirrored data blocks
Q10 
Use On-Demand to learn the pattern, then Reserve the baseline, Spot the burst
Q11 
More DPUs cannot fix serial bottlenecks, small files, or data skew
Q12 
COPY speed is determined by file count matching slice count, not file size



©
Page 26 
DataArchitectStudio Tricky Concepts in Data Modeling · 2026 
# 
Lesson
Q13 
Auto Scaling on the wrong metric can spend more than doing nothing
Q14 
Kinesis throughput limits apply per shard, not just per stream
Q15 
Automatic Vacuum is best-effort — busy clusters need scheduled manual vacuum



© DataArchitectStudio
The best way to use these questions: read the question, stop, write down your answer, then read the explanation. Where your answer differs from the explanation — that is your learning gap. Focus there. 
Page 27 
