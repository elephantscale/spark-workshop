# Spark Caching

Let's see how to cache data in memory using Spark

## STEP-1: Generate some large data

Under `data/twinkle` directory we have created some large data files for you. 

You can generate more data if you'd like.

```bash
$   cd  spark-workshop
$   cd data/twinkle
$   bash ./create-data-files.sh
```

This will create a bunch of data files of size varying from 1MB to 1GB

## Step-2: Start PySpark

```bash
# Be in project dir: 
$   cd ~/spark-workshop

$   ~/spark/bin/pyspark
```

## Step-3: Set Logging

```python
sc.setLogLevel("INFO")
```

## STEP 3: Recording Caching times

Download and inspect the Excel worksheet : [caching-worksheet](caching-worksheet.xlsx).   
We are going to fill in the values here to understand how caching performs.

## STEP 4: Load Data

Load a big file (e.g 500M.data)

```python
# data_location = 'data/twinkle/500M.data'
data_location = 's3://elephantscale-public/data/text/twinkle/100M.data'
# data_location = 'https://elephantscale-public.s3.amazonaws.com/data/text/twinkle/100M.data'

f = spark.read.text(data_location)

print(f)
```

Do a count:

```python
f.count()
# output might look like
# Job 1 finished: count at <console>:30, took __3.792822__ s
```

Do a couple more counts and record

```python
f.count()
f.count()
```

## Step-5: Cache

Let's cache the data

```python
f.cache()
```

## Step-6: Counts after cache

Do a couple of counts and record the times.

```python
f.count()
f.count()
```

## STEP-7:  Understanding Cache storage

Go to spark shell UI @ port 4040  
**=> Inspect 'storage' tab**  

## Step-8: Caching SQL Tables

Let's try caching tables

```python

data_location = "data/house-sales/house-sales-simplified.csv" 
# data_location =  's3://elephantscale-public/data/house-prices/house-sales-simplified.csv'
# data_location = 'https://elephantscale-public.s3.amazonaws.com/data/house-prices/house-sales-simplified.csv'

house_sales = spark.read.\
        option("header" ,"true").\
        option("inferSchema", "true").\
        csv(data_location)

house_sales.createOrReplaceTempView("house_sales")
```

```python
# query before caching
sql = """
select Bedrooms, count(*) as total 
from house_sales 
group by Bedrooms 
order by total desc
"""

spark.sql(sql).show()
```

Cache

```python
spark.sql("cache table house_sales");
```

Query after caching

```python
spark.sql(sql).show()
```

## Some Sample Code for Timing

```python
import time

t1 = time.perf_counter()
spark.sql(sql).show()
t2 = time.perf_counter()
print ("query took {:,.2f} ms ".format( (t2-t1)*1000))
```