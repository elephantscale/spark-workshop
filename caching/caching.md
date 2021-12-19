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
f = spark.read.text("data/twinkle/500M.data")
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

## Step-8: Discuss Caching