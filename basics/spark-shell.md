# Basics - Hello World

## Start PySpark

```bash
# Be in project dir: 
$   cd ~/spark-workshop

$   ~/spark/bin/pyspark
```

## Load Some Data

In PySpark:

```python
data_location = 'data/twinkle/sample.txt'
# data_location = 's3://elephantscale-public/data/text/twinkle/sample.txt'
# data_location = 'https://elephantscale-public.s3.amazonaws.com/data/text/twinkle/sample.txt'

f = spark.read.text(data_location)

# display data
f.show(10, False)  # upto 10 lines, do not truncate

# display count
f.count()
```

## Inspect Spark UI

Spark UI will be available at port 4040+

## Processing Data

```python
# look for lines with twinkle

#filtered = f.filter(f.value.contains('twinkle'))
filtered = f.filter (f['value'].contains('twinkle'))
filtered.show(10, False)
print (filtered.count())
```

Also inspect the Spark UI
