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
f = spark.read.text("data/twinkle/sample.txt")

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
