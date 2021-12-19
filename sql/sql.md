# Spark SQL

Let's analyze the same 'house sales' data using SQL!

## Step-1: Start PySpark

```bash
# Be in project dir: 
$   cd ~/spark-workshop

$   ~/spark/bin/pyspark
```

## Step-2: Load Data

```python
## Read data
house_sales = spark.read.\
        option("header" ,"true").\
        option("inferSchema", "true").\
        csv("data/house-sales/house-sales-simplified.csv")
house_sales.count()
```

## Step-3: Register as SQL table

```python
house_sales.createOrReplaceTempView("house_sales")
spark.catalog.listTables()
```

## Step-4: Query!

```python
## Simple SQL select
spark.sql("select * from house_sales").show()


## Group by query 'sales vs bedrooms'
spark.sql("select Bedrooms, count(*) as total from house_sales group by Bedrooms order by total desc").show()


## Min, Max, AVG prices
sql="""
select Bedrooms, 
       MIN(SalePrice) as min, 
       AVG(SalePrice) as avg, 
       MAX(SalePrice) as max 
from house_sales 
group by Bedrooms
"""

spark.sql(sql).show()
```

## Step-5: Practice Queries

Now that you see how SparkSQL works, try to answer the following questions using SparkSQL.

Query-1: Find average saleprice per 'property type'

Query-2: Find the history of 3 bedrooms + 2 bathrooms saleprice over the past 20 years.  Sort it from earliest year to latest