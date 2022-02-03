# Analyzing Structured Data

## Step-1: Explore the data

The data is in `data/house-sales` directory.  
Go ahead and look at some [sample house sales data](../data/house-sales/house-sales-sample.csv).

It is CSV data that looks like.

```text
Date,SalePrice,SqFtLot,SqFtTotLiving,Bathrooms,Bedrooms,YrBuilt,ZipCode
1/3/06,436000,6923,2850,3,4,1947,98199
1/26/06,595000,7000,2970,2.25,3,1967,98199
2/7/06,618000,7680,2010,1.75,5,1950,98199
2/9/06,432500,7000,1670,1.5,4,1963,98199
2/17/06,725000,6000,4220,4.5,8,1957,98199
3/1/06,998000,5854,3680,3,4,1967,98199
```

## Step-2: Start PySpark

```bash
# Be in project dir: 
$   cd ~/spark-workshop

$   ~/spark/bin/pyspark
```

## Step-3: Load CSV Data

We are going let Spark figure out the schema also.

```python
data_location = "data/house-sales/house-sales-simplified.csv" 
# data_location =  's3://elephantscale-public/data/house-prices/house-sales-simplified.csv'
# data_locatiion = 'https://elephantscale-public.s3.amazonaws.com/data/house-prices/house-sales-simplified.csv'


sales = spark.read.\
        option("header" ,"true").\
        option("inferSchema", "true").\
        csv(data_location)

sales.show()

sales.count()

# print schema
sales.printSchema()
```

## Step-4: Analyze Data

```python
## use describe to understand 'SalePrice'
sales.describe(["Bedrooms", "Bathrooms", "SalePrice"]).show()
```

```python
## Produce a report of 'sales by number of bedrooms' 
sales.groupBy("Bedrooms").count().show()
sales.groupBy("Bedrooms").count().orderBy("Bedrooms").show()
```

## Step-5: Filter Outliers

Let's only consider bedrooms less than 5

```python
under_5br = sales.filter("Bedrooms  < 5")
under_5br.count()
under_5br.sample(0.1).show()
under_5br.groupBy("Bedrooms").count().show()
```
