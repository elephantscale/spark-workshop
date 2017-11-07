#!/bin/bash

## optional, if you want to use  python set the PATH here
#export PATH=$HOME/anaconda/bin:$PATH

export SPARK_HOME=$HOME/spark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

## pyspark i local mode
$SPARK_HOME/bin/pyspark

## pyspark connect to cluster
#$SPARK_HOME/bin/pyspark --master spark://localhost:7077  --executor-memory 4g --driver-memory 1g


## one liner
 #PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" ~/spark/bin/pyspark
