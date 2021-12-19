# Basics - Start Spark

## Install Spark

Follow [setup guide](../setup.md) to install Spark.

We assume Spark is installed in `~/spark`

## Starting PySpark

Open a terminal and execute the following command.

```bash
$   ~/spark/bin/pyspark
```

This will start Spark and drop you into PySpark shell.  You will see a console output like this:

```text
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/
```

Also you might access the Spark UI on [localhost:4040](http://localhost:4040)

## Quick Test

In Spark shell try this out:

```python
> spark.range(1,10).show()
```

If you get an output like this, you are good:

```text
+---+
| id|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+
```
