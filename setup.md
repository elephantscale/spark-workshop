# Setting up Spark

This guide assumes you have a Ubuntu Linux.  This is verified on Ubuntu 20.04.  But will probably work on recent Ubuntu Linux.

If you have other system like MacOS, please adjust accordingly.

## Step-1: Install JDK-11

We need JDK (Java Development Kit) not JRE (Java runtime environment), in-order  to run Spark.

```bash
$   sudo apt update
$   sudo apt install -y openjdk-11-jdk
```

Verify this by 

```bash
$   java -version
```

## Step-2: Install Anaconda Python

Needed to run Spark Python applications.

```bash
$   wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh

$    bash ./Anaconda3-2021.11-Linux-x86_64.sh
# go through the steps
```

After install is complete, open a new terminal so changes can take effect.  And then verify as

```bash
$   conda --version

$   python --version
```

Make sure the python version is from Anaconda.


## Step-3: Setup Spark

```bash
$   cd  # be in home dir

$   wget https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz

$   tar xvf spark-3.2.0-bin-hadoop3.2.tgz

$   mv spark-3.2.0-bin-hadoop3.2 spark

```

After this we will have spark installed in `~/spark`

## Step-4: Test PySpark

```bash
$   ~/spark/bin/pyspark
```

This will drop you into PySpark shell.  Try the following

```python
spark.range(1,10).show()
```

If you see an output like this, you are good

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

## (Optional) Step-5: Jupyter and Spark

Install following packages  (these are great for data analytics with Pyspark)

```bash
$   conda install numpy  pandas  matplotlib  seaborn  jupyter  jupyterlab
$   conda install -c conda-forge findspark
```

Running Jupyter with Spark.

```bash
export SPARK_HOME=$HOME/spark
jupyter lab
```

