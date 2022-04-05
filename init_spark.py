# initialize Spark Session

def init_spark():
    print("Initializing Spark...")
    import findspark
    findspark.init()  # uses SPARK_HOME
    print("Spark found in : ", findspark.find())

    import pyspark
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    # use a unique tmep dir for warehouse dir, so we can run multiple spark sessions in one dir
    import tempfile
    tmpdir = tempfile.TemporaryDirectory()
    #       print(tmpdir.name)

    config = ( SparkConf()
             .setAppName("TestApp")
             .setMaster("local[*]")
             .set('executor.memory', '2g')
             .set('spark.sql.warehouse.dir', tmpdir.name)
             )

    print("Spark config:\n\t", config.toDebugString().replace("\n", "\n\t"))
    spark = SparkSession.builder.config(conf=config).getOrCreate()
    print('Spark UI running on port ' + spark.sparkContext.uiWebUrl.split(':')[2])

    return spark
## end def -- init_spark
