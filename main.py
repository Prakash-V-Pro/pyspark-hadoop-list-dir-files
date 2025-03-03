def get_dir_and_files(spark, base_folder):
  """ 
  function returns files specified in the base_folder using hadoop fs
    Params: 
      spark: SparkSession object
      base_folder: path to check the files
  """

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(base_folder)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    statuses = fs.listStatus(hadoop_path)

    print(statuses)
    dirs = []
    for status in statuses:
        if status.isDirectory():
            dirs.append(status.getPath().getName())
    
    return dirs

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    session = SparkSession\
        .builder\
        .appName("app_name")\
        .getOrCreate()
    
    get_dirs_and_files(spark=session, base_folder= "/")

