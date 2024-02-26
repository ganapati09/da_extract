from pyspark.sql import *
def start_spark_session():
    spark = SparkSession.builder \
        .appName("extract app") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.extraClassPath",
                "C:/Users/ghegd/Downloads/sqljdbc_12.6.0.0_enu/sqljdbc_12.6/enu/jars/mssql-jdbc-12.6.0.jre8.jar") \
        .getOrCreate()
    return spark

def stop_spark_session(session_name):
    session_name.stop()