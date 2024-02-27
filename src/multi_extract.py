from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, current_date, floor


def run_extact():
    spark = SparkSession.builder \
        .appName("extract app") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.extraClassPath",
                "C:/Users/ghegd/Downloads/sqljdbc_12.6.0.0_enu/sqljdbc_12.6/enu/jars/mssql-jdbc-12.6.0.jre8.jar") \
        .getOrCreate()

    jdbc_url = f"jdbc:sqlserver://localhost:1433;database=dw_stg;"
    properties = {
        "user": "pyspark_user",
        "password": "root",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "trustServerCertificate": "true"  # Specify properties like this
    }

    customer_df = spark.read.jdbc(url=jdbc_url, table="customer", properties=properties)
    orders_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=properties)
    product_df = spark.read.jdbc(url=jdbc_url, table="product", properties=properties)
    address_df = spark.read.jdbc(url=jdbc_url, table="address", properties=properties)
    # customers with most orders
    most_orders =  customer_df.join(orders_df,on="CustomerID",how="inner")
    most_orders = most_orders.drop("OrderID")

    most_orders.show()
    # customer id  with most orders
    precious_customer = most_orders.groupby("CustomerID").count().alias("count").orderBy("count",ascending=False).limit(1)
    precious_customer.show()

    customers_age_Df = most_orders \
        .withColumn("age", floor(datediff( current_date(),most_orders.DateOfBirth)/365))
    customers_age_Df.groupby("age").count().alias("count1").orderBy("count1",ascending=False).show()
run_extact()