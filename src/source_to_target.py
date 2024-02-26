import uuid
from datetime import datetime
from pyspark.sql import *
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

import logging
# Set up logging configuration
logging.basicConfig(filename='logs/etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def source_to_target(spark):
    current_date = datetime.now().strftime("%Y-%m-%d")
    # Define the schema
    schema = StructType([
        StructField('OrderID', LongType(), nullable=False),
        StructField('OrderDate', LongType(), nullable=False),
        StructField('Amount', DoubleType(), nullable=False),
        StructField('ProductID', LongType(), nullable=False),
        StructField('Quantity', LongType(), nullable=False),
        StructField('CustomerID', LongType(), nullable=False),
        StructField('DeliveryAddress', StringType(), nullable=False),
        StructField('VendorName', StringType(), nullable=False),
        StructField('PaymentType', StringType(), nullable=False),
        StructField('ProductName', StringType(), nullable=False),
        StructField('ProductCategory', StringType(), nullable=False),
        StructField('ProductSubcategory', StringType(), nullable=False),
        StructField('Price', DoubleType(), nullable=False),
        StructField('CustomerFirstName', StringType(), nullable=False),
        StructField('CustomerLastName', StringType(), nullable=False),
        StructField('Gender', StringType(), nullable=False),
        StructField('DateOfBirth', DateType(), nullable=False),
        StructField('SubscriptionStartDate', LongType(), nullable=False)
    ])

    df = spark.read \
            .schema(schema) \
            .parquet(f"../data/{current_date}/data.parquet")
    # Convert the timestamp column to a proper timestamp
    df = df.withColumn("OrderDate", (df["OrderDate"] / 1e9).cast("timestamp")) \
        .withColumn("SubscriptionStartDate", (df["SubscriptionStartDate"] / 1e9).cast("timestamp"))

    customer_df = df.select("CustomerID","CustomerFirstName","CustomerLastName","Gender","DateOfBirth","SubscriptionStartDate").distinct()

    product_df = df.select("ProductID","ProductName","ProductCategory","ProductSubcategory","Price").distinct()

    df = df.withColumn("address_id",monotonically_increasing_id())

    address_df = df.select("DeliveryAddress","address_id").distinct()

    jdbc_url = f"jdbc:sqlserver://localhost:1433;database=pdf;"
    properties = {
        "user": "pyspark_user",
        "password": "root",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "trustServerCertificate": "true"  # Specify properties like this
    }
    customer_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "customer") \
        .options(**properties)  \
        .mode("overwrite") \
        .save()
    logging.info("customer data  written to customer table")
    product_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "product") \
        .options(**properties)  \
        .mode("overwrite") \
        .save()
    logging.info("product data written to  product  table")
    address_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "address") \
        .options(**properties)  \
        .mode("overwrite") \
        .save()
    logging.info("address data written to  address  table")
    df = df.drop("DeliveryAddress","VendorName","PaymentType","ProductName","ProductCategory","ProductSubcategory","Price","CustomerFirstName","CustomerLastName","Gender","DateOfBirth","SubscriptionStartDate")

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "orders") \
        .options(**properties)  \
        .mode("overwrite") \
        .save()
    logging.info("orders data written to  orders  table")

