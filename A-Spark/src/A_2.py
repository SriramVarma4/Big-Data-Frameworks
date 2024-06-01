from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

spark = SparkSession.builder.appName("Norders").getOrCreate()

customer_schema = StructType([
    StructField("custkey", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("addres", StringType(), True),
    StructField("nationkey", IntegerType(), True),
    StructField("phone", StringType(), True),
    StructField("acctbal", FloatType(), True),
    StructField("mktsegment", StringType(), True),
    StructField("comment", StringType(), True)
])

orders_schema = StructType([
    StructField("orderkey", IntegerType(), True),
    StructField("custkey", IntegerType(), True),
    StructField("orderstatus", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("orderdate", DateType(), True),
    StructField("orderpriority", StringType(), True),
    StructField("clerk", StringType(), True),
    StructField("shippriority", IntegerType(), True),
    StructField("comment", StringType(), True)
])

customers = spark.read.csv("customers.csv", sep="|", header=True, schema=customer_schema)
orders = spark.read.csv("orders.csv", sep="|", header=True, schema=orders_schema)
customers_with_orders = orders.select("custkey").distinct()
all_customers = customers.select("custkey")
customers_without_orders = all_customers.exceptAll(customers_with_orders)
result = customers_without_orders.join(customers, "custkey").select("name")
result.show(result.count(), False)
spark.stop()
