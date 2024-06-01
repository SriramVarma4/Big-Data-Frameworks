from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

spark = SparkSession.builder.appName("Bal").getOrCreate()

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
customersf = customers.filter("acctbal > 1000")

orders = spark.read.csv("orders.csv", sep="|", header=True, schema=orders_schema)
ordersf = orders.filter("orderdate > '1995-01-01'")

join = customersf.join(ordersf, "custkey")

result = join.groupBy("name", "addres").agg(avg("price").alias("avg order price"))
result.show(result.count(), False)
spark.stop()
