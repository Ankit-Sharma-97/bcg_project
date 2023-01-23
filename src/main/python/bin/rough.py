from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
current_path = os.getcwd() + '/bin'

print(current_path)
df = spark.read.csv("/home/ankit/PycharmProjects/etl1/src/main/python/staging/dim/Charges_use.csv",inferSchema=True,header=True)
df.printSchema()


