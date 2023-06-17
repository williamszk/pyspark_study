"""Find the least popular super heroes"""

from pyspark import SparkConf, SparkContext, Row
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    LongType,
)
import codecs


conf = SparkConf().setMaster("local").setAppName("MostPopularSuperHero")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Marvel-Graph.txt
# Marvel-Names.txt

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

names = spark.read.option("sep", " ").schema(schema).csv("datasets/Marvel-Names.txt")
names.show(10)
lines = spark.read.text("datasets/Marvel-Graph.txt")

connections = (
    lines.withColumn("id", func.split(func.col("value"), " ")[0])
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)

min_num_connections_option = connections.agg(func.min("connections")).first()
if min_num_connections_option:
    min_num_connections = min_num_connections_option[0]

print("min_num_connections: ", min_num_connections)
# connections.show(10)
connections_00 = connections.filter(func.col("connections") == min_num_connections)
connections_00.show(10)

connections_01 = connections_00.join(names, "id")
connections_01.show()

connections_02 = connections_01.drop(func.col("connections"))
connections_02.show(100)

optional_result = connections_02.first()
if optional_result:
    print(optional_result[1])
