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

connections.show(10)

most_popular = connections.sort(func.col("connections").desc()).first()

if most_popular:
    most_popular_name = (
        names.filter(func.col("id") == most_popular[0]).select("name").first()
    )
    if most_popular_name:
        print(
            f"The most hero is: {str(most_popular_name[0])}. With {str(most_popular[1])} co-heros."
        )
else:
    print("Sorry, we couldn't find the most popular super hero")
