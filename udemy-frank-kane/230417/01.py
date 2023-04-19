from pyspark import SparkConf, SparkContext
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

conf = SparkConf().setMaster("local").setAppName("MovieRating")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

movies = spark.read.option("sep", "\t").schema(schema).csv("datasets/ml-100k/u.data")

movies.show(10)

top_movies = movies.groupBy("movieID").count().orderBy(func.desc("count"))

top_movies.show(10)

spark.stop()
