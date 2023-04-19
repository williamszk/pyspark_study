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
import codecs


conf = SparkConf().setMaster("local").setAppName("MovieRating")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def load_movie_names():
    movie_names = {}

    with codecs.open(
        "datasets/ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore"
    ) as f:
        for line in f:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]

    return movie_names


name_dict = spark.sparkContext.broadcast(load_movie_names())

schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

movies = spark.read.option("sep", "\t").schema(schema).csv("datasets/ml-100k/u.data")

print("movies")
movies.show(10)
print("#lines: ", movies.count())

movies_counts = movies.groupBy("movieID").count()

print("movies_counts")
movies_counts.show(10)
print("#lines: ", movies_counts.count())


def lookup_name(movie_id):
    return name_dict.value[movie_id]


lookup_name_udf = func.udf(lookup_name)

movies_with_names = movies_counts.withColumn(
    "movieTitle", lookup_name_udf(func.col("movieID"))
).orderBy(func.desc("count"))

print("movies_with_names")
movies_with_names.show(10)
print("#lines: ", movies_with_names.count())
