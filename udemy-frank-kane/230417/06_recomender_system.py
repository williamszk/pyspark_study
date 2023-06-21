# https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/learn/lecture/3726154#overview


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


# def computeCosineSimilarity(spark, data):
def computeCosineSimilarity(data):
    pairScores = (
        data.withColumn("xx", func.col("ratings1") * func.col("ratings1"))
        .withColumn("yy", func.col("ratings2") * func.col("ratings2"))
        .withColumn("xy", func.col("ratings1") * func.col("ratings2"))
    )

    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
        func.sum(func.col("xy")).alias("numerator"),
        (
            func.sqrt(func.sum(func.col("xx")))
            * func.sqrt(func.sum(func.col("yy"))).alias("denominator")
        ),
        func.count(func.col("xy")).alias("numPairs"),
    )

    result = calculateSimilarity.withColumn(
        "score",
        func.when(
            func.col("denominator") != 0,
            func.col("numerator") / func.col("denominator"),
        ).otherwise(0),
    ).select("movie1", "movie2", "score", "numPairs")

    return result


def getMovieName(movieNames, movieId):
    result = (
        movieNames.filter(func.col("movieID") == movieId)
        .select("movieTitle")
        .collect()[0]
    )

    return result[0]


# ===================== main =====================================

# def main():
movieNameSchema = StructType(
    [
        StructField("movieId", IntegerType(), True),
        StructField("movieTitles", StringType(), True),
    ]
)

moviesSchema = StructType(
    [
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# create a broadcast dataset of movieID and movieTitle
movieNames = (
    spark.read.option("sep", "|")
    .option("charset", "ISO-8859-1")
    .schema(movieNameSchema)
    .csv("./datasets/ml-100k/u.item")
)

type(movieNames)

movieNames.show(10)

movies = (
    spark.read.option("sep", "\t")
    .option("charset", "ISO-8859-1")
    .schema(moviesSchema)
    .csv("./datasets/ml-100k/u.data")
)

movies.show(3)

# ignore timestamp
ratings = movies.select("userId", "movieId", "rating")
ratings.show(10)

# doing a self join
moviePairs = (
    ratings.alias("ratings1")
    .join(
        ratings.alias("ratings2"),
        (func.col("ratings1.userId") == func.col("ratings2.userId"))
        & (func.col("ratings1.movieId") < func.col("ratings2.movieId")),
    )
    .select(
        func.col("ratings1.movieId").alias("movie2"),
        func.col("ratings1.rating").alias("rating1"),
        func.col("ratings2.rating").alias("rating2"),
    )
)

moviePairs.show(10)

# if __name__ == "__main__":
#     main()
