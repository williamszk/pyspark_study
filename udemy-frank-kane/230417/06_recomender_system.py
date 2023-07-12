# https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/learn/lecture/3726154#overview

import sys
from pyspark import SparkConf, SparkContext, Row
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
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


def computeCosineSimilarity(spark, data):
    # data = moviePairs
    # moviePairs.show(10)
    pairScores = (
        data.withColumn("xx", fn.col("ratings1") * fn.col("ratings1"))
        .withColumn("yy", fn.col("ratings2") * fn.col("ratings2"))
        .withColumn("xy", fn.col("ratings1") * fn.col("ratings2"))
    )

    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
        fn.sum(fn.col("xy")).alias("numerator"),
        (
            fn.sqrt(fn.sum(fn.col("xx")))
            * fn.sqrt(fn.sum(fn.col("yy"))).alias("denominator")
        ),
        fn.count(fn.col("xy")).alias("numPairs"),
    )

    result = calculateSimilarity.withColumn(
        "score",
        fn.when(
            fn.col("denominator") != 0,
            fn.col("numerator") / fn.col("denominator"),
        ).otherwise(0),
    ).select("movie1", "movie2", "score", "numPairs")

    return result


def getMovieName(movieNames, movieId):
    result = (
        movieNames.filter(fn.col("movieID") == movieId)
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
        StructField("ratings", IntegerType(), True),
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
ratings = movies.select("userId", "movieId", "ratings")
ratings.show(10)

# doing a self join
moviePairs = (
    ratings.alias("ratings1")
    .join(
        ratings.alias("ratings2"),
        (fn.col("ratings1.userId") == fn.col("ratings2.userId"))
        & (fn.col("ratings1.movieId") < fn.col("ratings2.movieId")),
    )
    .select(
        fn.col("ratings1.movieId").alias("movie1"),
        fn.col("ratings2.movieId").alias("movie2"),
        fn.col("ratings1.ratings").alias("ratings1"),
        fn.col("ratings2.ratings").alias("ratings2"),
    )
)

# this is a function ----------------------------------------------------------
# moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()
# ratings.count()
# 100_000
moviePairs.show(10)
# moviePairs.count()
# 10_050_406
data = moviePairs
pairScores = (
    data.withColumn("xx", fn.col("ratings1") * fn.col("ratings1"))
    .withColumn("yy", fn.col("ratings2") * fn.col("ratings2"))
    .withColumn("xy", fn.col("ratings1") * fn.col("ratings2"))
)

pairScores.show(10)

# calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
#     fn.sum(fn.col("xy")).alias("numerator"),
# )
# calculateSimilarity.show(10)

# df01 = pairScores.withColumn("col_with_1s", fn.lit(1))
# df01.show(10)

# df02 = df01.groupBy("movie1", "movie2").agg(fn.sum(fn.col("col_with_1s")))
# df02.show(10)

# df03 = pairScores.groupBy("movie1", "movie2").agg(
#     fn.count(fn.col("xy"))
# )
# df03.show(10)

calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
    fn.sum(fn.col("xy")).alias("numerator"),
    (fn.sqrt(fn.sum(fn.col("xx"))) * fn.sqrt(fn.sum(fn.col("yy")))).alias(
        "denominator"
    ),
    fn.count(fn.col("xy")).alias("numPairs"),
)

calculateSimilarity.show(10)

result = calculateSimilarity.withColumn(
    "score",
    fn.when(
        fn.col("denominator") != 0,
        fn.col("numerator") / fn.col("denominator"),
    ).otherwise(0),
).select("movie1", "movie2", "score", "numPairs")

moviePairSimilarities = result
moviePairSimilarities.show(10)
# this is the end of a function -----------------------------------------------

if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieId = int(sys.argv[1])


# if __name__ == "__main__":
#     main()
