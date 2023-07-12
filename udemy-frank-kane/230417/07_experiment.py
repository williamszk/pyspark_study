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

dfSchema = StructType(
    [
        StructField("movie1", IntegerType(), True),
        StructField("movie2", IntegerType(), True),
        StructField("value", IntegerType(), True),
    ]
)

df01 = (
    spark.read.option("sep", ",")
    .option("header", "true")
    .option("charset", "ISO-8859-1")
    .schema(dfSchema)
    .csv("./datasets/experiment_01.csv")
)

df01.show(10)

df02 = df01.groupBy("movie1", "movie2").agg(func.sum(func.col("value")))

df02.show(10)

# >>> df01.show(10)
# +------+------+-----+
# |movie1|movie2|value|
# +------+------+-----+
# |     1|     2|    3|
# |     1|     2|    4|
# |     2|     1|    1|
# |     2|     1|    3|
# +------+------+-----+

# >>> df02 = df01.groupBy("movie1", "movie2").agg(
# ...     func.sum(func.col("value"))
# ... )
# >>>
# >>> df02.show(10)
# +------+------+----------+
# |movie1|movie2|sum(value)|
# +------+------+----------+
# |     1|     2|         7|
# |     2|     1|         4|
# +------+------+----------+
