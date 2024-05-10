from pyspark.sql import functions as F
from utils import init_spark

spark, sc = init_spark()

def __get_name_source(col):
    return F.when(F.lower(F.col(col)).contains("netflix"), "Netflix")\
        .when(F.lower(F.col(col)).contains("amazon_prime"), "Amazon Prime")\
        .when(F.lower(F.col(col)).contains("disney_plus"), "Disney Plus")\
        .otherwise(F.col(col))

def __get_raw():
    df = spark.read.option("delimiter", ",").option("header", True).csv("sources/*.csv")
    df = df.withColumn("source", F.input_file_name())
    df = df.withColumn("id_title", F.md5(F.col("title")))
    # df.show(truncate=False)
    return df

def get_all_titles():
    df_raw = __get_raw()
    df = df_raw.withColumn("source", __get_name_source("source"))
    df = df.select("source", "title", "type", "director", "country")
    # df.show(truncate=False)
    return df

def get_titles_by_source(source):
    df_titles = get_all_titles()
    df = df_titles.filter(F.upper(F.col("source")) == source.upper())
    df = df.select("source", "title", "type", "director", "country")
    # df.show(truncate=False)
    return df

def get_type_total():
    df_curated = get_all_titles()
    df = df_curated.groupBy("source", "type").agg(F.count(F.col("type")).alias("total"))
    df = df.na.drop()
    # df.show(truncate=False)
    return df

# print("Total items:", df_final.count())

# df_sources = df_final.groupBy("source").agg(F.count(F.col("source")).alias("total"))
# df_sources.show(truncate=False)

# df_type = df_final.groupBy("source", "type").agg(F.count(F.col("type")).alias("total"))
# df_type.show(truncate=False)

# df_filter = df_final.filter((F.col("title") == "Zombie Dumb"))
# df_filter.show(truncate=False)

