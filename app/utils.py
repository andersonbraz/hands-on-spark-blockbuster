from pyspark import SparkConf
from pyspark.sql import SparkSession

def init_spark() -> SparkSession:
    """Create a Spark Session for Streamlit app"""
    conf = SparkConf().setAppName("my-app-colab").setMaster("local")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark, spark.sparkContext