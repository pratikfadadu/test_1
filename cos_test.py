
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import numpy as np


if __name__ == '__main__':
    spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()

    df = spark.read.format('parquet').options(header='true', delimiter=',').load("cos://ssc-dsh-rei.south/salesTlog")
    print("ppptttrt....")
    print(df.show())