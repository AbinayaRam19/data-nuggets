import json
from pyspark.sql import SparkSession, DataFrame
from es_connect import ESConnect
import sys


class SparkWithES:
    def es_func(self):
        es = ESConnect("https://localhost:9200", "", "")

        spark = SparkSession.builder \
            .appName("Spark Elasticsearch Connect Example") \
            .getOrCreate()

        schema = "code int, occupation string, employed_population bigint"

        df = spark.read.format("csv").load("E:\Abi\workspace\Python-Codebase\com\es\\resources\occupation-census.csv",
                                           schema=schema)

        print("COUNT OF RECORDS ", df.count())

        es.write("census_index", df)
        print("****************************ES WRITE COMPLETED************************")

        result = es.select(spark, "census_index", df, {"code": [541112, 271213]})
        print("****************************ES SEARCH RESULT**************************")
        print("RESULT  ===> ")
        result.show(truncate=False)
        print("COUNT OF RESULT ", result.count())


if __name__ == "__main__":
    app = SparkWithES()  # Initialize the application
    app.es_func()
