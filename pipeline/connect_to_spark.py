from pyspark.sql import *
from delta import *


def connect_to_spark():
    builder = (
        SparkSession
        .builder
        # .master("local[*]")
        .remote("sc://localhost:15002")
        .appName("Pollen")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # Utworzenie sesji.
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    return session
