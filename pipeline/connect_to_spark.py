from pyspark.sql import SparkSession


def connect_to_spark():
  # Connect to the running Spark Connect server.
  # Delta jars and extensions are pre-loaded on the server side,
  # so no client-side package or extension configuration is required.
  session = (
      SparkSession.builder.remote("sc://localhost:15002")
      .appName("Pollen")
      .getOrCreate()
  )

  return session