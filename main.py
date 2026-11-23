from config import DATA_DIR
from pipeline.nan_counter import nan_counter
from pipeline.connect_to_spark import connect_to_spark
from pyspark.sql import functions as F
from pipeline.data import save_silver_data
from pipeline.metadata import save_metadata


def main():
    spark_session = connect_to_spark()

    save_metadata()
    save_silver_data()

    pollen_historical = spark_session.read.format("delta").load(str(DATA_DIR / "delta" / "silver" / "pollen_historical"))
    print(f"pollen_historical total rows: {pollen_historical.count()}")
    # pollen_historical.groupby("station_abbr").count().show()
    nan_counter(pollen_historical)

    pollen_recent = spark_session.read.format("delta").load(str(DATA_DIR / "delta" / "silver" / "pollen_recent"))
    print(f"pollen_recent total rows: {pollen_recent.count()}")
    #pollen_recent.groupby("station_abbr").count().show()
    nan_counter(pollen_recent)

    meteo_historical = spark_session.read.format("delta").load(str(DATA_DIR/"delta"/"silver"/"meteo_historical"))
    print(f"meteo_historical total rows: {meteo_historical.count()}")
    # meteo_historical.groupby("station_abbr").count().show()
    nan_counter(meteo_historical)

    meteo_recent = spark_session.read.format("delta").load(str(DATA_DIR / "delta" / "silver" / "meteo_recent"))
    print(f"meteo_recent total rows: {meteo_recent.count()}")
    # meteo_historical.groupby("station_abbr").count().show()
    nan_counter(meteo_recent)


    parameters = spark_session.read.format("delta").load(str(DATA_DIR/"delta"/"metadata"/"parameters"))
    parameters.show()
    print(f"Total rows: {parameters.count()}")

    pollen_stations = spark_session.read.format("delta").load(str(DATA_DIR / "delta" / "metadata" / "pollen_stations"))
    pollen_stations.show()
    print(f"Total rows: {pollen_stations.count()}")

if __name__ == "__main__":
    main()