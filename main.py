from config import DATA_DIR
from pipeline.connect_to_spark import connect_to_spark
from pipeline.data import save_silver_data
from pipeline.metadata import save_metadata


def main():
    spark_session = connect_to_spark()

    save_metadata()
    save_silver_data()

    meteo_historical = spark_session.read.format("delta").load(str(DATA_DIR/"delta"/"silver"/"meteo_historical"))
    print(f"Total rows: {meteo_historical.count()}")
    meteo_historical.groupby("station_abbr").count().show()

    pollen_historical = spark_session.read.format("delta").load(str(DATA_DIR / "delta" / "silver" / "pollen_historical"))
    print(f"Total rows: {pollen_historical.count()}")
    pollen_historical.groupby("station_abbr").count().show()

    parameters = spark_session.read.format("delta").load(str(DATA_DIR/"delta"/"metadata"/"parameters"))
    parameters.show()
    print(f"Total rows: {parameters.count()}")

    pollen_stations = spark_session.read.format("delta").load(str(DATA_DIR / "delta" / "metadata" / "pollen_stations"))
    pollen_stations.show()
    print(f"Total rows: {pollen_stations.count()}")

if __name__ == "__main__":
    main()