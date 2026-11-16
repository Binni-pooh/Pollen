from config import DATA_DIR
from pipeline.connect_to_spark import connect_to_spark
from pipeline.data import save_silver_data


def main():
    spark_session = connect_to_spark()

    save_silver_data()

    meteo_historical = spark_session.read.format("delta").load(str(DATA_DIR/"delta"/"silver"/"meteo_historical"))

    meteo_historical.show()

if __name__ == "__main__":
    main()