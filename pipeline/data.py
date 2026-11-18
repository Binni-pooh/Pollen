from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from config import DATA_DIR
from data_loader import load_or_download_csv
from pipeline.connect_to_spark import connect_to_spark


def download_data_by_station(station_abbr, station_type: str):
    if station_type == "pollen_historical":
        base_url = 'https://data.geo.admin.ch/ch.meteoschweiz.ogd-pollen/'
        file_start = "ogd-pollen_"
        file_end = "_d_historical.csv"
    elif station_type == "pollen_recent":
        base_url = 'https://data.geo.admin.ch/ch.meteoschweiz.ogd-pollen/'
        file_start = "ogd-pollen_"
        file_end = "_h_recent.csv"
    elif station_type == "phenology":
        base_url = 'https://data.geo.admin.ch/ch.meteoschweiz.ogd-phenology/'
        file_start = "ogd-phenology_"
        file_end = "_y.csv"
    elif station_type == "meteo_historical":
        base_url = 'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/'
        file_start = "ogd-smn_"
        file_end = "_d_historical.csv"
    elif station_type == "meteo_recent":
        base_url = 'https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/'
        file_start = "ogd-smn_"
        file_end = "_h_recent.csv"
    else :
        raise ValueError("Unknown type")

    for station in station_abbr:
        url = base_url + station.lower() + "/"
        file = file_start + station.lower() + file_end
        load_or_download_csv(url, file, save_dir=str(DATA_DIR / station_type))

def save_data_folder(path: str, name):
    spark_session = connect_to_spark()

    data = (spark_session.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .csv(str(Path(path) / "*.csv"))
            )

    (data.write
     .format("delta")
     .mode("overwrite")
     .save(str(DATA_DIR / "delta" / "bronze" / name))
     )

def clean_bronze_data(path: str, datatype: str):
    # Load data
    spark_session = connect_to_spark()
    data = spark_session.read.format("delta").load(path)
    parameters = spark_session.read.format("delta").load(str(DATA_DIR/"delta"/"metadata"/"parameters"))

    # Clean the columns
    valid_parameters = [row["parameter_shortname"] for row in parameters.select("parameter_shortname").collect()]
    mandatory_columns = ["station_abbr", "reference_timestamp"]
    value_columns = [col for col in data.columns if col in valid_parameters]
    relevant_columns = mandatory_columns + value_columns

    cleaned_data = data.select(*relevant_columns)

    # Parse timestamps
    cleaned_data = cleaned_data.withColumn(
        "reference_timestamp",
        F.to_timestamp(F.col("reference_timestamp"), "dd.MM.yyyy HH:mm")
    )

    if datatype == "pollen" :
        # Fill Nans
        # Define window specifications partitioned by station and ordered chronologically
        base_window = Window.partitionBy("station_abbr").orderBy("reference_timestamp")
        cumulative_window = base_window.rowsBetween(Window.unboundedPreceding, 0)

        # Apply conditional filling for each pollen parameter column safely
        for col_name in value_columns:
            # Check for BOTH SQL nulls (None) and floating-point NaNs
            is_null = F.col(col_name).isNull() | F.isnan(F.col(col_name))

            # 1. Create a unique group ID for consecutive missing blocks
            not_null_flag = F.when(is_null, 0).otherwise(1)
            group_id = F.sum(not_null_flag).over(cumulative_window)

            # 2. Count consecutive missing values within the gap block
            group_window = Window.partitionBy("station_abbr", group_id)
            null_count_in_group = F.sum(F.when(is_null, 1).otherwise(0)).over(group_window)

            # 3. Compute standard forward-filled value
            forward_filled = F.last(F.col(col_name), ignorenulls=True).over(base_window)

            # 4. Fill only if missing AND gap length is within a safe limit (e.g., <= 3)
            # This prevents processing infinitely large skew groups
            cleaned_data = cleaned_data.withColumn(
                col_name,
                F.when(is_null & (null_count_in_group <= 3), forward_filled).otherwise(
                    F.col(col_name)
                ),
            )
    elif datatype == "phenology":
        for col_name in value_columns:
            cleaned_data = cleaned_data.withColumn(
                col_name,
                # 1. Cast to bigint to remove the .0 and convert float NaNs to nulls
                # 2. Cast to string so to_timestamp can read it
                # 3. Parse into a proper timestamp
                F.to_timestamp(F.col(col_name).cast("bigint").cast("string"), "yyyyMMdd")
            )
    elif datatype == "meteo":
        pollen_stations = (
            spark_session.read.format("delta")
            .load(str(DATA_DIR / "delta" / "metadata" / "pollen_stations"))
            .select(
                "closest_meteo_station",
                F.to_timestamp(F.col("data_since"), "dd.MM.yyyy HH:mm").alias("data_since"),
            )
        )

        # Join with pollen stations metadata and filter out records before data_since
        cleaned_data = (
            cleaned_data.join(
                F.broadcast(pollen_stations),
                cleaned_data["station_abbr"] == pollen_stations["closest_meteo_station"],
                how="left",
            )
            .filter(
                (F.col("reference_timestamp") >= F.col("data_since"))
                | F.col("data_since").isNull()
            )
            .drop("data_since", "closest_meteo_station")
        )
    else :
        raise ValueError("Unknown type")

    return cleaned_data

def save_silver_data():
    spark_session = connect_to_spark()
    # metadata_dir = DATA_DIR/"delta"/"metadata"
    # pollen_stations = spark_session.read.format("delta").load(str(metadata_dir/"pollen_stations"))
    # phenology_stations = spark_session.read.format("delta").load(str(metadata_dir/"phenology_stations"))
    # meteo_stations = spark_session.read.format("delta").load(str(metadata_dir/"meteo_stations"))
    #
    # # Extract station abbreviation strings into Python lists to iterate over
    # pollen_abbrs = [row["station_abbr"] for row in pollen_stations.select("station_abbr").collect()]
    # phenology_abbrs = [row["station_abbr"] for row in phenology_stations.select("station_abbr").collect()]
    # meteo_abbrs = [row["station_abbr"] for row in meteo_stations.select("station_abbr").collect()]
    #
    # # Download raw data using correct type keywords matching download_data_by_station()
    # download_data_by_station(pollen_abbrs, "pollen_historical")
    # download_data_by_station(phenology_abbrs, "phenology")
    # download_data_by_station(meteo_abbrs, "meteo_historical")
    #
    # save_data_folder(str(DATA_DIR/"pollen"), "pollen_historical")
    # save_data_folder(str(DATA_DIR/"phenology"), "phenology")
    # save_data_folder(str(DATA_DIR/"meteo"), "meteo_historical")

    silver_pollen = clean_bronze_data(str(DATA_DIR/"delta"/"bronze"/"pollen_historical"), "pollen")
    silver_phenology = clean_bronze_data(str(DATA_DIR/"delta"/"bronze"/"phenology"), "phenology")
    silver_meteo = clean_bronze_data(str(DATA_DIR/"delta"/"bronze"/"meteo_historical"), "meteo")

    print(f"Total rows: {silver_pollen.count()}")
    # silver_pollen.show()
    silver_pollen.write\
        .format("delta")\
        .mode("overwrite")\
        .save(str(DATA_DIR/"delta"/"silver"/"pollen_historical"))
    print(f"Total rows: {silver_phenology.count()}")
    # silver_phenology.show()
    silver_phenology.write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(DATA_DIR / "delta" / "silver" / "phenology"))
    print(f"Total rows: {silver_meteo.count()}")
    # silver_meteo.show()
    silver_meteo.write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(DATA_DIR / "delta" / "silver" / "meteo_historical"))

if __name__ == "__main__":
    save_silver_data()