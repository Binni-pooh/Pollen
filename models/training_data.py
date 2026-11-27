from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F

from config import DATA_DIR
from pipeline.connect_to_spark import connect_to_spark


def get_training_data():
    """Build aligned train and test feature/target splits for a pollen-meteo station pair."""
    # -------------------------------------------------------------------------
    # 1. Configuration and temporal split boundaries
    # -------------------------------------------------------------------------
    meteo_parameters = [
        "erefaod0",
        "ure200d0",
        "rka150d0",
        "prestad0",
        "gre000d0",
        "sre000d0",
        "tre200d0",
        "fu3010d0",
    ]
    target_parameter = "kabetud0"
    pollen_station = "PLO"
    train_years = 9
    test_years = 1

    # -------------------------------------------------------------------------
    # 2. Load Delta tables and resolve mapped meteorological station
    # -------------------------------------------------------------------------
    spark_session = connect_to_spark()
    pollen_stations = spark_session.read.format("delta").load(
        str(DATA_DIR / "delta" / "metadata" / "pollen_stations")
    )
    pollen_historical = spark_session.read.format("delta").load(
        str(DATA_DIR / "delta" / "silver" / "pollen_historical")
    )
    meteo_historical = spark_session.read.format("delta").load(
        str(DATA_DIR / "delta" / "silver" / "meteo_historical")
    )

    station_row = (
        pollen_stations.filter(F.col("station_abbr") == pollen_station)
        .select("closest_meteo_station", "data_since")
        .first()
    )
    if (
            station_row is None
            or station_row["closest_meteo_station"] is None
            or station_row["data_since"] is None
    ):
        raise ValueError(
            f"Missing metadata (closest_meteo_station or data_since) for station '{pollen_station}'."
        )
    meteo_station = station_row["closest_meteo_station"]
    start_dt = datetime.strptime(str(station_row["data_since"]).strip(), "%d.%m.%Y %H:%M")
    start_year = start_dt.year

    # -------------------------------------------------------------------------
    # 3. Filter station series and align all available rows 1-to-1
    # -------------------------------------------------------------------------
    base_pollen = pollen_historical.filter(
        (F.col("station_abbr") == pollen_station)
    ).select("reference_timestamp", target_parameter)

    base_meteo = meteo_historical.filter(
        (F.col("station_abbr") == meteo_station)
    ).select("reference_timestamp", *meteo_parameters)

    # Inner join removes excess meteo rows where pollen targets are missing
    paired_clean_pd = (
        base_meteo.join(base_pollen, on="reference_timestamp", how="inner")
        .orderBy("reference_timestamp")
        .toPandas()
        .dropna(subset=[*meteo_parameters, target_parameter])
        .reset_index(drop=True)
    )

    if paired_clean_pd.empty:
        raise ValueError(
            f"No aligned non-null rows found for station '{pollen_station}'."
        )

    max_ts = paired_clean_pd["reference_timestamp"].max().to_pydatetime()

    # -------------------------------------------------------------------------
    # 4. Partition continuous data into 9y train / 1y test blocks (tail -> train)
    # -------------------------------------------------------------------------
    train_parts = []
    test_parts = []
    block_start_year = start_year

    while True:
        block_start_dt = datetime(block_start_year, 1, 1, 0, 0, 0)
        if block_start_dt > max_ts:
            break

        train_end_dt = datetime(block_start_year + train_years, 1, 1, 0, 0, 0)
        test_end_dt = datetime(
            block_start_year + train_years + test_years, 1, 1, 0, 0, 0
        )

        # If there is not enough timeline to reach the test period, treat entire block as train
        train_mask = (paired_clean_pd["reference_timestamp"] >= block_start_dt) & (
                paired_clean_pd["reference_timestamp"] < train_end_dt
        )
        train_parts.append(paired_clean_pd.loc[train_mask])
        if train_end_dt > max_ts:
            break

        # Add test slice if available within max_ts; otherwise push remaining rows to train
        if train_end_dt <= max_ts:
            test_mask = ((paired_clean_pd["reference_timestamp"] >= train_end_dt) &
                         (paired_clean_pd["reference_timestamp"] < test_end_dt))
            test_slice = paired_clean_pd.loc[test_mask]
            if not test_slice.empty:
                test_parts.append(test_slice)

        block_start_year += train_years + test_years

    # -------------------------------------------------------------------------
    # 5. Assemble continuous DataFrames and Series sorted by timestamp
    # -------------------------------------------------------------------------
    train_df = (
        pd.concat(train_parts, ignore_index=True)
        if train_parts
        else paired_clean_pd.iloc[0:0].copy()
    )
    test_df = (
        pd.concat(test_parts, ignore_index=True)
        if test_parts
        else paired_clean_pd.iloc[0:0].copy()
    )

    # Sort chronologically so training and testing remain continuous in time
    if not train_df.empty:
        train_df = train_df.sort_values("reference_timestamp").reset_index(drop=True)
    if not test_df.empty:
        test_df = test_df.sort_values("reference_timestamp").reset_index(drop=True)

    features_train = train_df[meteo_parameters]
    target_train = train_df[target_parameter]
    features_test = test_df[meteo_parameters]
    target_test = test_df[target_parameter]

    # -------------------------------------------------------------------------
    # 6. Return modeling splits and schema metadata and print data parameters
    # -------------------------------------------------------------------------
    print("--- Data ---")
    print(f"Training meteo parameters: {meteo_parameters}")
    print(f"Target pollen parameter: {target_parameter}")
    print(f"Pollen station: {pollen_station}")
    print(f"Data with NULLs: {base_pollen.count()} rows")
    print(f"Data without NULLs: {len(paired_clean_pd)} rows")
    print(f"Training set {len(target_train)} rows")
    print(f"Testing set {len(target_test)} rows")
    print()

    return (
        features_train,
        features_test,
        target_train,
        target_test,
        meteo_parameters,
        target_parameter,
    )