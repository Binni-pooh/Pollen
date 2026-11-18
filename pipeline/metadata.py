import pandas as pd
from pathlib import Path
from scipy.spatial import cKDTree

from config import DATA_DIR
from data_loader import load_or_download_csv
from pipeline.connect_to_spark import connect_to_spark

def save_metadata():
    parameters = relevant_parameters()
    pollen_stations, phenology_stations, meteo_stations = relevant_stations()
    datainventory = relevant_datainventory()

    spark_session = connect_to_spark()

    base_delta_path = DATA_DIR / "delta" / "metadata"

    # Save each DataFrame to the robust absolute path
    spark_session.createDataFrame(parameters).write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(base_delta_path / "parameters"))

    spark_session.createDataFrame(pollen_stations).write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(base_delta_path / "pollen_stations"))

    spark_session.createDataFrame(phenology_stations).write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(base_delta_path / "phenology_stations"))

    spark_session.createDataFrame(meteo_stations).write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(base_delta_path / "meteo_stations"))

    # spark_session.createDataFrame(datainventory).write \
    #     .format("delta") \
    #     .mode("overwrite") \
    #     .save(str(base_delta_path / "datainventory"))

def relevant_parameters():
    save_dir = str(DATA_DIR / "meta")
    # Selecting relevant pollen parameters
    pollen_parameters = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-pollen/",
                                         "ogd-pollen_meta_parameters.csv",
                                         save_dir)
    relevant_mask = pollen_parameters["parameter_shortname"].str.contains(r'1$|y\d+$', regex=True, na=False)
    relevant_pollen_parameters = pollen_parameters[~relevant_mask]

    pollen_genera = relevant_pollen_parameters['parameter_description_en'].str.extract(r'/\s*([A-Za-z]+)', expand=False)
    unique_pollen_genera = pollen_genera.dropna().unique()

    # Selecting relevant phenology parameters
    phenology_parameters = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-phenology/",
                                            "ogd-phenology_meta_parameters.csv",
                                            save_dir)
    phenology_genera = phenology_parameters['parameter_description_en'].str.extract(r'/\s*([A-Za-z]+)', expand=False)
    relevant_phenology_parameters = phenology_parameters[phenology_genera.isin(unique_pollen_genera)]

    # Selecting relevant meteorological parameters
    meteo_parameters = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/",
                                        "ogd-smn_meta_parameters.csv",
                                        save_dir)
    relevant_mask = meteo_parameters["parameter_shortname"].str.contains(r'[my]\S?$', regex=True, na=False)

    relevant_meteo_parameters = meteo_parameters[~relevant_mask]

    #Concatenating and cleaning parameters
    raw_relevant_parameters = pd.concat(
        [relevant_pollen_parameters, relevant_phenology_parameters, relevant_meteo_parameters],
        ignore_index=True
    )

    # columns_to_drop = ["parameter_description_de", "parameter_description_fr", "parameter_description_it",
    #                    "parameter_group_de", "parameter_group_fr", "parameter_group_it",]
    columns_to_drop = raw_relevant_parameters.columns[
        raw_relevant_parameters.columns.str.contains(r'it$|fr$|de$', regex=True, na=False)
    ]
    clean_relevant_parameters = raw_relevant_parameters.drop(columns=columns_to_drop, errors='ignore')
    return clean_relevant_parameters.to_dict(orient="records")


def relevant_stations():
    save_dir = str(DATA_DIR / "meta")
    pollen_stations = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-pollen/",
                                           "ogd-pollen_meta_stations.csv",
                                           save_dir)
    phenology_stations = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-phenology/",
                                              "ogd-phenology_meta_stations.csv",
                                              save_dir)
    meteo_stations = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/",
                                          "ogd-smn_meta_stations.csv",
                                          save_dir)
    pollen_datainventory = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-pollen/",
                                                "ogd-pollen_meta_datainventory.csv",
                                                save_dir)

    # Add the closest stations
    pollen_stations = nearest_stations(pollen_stations, phenology_stations, "phenology")
    pollen_stations = nearest_stations(pollen_stations, meteo_stations, "meteo")

    phenology_stations = phenology_stations[
        phenology_stations["station_abbr"].isin(pollen_stations["closest_phenology_station"])
    ]
    meteo_stations = meteo_stations[
        meteo_stations["station_abbr"].isin(pollen_stations["closest_meteo_station"])
    ]

    # Add the start measurements date
    earliest_map = (
        pollen_datainventory.sort_values("data_since")
        .drop_duplicates(subset=["station_abbr"], keep="first")
        .set_index("station_abbr")["data_since"]
    )

    pollen_stations["data_since"] = pollen_stations["station_abbr"].map(earliest_map)

    # Drop irrelevant columns
    columns_to_drop = pollen_stations.columns[pollen_stations.columns.str.contains(r'_(it|fr|de)$', regex=True, na=False)]

    clean_pollen_stations = pollen_stations.drop(columns=columns_to_drop).to_dict(orient="records")
    clean_phenology_stations = phenology_stations.drop(columns=columns_to_drop).to_dict(orient="records")
    clean_meteo_stations = meteo_stations.drop(columns=columns_to_drop).to_dict(orient="records")
    
    return clean_pollen_stations, clean_phenology_stations, clean_meteo_stations

def relevant_datainventory():
    save_dir = str(DATA_DIR / "meta")
    pollen_datainventory = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-pollen/",
                                           "ogd-pollen_meta_datainventory.csv",
                                           save_dir)
    # phenology_datainventory = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-phenology/",
    #                                           "ogd-phenology_meta_datainventory.csv",
    #                                           save_dir)
    # meteo_datainventory = load_or_download_csv("https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/",
    #                                       "ogd-smn_meta_datainventory.csv",
    #                                       save_dir)

    earliest_inventory = (
        pollen_datainventory
        .sort_values("data_since")
        .drop_duplicates(subset=["station_abbr"], keep="first")
    )

    return earliest_inventory.to_dict(orient="records")

def nearest_stations(target_stations_df: pd.DataFrame, search_stations_df: pd.DataFrame, station_type = ""):
    """
    Searches for nearest station for each of target station.
    :param target_stations_df: dataframe of stations for which we want to find nearest stations
    :param search_stations_df: dataframe of stations among which we want to find nearest stations
    :param station_type: station type to insert into column name
    :return:
    """
    target_stations_df = target_stations_df.copy()

    target_coords = target_stations_df[["station_coordinates_lv95_east", "station_coordinates_lv95_north"]].values
    search_coords = search_stations_df[["station_coordinates_lv95_east", "station_coordinates_lv95_north"]].values

    search_tree = cKDTree(search_coords)
    distances, indices = search_tree.query(target_coords)

    target_stations_df[f"closest_{station_type}_station"] = search_stations_df.iloc[indices]["station_abbr"].values
    target_stations_df[f"{station_type}_distance_km"] = distances / 1000

    return target_stations_df

if __name__ == "__main__":
    save_metadata()