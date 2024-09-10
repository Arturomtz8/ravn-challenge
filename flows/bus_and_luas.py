from bq_uploader import update_bq_table
from data_downloader import (
    download_csv_by_year,
    download_cycles_count,
    download_three_previous_months_bikes_stations,
)
from prefect import flow


@flow()
def get_bus_data():
    df = download_csv_by_year(
        "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/TOA14/CSV/1.0/en",
        2023,
        "bus",
    )
    update_bq_table("dublin_data.bus", df)


@flow()
def get_luas_data():
    df = download_csv_by_year(
        "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/TOA11/CSV/1.0/en",
        2023,
        "luas",
    )
    update_bq_table("dublin_data.luas", df)


@flow()
def get_merrion_square_weather_data():
    df = download_csv_by_year(
        "https://cli.fusio.net/cli/climate_data/webdata/dly3923.csv",
        2023,
        "merrion_square_weather",
    )
    update_bq_table("dublin_data.merrion_square_weather", df)


@flow()
def get_bikes_station_data():
    df = download_three_previous_months_bikes_stations()
    update_bq_table("dublin_data.bikes_station", df)


@flow()
def get_cycle_counts_data():
    df = download_cycles_count(
        "https://data.smartdublin.ie/dataset/d26ce6c0-2e1c-4b72-8fbd-cb9f9cbbc118/resource/d5c64b9a-27af-411f-81b1-35b3add5a279/download/cycle-counts-1-jan-31-december-2023.xlsx",
    )
    update_bq_table("dublin_data.cycle_counts", df)


if __name__ == "__main__":
    get_bus_data()
    get_luas_data()
    get_merrion_square_weather_data()
    get_bikes_station_data()
    get_cycle_counts_data()
