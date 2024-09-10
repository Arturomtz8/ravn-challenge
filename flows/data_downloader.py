import logging
import re
from datetime import datetime
from io import BytesIO, StringIO

import pandas as pd
import polars as pl
import requests
from bq_uploader import update_bq_table
from prefect import task


@task(log_prints=True)
def download_csv_by_year(url, year, filename):
    response = requests.get(url)

    if response.status_code == 200:
        csv_data = StringIO(response.content.decode("utf-8"))
        # merrion square file contains different format date and more rows
        if filename == "merrion_square_weather":
            df = pd.read_csv(csv_data, skiprows=12)
            df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y", errors="coerce")
            df_by_year = df[df["date"].dt.year == year]
        else:
            df = pd.read_csv(csv_data)
            df_by_year = df[df["Year"] == year]
            df_by_year.drop(columns=["TLIST(A1)"], inplace=True)

        df_by_year.columns = [col.replace(".", "_") for col in df_by_year.columns]
        logging.info(f"Everything ran smoothly with {url} and {year}")
        return df_by_year

    else:
        logging.error(
            f"Failed to download the file {filename}. Status code: {response.status_code}"
        )


@task(log_prints=True)
def download_three_previous_months_bikes_stations():
    now = datetime.now()

    def get_month_bounds(date, offset=0):
        start_date = (date - pd.DateOffset(months=offset)).replace(day=1)
        end_date = start_date.replace(
            month=start_date.month % 12 + 1, day=1
        ) - pd.Timedelta(days=1)
        return start_date, end_date

    month_bounds = [get_month_bounds(now, offset) for offset in range(3)]
    combined_df = pl.DataFrame()
    for i, bounds in enumerate(month_bounds):
        start_date = bounds[0].strftime("%Y-%m-%d")
        end_date = bounds[1].strftime("%Y-%m-%d")
        df = pl.read_csv(
            f"https://data.smartdublin.ie/dublinbikes-api/bikes/dublin_bikes/historical/stations.csv?dt_start={start_date}&dt_end={end_date}"
        )
        combined_df = pl.concat([combined_df, df])
        print(f"combined and going in {i} range")

    return combined_df


@task(log_prints=True)
def download_cycles_count(url):
    try:
        df = pd.read_excel(url)

        def clean_column_names(column_name):
            cleaned_name = re.sub(r"\s*\([^)]*\)", "", column_name)
            return cleaned_name

        df.columns = [clean_column_names(col) for col in df.columns]
        df = df.groupby(df.columns, axis=1).sum()
        df.columns = [col.replace("/", "_") for col in df.columns]
        return df
    except Exception as e:
        print(f"Exception occurred {e}")
