# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
from pathlib import Path
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from data_downloader import DataDownloader
from jwm_data_downloader import JwmDataDownloader

load_dotenv()
# Configuration
FUNCTION_APP_URL = os.environ.get("MC_FUNCTION_APP_URL")
FUNCTION_CODE = os.environ.get("MC_FUNCTION_CODE")
downloader = DataDownloader(FUNCTION_APP_URL, FUNCTION_CODE)


USERNAME = os.environ.get("JWM_USERNAME")
PASSWORD = os.environ.get("JWM_PASSWORD")
downloader_jwm = JwmDataDownloader(username=USERNAME, password=PASSWORD)


# %% [markdown]
# ### Functions to convert to UTC


# %%
### function to add UTC timestamps based on date and hour columns for hourly data
def add_utc_25(
    df: pd.DataFrame,
    date_col: str = "date",
    hour_col: str = "hour",
    tz: str = "Europe/Warsaw",
    out_col: str = "Date_utc",
    local_col: str = "Date_cet",
) -> pd.DataFrame:
    """
    Mapuje (data, godzina 0..22/23/24) -> lokalny timestamp w tz (start godziny),
    a następnie konwertuje do UTC. Unika duplikatów w marcu i poprawnie rozróżnia
    podwójną 02:00 w październiku.
    """
    out = df.copy()
    # Klucz do łączenia — północ danego dnia (bez strefy)
    out["_date_key"] = pd.to_datetime(out[date_col]).dt.normalize()
    out[hour_col] = out[hour_col].astype(int)
    maps = []
    for d in out["_date_key"].dropna().unique():
        # północ lokalna na początku i końcu doby
        start = pd.Timestamp(d).tz_localize(tz)
        end = (pd.Timestamp(d) + pd.Timedelta(days=1)).tz_localize(tz)
        # ciąg godzin tej doby w lokalnej strefie (długość 23/24/25)
        rng = pd.date_range(start, end, freq="h", inclusive="left")
        maps.append(
            pd.DataFrame(
                {
                    "_date_key": d,
                    hour_col: np.arange(0, len(rng), dtype=int),  # zmienione z 1 na 0
                    local_col: rng,
                }
            )
        )
    mapping = (
        pd.concat(maps, ignore_index=True)
        if maps
        else pd.DataFrame(columns=["_date_key", hour_col, local_col])
    )
    # Dołączamy lokalny czas; nieistniejące kombinacje dostaną NaT
    out = out.merge(mapping, on=["_date_key", hour_col], how="left")
    # Konwersja do UTC
    out[out_col] = out[local_col].dt.tz_convert("UTC")
    # Porządki
    out.drop(columns=["_date_key"], inplace=True)
    return out


# %% [markdown]
# # Peak Hours

# %% [markdown]
# ## MC Base

# %%
# Load peak hours data from MC base
ph_mc = downloader.get_csv_as_dataframe("power", "godziny_szczytu.csv")
# create date columns
ph_mc["date"] = ph_mc["date_time"].astype(str).str.split(" ", expand=True)[0]
ph_mc["date"] = pd.to_datetime(ph_mc["date"], format="%Y-%m-%d")
# crate column witch give numbers from 0 to 23/24/25 gruping on date
ph_mc["hour_idx"] = ph_mc.groupby(["date", "data_publikacji"]).cumcount()
# Teraz użyj hour_idx jako hour_col
ph_mc = add_utc_25(
    ph_mc,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# hour column
ph_mc["hour"] = ph_mc["Date_cet"].dt.hour
# # create Date_of_publication_cet
ph_mc["Date_of_publication_cet"] = pd.to_datetime(
    ph_mc["data_publikacji"].str[:16], format="%Y-%m-%d %H:%M"
).dt.tz_localize("Europe/Warsaw", ambiguous="infer", nonexistent="shift_forward")
# # drop unnecessary columns
ph_mc = ph_mc.drop(
    columns=[
        "date",
        "hour_idx",
        "date_time",
        "data_publikacji",
        "bussiness_date",
        "zapotrzebowanie",
    ]
)
# drop duplicates
ph_mc.drop_duplicates(subset=["Date_utc"], inplace=True)
# rename columns
ph_mc = ph_mc.rename(
    columns={
        "godzina_szczytu": "usage_forecast",
    }
)
# choose data range
ph_mc = ph_mc[ph_mc["Date_cet"] < "2025-08-30 00:00:00+02:00"].copy()

# %% [markdown]
# ## JWM Base

# %%
# Load peak hours data from JWM base
ph_jwm = downloader_jwm.download_as_dataframe("utc/peak_hours.csv")
# drop columns
ph_jwm.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
ph_jwm = ph_jwm.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# to datetime
ph_jwm["Date_utc"] = pd.to_datetime(ph_jwm["Date_utc"])
ph_jwm["Date_of_publication_utc"] = pd.to_datetime(ph_jwm["Date_of_publication_utc"])
ph_jwm["Date_of_update_utc"] = pd.to_datetime(ph_jwm["Date_of_update_utc"])
# CET
ph_jwm["Date_of_publication_cet"] = ph_jwm["Date_of_publication_utc"].dt.tz_convert(
    "Europe/Warsaw"
)
ph_jwm["Date_cet"] = ph_jwm["Date_utc"].dt.tz_convert("Europe/Warsaw")
ph_jwm["Date_of_update_cet"] = ph_jwm["Date_of_update_utc"].dt.tz_convert(
    "Europe/Warsaw"
)
# hour column
ph_jwm["hour"] = ph_jwm["Date_cet"].dt.hour


# %% [markdown]
# ## Join datasets

# %%
# Join datasets
ph = pd.concat([ph_mc, ph_jwm], ignore_index=True)

# Replace values in usage_forecast - using assignment
ph["usage_forecast"] = ph["usage_forecast"].replace(
    {
        "ZALECANE_UZYTKOWANIE": "RECOMMENDED_USAGE",
        "ZALECANE_OSZCZEDZANIE": "RECOMMENDED_SAVING",
        "NORMALNE_UZYTKOWANIE": "NORMAL_USAGE",
        "WYMAGANE_OGRANICZANIE": "USAGE_LIMIT_REQUIRED",
    }
)
peak_hours_map = {
    0: "RECOMMENDED_USAGE",
    1: "NORMAL_USAGE",
    2: "RECOMMENDED_SAVING",
    3: "USAGE_LIMIT_REQUIRED",
}
peak_hours_mapping = {v: k for k, v in peak_hours_map.items()}
ph["peak_hours_actual"] = ph["usage_forecast"].map(peak_hours_mapping)
# fill na
ph.loc[ph["Date_of_publication_utc"].isna(), "Date_of_publication_utc"] = (
    pd.to_datetime(ph["Date_utc"] - pd.to_timedelta("1 day")).dt.normalize()
    + pd.to_timedelta(23, unit="h")
    + pd.to_timedelta(59, unit="m")
)
ph.loc[ph["Date_of_update_utc"].isna(), "Date_of_update_utc"] = ph[
    "Date_of_publication_utc"
]
ph["Date_of_update_cet"] = ph["Date_of_update_utc"].dt.tz_convert("Europe/Warsaw")

# %%
out_path = Path(__file__).parent / "../out"
ph.to_parquet(out_path / "peak_hours_actual.parquet")
