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
### for 15min data
def add_utc_25_15min(
    df: pd.DataFrame,
    date_col: str = "date",
    hour_index_col: str = "hour_index",
    tz: str = "Europe/Warsaw",
    out_col: str = "Date_utc",
    local_col: str = "Date_cet",
) -> pd.DataFrame:
    """
    Mapuje (data, indeks godziny 0..91/95/99) -> lokalny timestamp w tz (start kwadransa),
    a następnie konwertuje do UTC. Unika duplikatów w marcu i poprawnie rozróżnia
    podwójną 02:00 w październiku.
    """
    out = df.copy()
    # Klucz do łączenia — północ danego dnia (bez strefy)
    out["_date_key"] = pd.to_datetime(out[date_col]).dt.normalize()
    out[hour_index_col] = out[hour_index_col].astype(int)

    maps = []
    for d in out["_date_key"].dropna().unique():
        # północ lokalna na początku i końcu doby
        start = pd.Timestamp(d).tz_localize(tz)
        end = (pd.Timestamp(d) + pd.Timedelta(days=1)).tz_localize(tz)
        # ciąg kwadransów tej doby w lokalnej strefie (długość 92/96/100)
        rng = pd.date_range(start, end, freq="15min", inclusive="left")
        maps.append(
            pd.DataFrame(
                {
                    "_date_key": d,
                    hour_index_col: np.arange(0, len(rng), dtype=int),
                    local_col: rng,
                }
            )
        )
    mapping = (
        pd.concat(maps, ignore_index=True)
        if maps
        else pd.DataFrame(columns=["_date_key", hour_index_col, local_col])
    )
    # Dołączamy lokalny czas; nieistniejące kombinacje dostaną NaT
    out = out.merge(mapping, on=["_date_key", hour_index_col], how="left")
    # Konwersja do UTC
    out[out_col] = out[local_col].dt.tz_convert("UTC")
    # Porządki
    out.drop(columns=["_date_key"], inplace=True)
    return out


# %% [markdown]
# # KSE Load - prognoza i faktyczne zapotrzebowanie

# %% [markdown]
# ## MC Base

# %%
# get demand pse
mc_kseload_f = downloader.get_csv_as_dataframe(
    "power_live", "pse_prognozowane_zapotrzebowanie.csv"
)
# date column
mc_kseload_f["date"] = pd.to_datetime(mc_kseload_f["date"])
# hour index
mc_kseload_f["hour_idx"] = mc_kseload_f.groupby(["date"]).cumcount()
# minute index
mc_kseload_f["minute"] = (
    mc_kseload_f["time"].str.split(" ").str[0].str.split(":").str[1].astype(float)
)
# add UTC timestamps
mc_kseload_f = add_utc_25_15min(
    mc_kseload_f,
    date_col="date",
    hour_index_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# chouse and rename columns
mc_kseload_f = mc_kseload_f[
    ["data_publikacji", "Date_utc", "Date_cet", "demand_forcast"]
]
mc_kseload_f = mc_kseload_f.rename(
    columns={
        "data_publikacji": "Date_of_publication_cet",
        "demand_forcast": "load_forecast",
    }
)
# set timezone
mc_kseload_f["Date_of_publication_cet"] = pd.to_datetime(
    mc_kseload_f["Date_of_publication_cet"]
).dt.tz_localize("Europe/Warsaw")
mc_kseload_f["Date_of_publication_utc"] = mc_kseload_f[
    "Date_of_publication_cet"
].dt.tz_convert("UTC")
# choose needed date range
mc_kseload_f = mc_kseload_f[
    mc_kseload_f["Date_cet"] < "2025-08-30 00:00:00+02:00"
].copy()

# %% [markdown]
# ## JWM Base

# %% [markdown]
# ### JWM actual eod

# %%
# # download data
# jwm_kseload_actuals = downloader_jwm.download_as_dataframe("utc/kse_load_actual_eod.csv")
# # drop irrelevant columns
# jwm_kseload_actuals = jwm_kseload_actuals.drop(columns=['plan_day','plan_indicator','delivery_end'])
# # rename columns
# jwm_kseload_actuals = jwm_kseload_actuals.rename(columns={
#     'delivery_start': 'Date_utc',
#     'publication_timestamp': 'Date_of_publication_utc'})
# # to datetime
# jwm_kseload_actuals['Date_utc'] = pd.to_datetime(jwm_kseload_actuals['Date_utc'])
# jwm_kseload_actuals['Date_of_publication_utc'] = pd.to_datetime(jwm_kseload_actuals['Date_of_publication_utc'])
# # dates in pl timezone
# jwm_kseload_actuals['Date_cet'] = jwm_kseload_actuals['Date_utc'].dt.tz_convert('Europe/Warsaw')
# jwm_kseload_actuals['Date_of_publication_pl'] = jwm_kseload_actuals['Date_of_publication_utc'].dt.tz_convert('Europe/Warsaw')
# # rename columns
# jwm_kseload_actuals.rename(columns={'load_forecast': 'load_forecast_late'}, inplace=True)


# %% [markdown]
# ### JWM forecast

# %%
# download data
jwm_kseload_forecast = downloader_jwm.download_as_dataframe("utc/kse_load_forecast.csv")
# drop irrelevant columns
jwm_kseload_forecast = jwm_kseload_forecast.drop(
    columns=["delivery_end", "timeseries_plan_indicator"]
)
# rename columns
jwm_kseload_forecast = jwm_kseload_forecast.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# to datetime
jwm_kseload_forecast["Date_utc"] = pd.to_datetime(jwm_kseload_forecast["Date_utc"])
jwm_kseload_forecast["Date_of_publication_utc"] = pd.to_datetime(
    jwm_kseload_forecast["Date_of_publication_utc"]
)
jwm_kseload_forecast["Date_of_update_utc"] = pd.to_datetime(
    jwm_kseload_forecast["Date_of_update_utc"]
)
# dates in pl timezone
jwm_kseload_forecast["Date_cet"] = jwm_kseload_forecast["Date_utc"].dt.tz_convert(
    "Europe/Warsaw"
)
jwm_kseload_forecast["Date_of_publication_cet"] = jwm_kseload_forecast[
    "Date_of_publication_utc"
].dt.tz_convert("Europe/Warsaw")
jwm_kseload_forecast["Date_of_update_cet"] = jwm_kseload_forecast[
    "Date_of_update_utc"
].dt.tz_convert("Europe/Warsaw")

# %% [markdown]
# ### JWM join

# %%
# # merg
# kse_load = jwm_kseload_forecast.merge(jwm_kseload_actuals[['Date_utc','Date_cet','load_actual','load_forecast_late']],
#                                       on=['Date_utc','Date_cet'], how='outer')

# %% [markdown]
# ## Join MC JWM
#

# %%
# merge
kse_load = pd.concat([mc_kseload_f, jwm_kseload_forecast])
# sort columns
kse_load = kse_load.reindex(sorted(jwm_kseload_forecast.columns), axis=1)
# drop data with same publication date and date_utc
kse_load = kse_load.drop_duplicates(
    subset=["Date_of_publication_utc", "Date_utc"], keep="last"
)

# %% [markdown]
# # save to parquet

# %%
# save to parquet

out_path = Path(__file__).parent / "../out"
kse_load.to_parquet(out_path / "kse_load_forecast.parquet", index=False)
