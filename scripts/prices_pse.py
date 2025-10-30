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
### function to add UTC timestamps based on date and hour columns
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
# # Rynek bilansujący

# %% [markdown]
# ## RB MC

# %% [markdown]
# ### RB MC history

# %%
# get rb_mc_his
rb_mc_his = downloader.get_csv_as_dataframe(
    "power", "pse_ceny_rozliczeniowe_2013-2024.csv"
)
# rename columns
rb_mc_his.rename(columns={"Godzina": "hour", "Data": "date"}, inplace=True)
# replece , with . in all columns
rb_mc_his = rb_mc_his.replace(",", ".", regex=True)
# rb_mceate date column
rb_mc_his["date"] = pd.to_datetime(rb_mc_his["date"].astype(str), format="%Y%m%d")
rb_mc_his["bilans_price"] = rb_mc_his["CRO"]
rb_mc_his["bilans_price"] = rb_mc_his["bilans_price"].astype(float)
# hour index
rb_mc_his["hour_idx"] = rb_mc_his.groupby("date").cumcount()
## Append UTC timestamps
rb_mc_his = add_utc_25(
    rb_mc_his,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# add last row
last = rb_mc_his.iloc[[-1]].copy()
last["Date_utc"] = last["Date_utc"] + pd.Timedelta(hours=1)
rb_mc_his = pd.concat([rb_mc_his, last], ignore_index=True)
### resample to 15 min
rb_mc_his_15 = rb_mc_his.set_index("Date_utc").resample("15min").ffill().reset_index()
# cet time
rb_mc_his_15["Date_cet"] = rb_mc_his_15["Date_utc"].dt.tz_convert("Europe/Warsaw")
# chouse relevant columns
rb_mc_his_15 = rb_mc_his_15[["Date_utc", "Date_cet", "bilans_price"]]
# cut last row
rb_mc_his_15 = rb_mc_his_15[:-1].copy()

# %% [markdown]
# ### RB MC new

# %%
# get cr_new
rb_mc_new = downloader.get_csv_as_dataframe("power", "pse_ceny_rozliczeniowe.csv")
# replece , with . in all columns
rb_mc_new = rb_mc_new.replace(",", ".", regex=True)
# rename columns
rb_mc_new.rename(columns={"doba": "date"}, inplace=True)
# hour index
rb_mc_new["hour_idx"] = rb_mc_new.groupby("date").cumcount()
## Append UTC timestamps
rb_mc_new = add_utc_25_15min(
    rb_mc_new,
    date_col="date",
    hour_index_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# rename columns
rb_mc_new.rename(
    columns={"cen_rozl": "bilans_price", "source_datetime": "Date_of_publication_cet"},
    inplace=True,
)
# chouse relevant columns
rb_mc_new = rb_mc_new[
    ["Date_utc", "Date_cet", "bilans_price", "Date_of_publication_cet"]
]

# %% [markdown]
# ### RB MC join

# %%
# join cr_his and rb_mc
rb_mc = pd.concat([rb_mc_his_15, rb_mc_new])
# chouse time period before 2024-06-14
rb_mc = rb_mc[rb_mc["Date_cet"] < "2024-06-14"].copy()
# utc time
rb_mc["Date_of_publication_cet"] = pd.to_datetime(
    rb_mc["Date_of_publication_cet"]
).dt.tz_localize("Europe/Warsaw")
rb_mc["Date_of_publication_utc"] = rb_mc["Date_of_publication_cet"].dt.tz_convert("UTC")

# %% [markdown]
# ## RB JWM

# %%
# get rb_jwm
rb_jwm = downloader_jwm.download_as_dataframe("utc/regulation_prices.csv")
# drop irrelevant columns
rb_jwm = rb_jwm.drop(columns=["Delivery end", "Type", "Date"])
# rename columns
rb_jwm = rb_jwm.rename(
    columns={
        "Delivery start": "Date_utc",
        "Publication timestamp": "Date_of_publication_utc",
        "Price": "bilans_price",
    }
)
# to datetime
rb_jwm["Date_utc"] = pd.to_datetime(rb_jwm["Date_utc"])
rb_jwm["Date_of_publication_utc"] = pd.to_datetime(rb_jwm["Date_of_publication_utc"])
# CET time
rb_jwm["Date_cet"] = rb_jwm["Date_utc"].dt.tz_convert("Europe/Warsaw")
rb_jwm["Date_of_publication_cet"] = rb_jwm["Date_of_publication_utc"].dt.tz_convert(
    "Europe/Warsaw"
)

# %% [markdown]
# ## RB MC join

# %%
# join rb_mc and rb_jwm
rb = pd.concat([rb_mc, rb_jwm], ignore_index=True)

# %% [markdown]
# # Fix1Fix2

# %% [markdown]
# ## MC Fix1Fix2

# %% [markdown]
# ### MC Fix1Fix2 history

# %%
# get fix_his
fix_mc_his = downloader.get_csv_as_dataframe(
    "power", "electricity_prices_day_ahead_hourly_all.csv"
)
# Convert the date column to datetime
fix_mc_his["date"] = pd.to_datetime(fix_mc_his["date"], dayfirst=True).dt.date
# hour index
fix_mc_his["hour_idx"] = fix_mc_his.groupby("date").cumcount()
## Append UTC timestamps
fix_mc_his = add_utc_25(
    fix_mc_his,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# rename columns
fix_mc_his.rename(
    columns={
        "fixing_i_price": "fixing1_price",
        "fixing_ii_price": "fixing2_price",
        "fixing_i_volume": "fixing1_volume",
        "fixing_ii_volume": "fixing2_volume",
    },
    inplace=True,
)
# drop irrelevant columns
fix_mc_his = fix_mc_his.drop(columns=["date", "hour_idx"])
# chouse data before 2024-06-14
fix_mc_his = fix_mc_his[
    pd.to_datetime(fix_mc_his["Date_cet"].dt.date) < "2024-11-15"
].copy()

# %% [markdown]
# ### MC Fix1Fix2 new

# %%
# get fix_new
fix_mc_new = downloader.get_csv_as_dataframe("power", "tge_energy.csv")
# date column to datetime
fix_mc_new["date"] = pd.to_datetime(fix_mc_new["date"], dayfirst=True).dt.date
# hour index
fix_mc_new["hour_idx"] = fix_mc_new.groupby("date").cumcount()
# Append UTC timestamps
fix_mc_new = add_utc_25(
    fix_mc_new,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# drop irrelevant columns
fix_mc_new = fix_mc_new.drop(
    columns=["date", "hour_idx", "time", "continuous_price", "continuous_volume"]
)
# replace ',' with '' and change to float
fix_mc_new["fixing1_price"] = (
    fix_mc_new["fixing1_price"].str.replace(" ", "").astype(float)
)
fix_mc_new["fixing2_price"] = (
    fix_mc_new["fixing2_price"].str.replace(" ", "").astype(float)
)
fix_mc_new["fixing1_volume"] = (
    fix_mc_new["fixing1_volume"].str.replace(" ", "").astype(float)
)
fix_mc_new["fixing2_volume"] = (
    fix_mc_new["fixing2_volume"].str.replace(" ", "").astype(float)
)

# %% [markdown]
# ### MC Fix1Fix2 join

# %%
# join
fix_mc = pd.concat([fix_mc_his, fix_mc_new])

# %% [markdown]
# ## JWM Fix1Fix2

# %% [markdown]
# ### JWM FixFix2 history

# %%
### FIX1
# download fix1_jwm_his
fix1_jwm_his = downloader_jwm.download_as_dataframe("utc/tge_fix_1_before_2025.csv")
# drop duplicates based on all columns
fix1_jwm_his.drop_duplicates(
    subset=fix1_jwm_his.columns.tolist(), keep="first", inplace=True
)
# rename columns
fix1_jwm_his.rename(
    columns={
        "Price": "fixing1_price",
        "Delivery start": "Date_utc",
        "Volume": "fixing1_volume",
    },
    inplace=True,
)
# Date_utc to datetime
fix1_jwm_his["Date_utc"] = pd.to_datetime(fix1_jwm_his["Date_utc"])
# CET time
fix1_jwm_his["Date_cet"] = fix1_jwm_his["Date_utc"].dt.tz_convert("Europe/Warsaw")
# choose relevant columns
fix1_jwm_his = fix1_jwm_his[["Date_utc", "Date_cet", "fixing1_price", "fixing1_volume"]]
### FIX2
# download fix2_jwm_his
fix2_jwm_his = downloader_jwm.download_as_dataframe("utc/tge_fix_2_before_2025.csv")
# drop duplicates based on all columns
fix2_jwm_his.drop_duplicates(
    subset=fix2_jwm_his.columns.tolist(), keep="first", inplace=True
)
# rename columns
fix2_jwm_his.rename(
    columns={
        "Price": "fixing2_price",
        "Delivery start": "Date_utc",
        "Volume": "fixing2_volume",
    },
    inplace=True,
)
# Date_utc to datetime
fix2_jwm_his["Date_utc"] = pd.to_datetime(fix2_jwm_his["Date_utc"])
# CET time
fix2_jwm_his["Date_cet"] = fix2_jwm_his["Date_utc"].dt.tz_convert("Europe/Warsaw")
# choose relevant columns
fix2_jwm_his = fix2_jwm_his[["Date_utc", "Date_cet", "fixing2_price", "fixing2_volume"]]
# Join
fix_jwm_his = (
    fix1_jwm_his.set_index("Date_utc")
    .join(
        fix2_jwm_his[["Date_utc", "fixing2_price", "fixing2_volume"]].set_index(
            "Date_utc"
        )
    )
    .reset_index()
)

# %%
# show duplicates in Date_utc
duplicates = fix2_jwm_his[fix2_jwm_his.duplicated(subset=["Date_utc"], keep=False)]
print("Duplicates in Date_utc:")
duplicates

# %% [markdown]
# ### JWM FixFix2 new

# %%
### FIX1
# download fix1_jwm_new
fix1_jwm_new = downloader_jwm.download_as_dataframe("utc/tge_fix_1.csv")
# drop duplicates based on all columns
fix1_jwm_new.drop_duplicates(
    subset=fix1_jwm_new.columns.tolist(), keep="first", inplace=True
)
# rename columns
fix1_jwm_new.rename(
    columns={"Price": "fixing1_price", "Delivery start": "Date_utc"}, inplace=True
)
# ### FIX2
# download fix2_jwm_new
fix2_jwm_new = downloader_jwm.download_as_dataframe("utc/tge_fix_2.csv")
# drop duplicates based on all columns
fix2_jwm_new.drop_duplicates(
    subset=fix2_jwm_new.columns.tolist(), keep="first", inplace=True
)
# rename columns
fix2_jwm_new.rename(
    columns={"Price": "fixing2_price", "Delivery start": "Date_utc"}, inplace=True
)
# join
fix_jwm_new = (
    fix1_jwm_new.set_index("Date_utc")
    .join(fix2_jwm_new[["Date_utc", "fixing2_price"]].set_index("Date_utc"))
    .reset_index()
)
# CET time
fix_jwm_new["Date_utc"] = pd.to_datetime(fix_jwm_new["Date_utc"])
fix_jwm_new["Date_cet"] = fix_jwm_new["Date_utc"].dt.tz_convert("Europe/Warsaw")
# drop irrelevant columns
fix_jwm_new = fix_jwm_new.drop(columns=["Type", "Date", "Delivery end"]).copy()

# %% [markdown]
# ### JWM FixFix2 join

# %%
# join fix_mc_his and fix_mc_new
fix_jwm = pd.concat([fix_jwm_his, fix_jwm_new], ignore_index=True)

# %% [markdown]
# ## Join MC and JWM Fix1Fix2

# %%
### MC Fix1Fix2 join
# from mc choose needed date period
fix_mc = fix_mc[(fix_mc["Date_cet"] < "2019-04-02")].copy()
# join mc and jwm fix1fix2
fix = pd.concat([fix_mc, fix_jwm], ignore_index=True)

# %% [markdown]
# # save to parquet

# %%
# save to parquet
out_path = Path(__file__).parent / "../out"
rb.to_parquet(out_path / "rb_price.parquet", index=False)
fix.to_parquet(out_path / "fix_price.parquet", index=False)
