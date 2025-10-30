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


# %% [markdown]
# # KSE Wielkości podstawowe Actual

# %% [markdown]
# ## Baza MC

# %% [markdown]
# ### MC pk5y actual history

# %%
# Raporty dobowe kse his
pk_actual_his = downloader.get_csv_as_dataframe("power", "raport_dobowy_old.csv")
# drop duplicates
pk_actual_his.drop_duplicates(
    subset=[
        "Data",
        "Godzina",
        "Krajowe_zapotrzebowanie_na_moc",
        "Suma_zdolnosci_wytworczych_jednostek_wytworczych_w_KSE",
    ],
    keep="first",
    inplace=True,
)
# rename columns
pk_actual_his.rename(
    columns={
        "Krajowe_zapotrzebowanie_na_moc": "Zapotrzebowanie_na_moc_MW",
        "Generacja_zrodel_wiatrowych": "Sumaryczna_generacja_źródeł_wiatrowych",
        "Generacja_zrodel_fotowoltaicznych": "Sumaryczna_generacja_źródeł_fotowoltaicznych",
        "Sumaryczna_generacja_jednostek_wytworczych_nieuczestniczacych_aktywnie_w_Rynku_Bilansujacym": "Sumaryczna_Generacja_Jednostek_Wytwórczych_nie_uczestniczących_aktywnie_w_Rynku_Bilansującym_[MW]",
        "Krajowe_saldo_wymiany_miedzysystemowej_rownoleglej": "Krajowe_saldo_wymiany_międzysystemowej__równoległa_[MW]",
        "Krajowe_saldo_wymiany_miedzysystemowej_nierownoleglej": "Krajowe_saldo_wymiany_międzysystemowej__nierównoległa_[MW]",
        "Sumaryczna_generacja_JG_aktywnych_JGWa_JGFWa_JGMa_i_JGPVa": "Suma_generacji_jednostek_grafikowych_w_KSE_(JGw,_JGm,_JGz_i_JGa)_[MW]",
        "Sumaryczna_generacja_JGWa": "Suma_generacji_Jednostek_Grafikowych_Wytwórczych_JGw_z_ZAK=1_(JGw1)_[MW]",
        "Sumaryczna_moc_ladowania_JGMa": "Sumaryczna_moc_ładowania",
        "Sumaryczna_generacja_JGMa": "Suma_generacji_Jednostek_Grafikowych_Magazynów_JGm_z_ZAK=1_(JGm1)_[MW]",
    },
    inplace=True,
)
# change , to .
pk_actual_his = pk_actual_his.replace(",", ".", regex=True)
# date column
pk_actual_his["date"] = pd.to_datetime(pk_actual_his["Data"])
# hour index
pk_actual_his["hour_idx"] = pk_actual_his.groupby("date").cumcount()
# add UTC timestamps
pk_actual_his = add_utc_25(
    pk_actual_his,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# hour column
pk_actual_his["hour"] = pk_actual_his["Date_cet"].dt.hour
# drop not needed columns
pk_actual_his.drop(columns=["Data", "Godzina"], inplace=True)
# change all columns to float except date, hour, Date_utc, Date_cet
cols_to_float = pk_actual_his.columns.difference(
    ["date", "hour", "Date_utc", "Date_cet"]
)
pk_actual_his[cols_to_float] = pk_actual_his[cols_to_float].astype(float)

### Make df for 15 min data
pk_actual_his_15 = pk_actual_his.copy()
# create df with last observation + 1 hour
last = pk_actual_his.iloc[[-1]].copy()
last["Date_utc"] = last["Date_utc"] + pd.Timedelta(hours=1)
# concat
pk_actual_his_15 = pd.concat([pk_actual_his_15, last])
# resample to 15 min
pk_actual_his_15 = (
    pk_actual_his_15.set_index(["Date_utc"]).resample("15min").mean().ffill()
)
# drop last row
pk_actual_his_15 = pk_actual_his_15.iloc[:-1].copy().reset_index()
### update cet columns
pk_actual_his_15["Date_cet"] = pk_actual_his_15["Date_utc"].dt.tz_convert(
    "Europe/Warsaw"
)

# %% [markdown]
# ### MC pk5y actual new

# %%
# raporty dobowe kse new
pk_actual_new = downloader.get_csv_as_dataframe("power", "raport_dobowy_kse.csv")
# date column
pk_actual_new["date"] = pd.to_datetime(pk_actual_new["Doba_handlowa"])
# hour index
pk_actual_new["hour_idx"] = pk_actual_new.groupby(["date"]).cumcount()
# minute index
pk_actual_new["minute"] = (
    pk_actual_new["ORED_Jednostka_czasu_od-do"]
    .str.split(" ")
    .str[0]
    .str.split(":")
    .str[1]
    .astype(float)
)
# add UTC timestamps
pk_actual_new = add_utc_25_15min(
    pk_actual_new,
    date_col="date",
    hour_index_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)

# %% [markdown]
# ### MC join histroy and new

# %%
# join cr_his and cr_new
pk_actual = pd.concat([pk_actual_his_15, pk_actual_new])
pk_actual["date"] = pd.to_datetime(pk_actual["date"], dayfirst=True)
pk_actual.drop(columns=["Doba_(udtczas)", "ORED_Jednostka_czasu_od-do"], inplace=True)

### Columns new names
rename_columns_pl_to_en = {
    "Data_publikacji": "Date_of_publication_cet",
    "Zapotrzebowanie_na_moc_MW": "domestic_power_demand",
    "Suma_generacji_Jednostek_Grafikowych_Agregatów_JGa_(JGa1)_[MW]": "generation_jga",
    "Suma_generacji_Jednostek_Grafikowych_Magazynów_JGm_z_ZAK=1_(JGm1)_[MW]": "generation_jgm1",
    "Suma_generacji_Jednostek_Grafikowych_Magazynów_JGm_z_ZAK=2_(JGm2)_[MW]": "generation_jgm2",
    "Sumaryczna_Generacja_Jednostek_Wytwórczych_nie_uczestniczących_aktywnie_w_Rynku_Bilansującym_[MW]": "generation_jgna",
    "Jednostka_grafikowa_odbiorcza": "generation_jgo",
    "Suma_generacji_Jednostek_Grafikowych_Wytwórczych_JGw_z_ZAK=1_(JGw1)_[MW]": "generation_jgw1",
    "Suma_generacji_Jednostek_Grafikowych_Wytwórczych_JGw_z_ZAK=2_(JGw2)_[MW]": "generation_jgw2",
    "Suma_generacji_Jednostek_Grafikowych_Źródeł_Wiatrowych_i_Fotowoltaicznych_JGz_z_ZAK=1_(JGz1)_[MW]": "generation_jgz1",
    "Suma_generacji_Jednostek_Grafikowych_Źródeł_Wiatrowych_i_Fotowoltaicznych_JGz_z_ZAK=2_(JGz2)_[MW]": "generation_jgz2",
    "Suma_generacji_Jednostek_Grafikowych_Źródeł_Wiatrowych_i_Fotowoltaicznych_JGz_z_ZAK=3_(JGz3)_[MW]": "generation_jgz3",
    "Suma_generacji_jednostek_grafikowych_w_KSE_(JGw,_JGm,_JGz_i_JGa)_[MW]": "generation_kse",
    "Sumaryczna_generacja_źródeł_fotowoltaicznych": "generation_photovoltaic",
    "Sumaryczna_generacja_źródeł_wiatrowych": "generation_wind",
    "Sumaryczna_moc_ładowania": "jgm_charging_power",
    "Krajowe_saldo_wymiany_międzysystemowej__nierównoległa_[MW]": "non_parallel_cross_system_balance",
    "Krajowe_saldo_wymiany_międzysystemowej__równoległa_[MW]": "parallel_cross_system_balance",
}
# Rename columns in pk_actual
pk_actual.rename(columns=rename_columns_pl_to_en, inplace=True)
# sort columns
pk_actual = pk_actual.reindex(sorted(pk_actual.columns), axis=1)
### Final dataframe from MC base
pk5y_actual_mc = pk_actual[
    [
        "Date_of_publication_cet",
        "Date_cet",
        "Date_utc",
        "domestic_power_demand",
        "generation_jga",
        "generation_jgm1",
        "generation_jgm2",
        "generation_jgna",
        "generation_jgo",
        "generation_jgw1",
        "generation_jgw2",
        "generation_jgz1",
        "generation_jgz2",
        "generation_jgz3",
        "generation_kse",
        "generation_photovoltaic",
        "generation_wind",
        "jgm_charging_power",
        "non_parallel_cross_system_balance",
        "parallel_cross_system_balance",
    ]
].copy()
# If Date_of_publication_cet is NaT, fill it with Date_cet plus two days
pk5y_actual_mc["Date_of_publication_cet"] = pk5y_actual_mc.apply(
    lambda row: row["Date_cet"] + pd.Timedelta(days=2)
    if pd.isna(row["Date_of_publication_cet"])
    else row["Date_of_publication_cet"],
    axis=1,
)
### choose only dates before 2024-06-14
pk5y_actual_mc = pk5y_actual_mc[pk5y_actual_mc["Date_cet"] < "2024-06-14"]

# %% [markdown]
# ## Baza JWM

# %%
# download kse from jwm base
pk5y_actual_jwm = downloader_jwm.download_as_dataframe("utc/kse.csv")
# rename columns
pk5y_actual_jwm = pk5y_actual_jwm.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# aditional columns
pk5y_actual_jwm["Date_cet"] = pd.to_datetime(pk5y_actual_jwm["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
pk5y_actual_jwm["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_actual_jwm["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop columns
pk5y_actual_jwm = pk5y_actual_jwm.drop(
    columns=["delivery_end", "Date_of_publication_utc", "plan_day", "plan_indicator"]
)
# sort columns
pk5y_actual_jwm = pk5y_actual_jwm.reindex(sorted(pk5y_actual_jwm.columns), axis=1)
# to datetime
pk5y_actual_jwm["Date_utc"] = pd.to_datetime(pk5y_actual_jwm["Date_utc"])

# %% [markdown]
# ## Join two base

# %%
# final dataframe
pk5y_actual = pd.concat([pk5y_actual_mc, pk5y_actual_jwm])
# aditional colum
pk5y_actual["cb_flow_actual"] = (
    pk5y_actual["non_parallel_cross_system_balance"]
    + pk5y_actual["parallel_cross_system_balance"]
)
# to datetime
pk5y_actual["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_actual["Date_of_publication_cet"]
)

# %% [markdown]
# # save to parquet

# %%

out_path = Path(__file__).parent / "../out"
pk5y_actual.to_parquet(out_path / "pk5y_actual.parquet")
