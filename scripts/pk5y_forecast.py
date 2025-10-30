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


# %%
# available = downloader_jwm.list_available_files()
# available
# downloader_jwm.download_as_dataframe("utc/pk5y_actual_at_10-00.csv")

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


#### function to add UTC timestamps based on date and hour columns, handling 2.5 hour for DST
def add_utc_half(df, date_col="date", hour_col="hour"):
    """
    Dodaje do DataFrame kolumny 'Date_cet' (czas lokalny Europe/Warsaw)
    oraz 'Date_utc' (czas UTC). Obsługuje zmianę czasu (np. 2.0 = pierwsza 02:00,
    2.5 = druga 02:00).
    """
    # konwersja kolumny daty
    base_date = pd.to_datetime(df[date_col])
    whole_hour = np.floor(df[hour_col]).astype(int)

    # przesunięcie dnia przy godzinie 24
    date_shift = base_date + pd.to_timedelta((whole_hour == 24).astype(int), unit="D")
    whole_hour = np.where(whole_hour == 24, 0, whole_hour)

    # budowa czasu lokalnego (naive, bez strefy)
    naive_local = date_shift + pd.to_timedelta(whole_hour, unit="h")

    # rozróżnienie dwóch "2:00" w dniu zmiany czasu
    ambiguous = np.where(
        df[hour_col] == 2.0, True, np.where(df[hour_col] == 2.5, False, np.nan)
    )

    # nadanie strefy czasowej
    df["Date_cet"] = naive_local.dt.tz_localize("Europe/Warsaw", ambiguous=ambiguous)

    # konwersja do UTC
    df["Date_utc"] = df["Date_cet"].dt.tz_convert("UTC")

    return df


# %% [markdown]
# # Baza danych

# %% [markdown]
# ## Plan koordynacyjny 5lat

# %% [markdown]
# ### Baza MC

# %% [markdown]
# ##### Old PSE

# %%
# Plan Koordynacyjny Histoira
pk_his = downloader.get_csv_as_dataframe(
    "power", "pse_plan_koordynacyjny_2021-2024.csv"
)
# rename columns
pk_his.rename(columns={"Doba": "date", "Godzina": "hour"}, inplace=True)
pk_his["date"] = pd.to_datetime(pk_his["date"])
# set index
pk_his.set_index(["date"], inplace=True)

# rename columns to pk_new names
pk_his = pk_his.rename(
    columns={
        "Moc dyspozycyjna JW i magazynów energii świadczących usługi bilansujące w ramach RB": "Moc dyspozycyjna JW i magazynow energii swiadczacych uslugi bilansujace w ramach RB",
        "Moc dyspozycyjna JW i magazynów energii świadczących usługi bilansujące w ramach RB dostępna dla OSP": "Moc dyspozycyjna JW i magazynow energii swiadczacych uslugi bilansujace w ramach RB dostepna dla OSP",
        "Obowiązki mocowe wszystkich jednostek rynku mocy": "Obowiazki mocowe wszystkich jednostek rynku mocy",
        "Planowane saldo wymiany międzysystemowej": "Planowane saldo wymiany miedzysystemowej",
        "Prognozowana generacja JW i magazynów energii nie świadczących usług bilansujących w ramach RB": "Prognozowana generacja JW i magazynow energii nie swiadczacych uslug bilansujacych w ramach RB",
        "Prognozowana sumaryczna generacja źródeł fotowoltaicznych": "Prognozowana sumaryczna generacja zrodel fotowoltaicznych",
        "Prognozowana sumaryczna generacja źródeł wiatrowych": "Prognozowana sumaryczna generacja zrodel wiatrowych",
        "Prognozowana wielkość niedyspozycyjności wynikająca z ograniczeń sieciowych występujących w sieci przesyłowej oraz sieci dystrybucyjnej w zakresie dostarczania energii elektrycznej": "Prognozowana wielkosc niedyspozycyjnosci wynikajaca z ograniczen sieciowych wystepujacych w sieci przesylowej oraz sieci dystrybucyjnej w zakresie dostarczania energii elektrycznej",
        "Przewidywana generacja zasobów wytwórczych nieobjętych obowiązkami mocowymi": "Przewidywana generacja zasobow wytworczych nieobjetych obowiazkami mocowymi",
        "Przewidywana generacja JW i magazynów energii świadczących usługi bilansujące w ramach RB (3) - (10) - (13)": "Przewidywana generacja JW i magazynow energii swiadczacych uslugi bilansujace w ramach RB",
        "Nadwyżka mocy dostępna dla OSP (8) + (10) - [(3)-(13)]-(14)": "Nadwyzka mocy dostepna dla OSP",
        "Nadwyżka mocy dostępna dla OSP ponad wymaganą rezerwę moc (5) - (4)": "Nadwyzka mocy dostepna dla OSP ponad wymagana rezerwe mocy",
        "Prognozowana wielkość niedyspozycyjności wynikających z warunków eksploatacyjnych JW świadczących usługi bilansujące w ramach RB": "Suma niedostepnosci (postoje + ubytki) ze wzgledu na warunki eksploatacyjne (WE)",
    }
)
# cut old data
pk_his = pk_his.loc[:"2024-06-14"].iloc[:-23].copy()
# hour index
pk_his["hour_idx"] = pk_his.groupby("date").cumcount()
# reset index
pk_his.reset_index(inplace=True)

## Append UTC timestamps
pk_his = add_utc_25(
    pk_his,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# hour column
pk_his["hour"] = pk_his["Date_cet"].dt.hour

# %% [markdown]
# ##### New PSE EOD from MC DB

# %%
# Plan Koordynacyjny Nowy
pk_new = downloader.get_csv_as_dataframe("power", "pse_plan_koordynacyjny.csv")
# date and hour column
pk_new[["date", "hour"]] = pk_new["Doba"].astype(str).str.split(" ", expand=True)
# crate column witch give numbers from 0 to 23/24/25 gruping on date
pk_new["hour_idx"] = pk_new.groupby("date").cumcount()

# Teraz użyj hour_idx jako hour_col
pk_new = add_utc_25(
    pk_new,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# hour column
pk_new["hour"] = pk_new["Date_cet"].dt.hour

# %% [markdown]
# ##### PSE LIVE from MC DB

# %%
### download pk data live
pk_live = downloader.get_csv_as_dataframe("power_live", "pse_plan_koordynacyjny.csv")
# replace spaces in column names
pk_live.columns = pk_live.columns.str.replace(" ", "_")
# date and hour column
pk_live[["date", "hour"]] = pk_live["Doba"].astype(str).str.split(" ", expand=True)
# hour index
pk_live["hour_idx"] = pk_live.groupby(["date", "Data_aktualizacji"]).cumcount()
# add UTC timestamps
pk_live = add_utc_25(
    pk_live,
    date_col="date",
    hour_col="hour_idx",
    tz="Europe/Warsaw",
    out_col="Date_utc",
    local_col="Date_cet",
)
# hour column
pk_live["hour"] = pk_live["Date_cet"].dt.hour

# %% [markdown]
# ###### Join all histroy of pk

# %%
# join dataframes
pk = pd.concat([pk_his, pk_new])
# replace spaces in column names
pk.columns = pk.columns.str.replace(" ", "_")
# drop columns
pk.drop(columns=["Doba", "Doba_handlowa"], inplace=True)

#### Non Linear History
pk_mc_pl = pd.concat([pk, pk_live], axis=0)
pk_mc_pl = pk_mc_pl.sort_values(["Date_utc"], ascending=True)
#### Uppercase columns names
pk_mc_pl.columns = [col[0].upper() + col[1:] if col else "" for col in pk_mc_pl.columns]

# rename columns to english
pk_mc = pk_mc_pl.rename(
    columns={
        "Data_aktualizacji": "Date_of_update_cet",
        "Data_publikacji": "Date_of_publication_cet",
        "Planowane_saldo_wymiany_miedzysystemowej": "cross_border_balance_forecast",
        "Moc_dyspozycyjna_JW_i_magazynow_energii_swiadczacych_uslugi_bilansujace_w_ramach_RB": "avail_cap_of_gen_unit_and_energy_storage",
        "Moc_dyspozycyjna_JW_i_magazynow_energii_swiadczacych_uslugi_bilansujace_w_ramach_RB_dostepna_dla_OSP": "avail_cap_of_gen_unit_and_energy_storage_osp",
        "Nadwyzka_mocy_dostepna_dla_OSP": "surplus_cap_avail_for_tso",
        "Wymagana_rezerwa_mocy_OSP": "required_power_reserve",
        "Przewidywana_generacja_zasobow_wytworczych_nieobjetych_obowiazkami_mocowymi": "pred_gen_by_res_not_covered_by_cap_market_obligation",
        "Suma_niedostepnosci_(postoje_+_ubytki)_ze_wzgledu_na_warunki_eksploatacyjne_(WE)": "sum_of_planned_unavailability",
        "Prognozowane_zapotrzebowanie_sieci": "grid_demand_forecast",
        "Nadwyzka_mocy_dostepna_dla_OSP_ponad_wymagana_rezerwe_mocy": "surplus_cap_avail_for_tso_over_pow_res",
        "Prognozowana_generacja_JW_i_magazynow_energii_nie_swiadczacych_uslug_bilansujacych_w_ramach_RB": "avail_gen_of_gen_unit_and_energy_storage_non_rb",
        "Planowane_ograniczenia_dyspozycyjnosci_i_odstawien_MWE": "planned_restrictions",
        "Prognozowana_sumaryczna_generacja_zrodel_wiatrowych": "wind_total_generation_forecast",
        "Przewidywana_generacja_JW_i_magazynow_energii_swiadczacych_uslugi_bilansujace_w_ramach_RB": "avail_gen_of_gen_unit_and_energy_storage_rb",
        "Prognozowana_sumaryczna_generacja_zrodel_fotowoltaicznych": "pv_total_generation_forecast",
        "Obowiazki_mocowe_wszystkich_jednostek_rynku_mocy": "cap_market_obligation_of_all_cap_market_units",
        "Prognozowana_wielkosc_niedyspozycyjnosci_wynikajaca_z_ograniczen_sieciowych_wystepujacych_w_sieci"
        "_przesylowej_oraz_sieci_dystrybucyjnej_w_zakresie_dostarczania_energii_elektrycznej": "unavailability_forecast",
    }
)
# Normalize date columns
pk_mc["Date_of_update_cet"] = pd.to_datetime(pk_mc["Date_of_update_cet"])
pk_mc["Date_of_publication_cet"] = pd.to_datetime(pk_mc["Date_of_publication_cet"])
# drop columns
pk_mc.drop(
    columns=["Data_utworzenia", "Date", "Hour", "Doba", "Doba_handlowa", "Hour_idx"],
    inplace=True,
)
# sort columns
pk_mc = pk_mc.reindex(sorted(pk_mc.columns), axis=1)

# Reset index to avoid duplicate label issues (as discussed earlier)
pk_mc = pk_mc.reset_index(drop=True)

# Compute the new datetime values and remove timezone before assignment
new_values = (
    pd.to_datetime(pk_mc["Date_cet"]).dt.normalize()
    + pd.to_timedelta("1 day")  # Assuming the update time is 10:00 AM
).dt.tz_localize(None)  # <-- This removes timezone info

# Assign only to rows where Date_of_update_cet is NaT
mask = pk_mc["Date_of_update_cet"].isna()
pk_mc.loc[mask, "Date_of_update_cet"] = new_values[mask]

# Fill NaT values in Date_of_publication_cet with Date_of_update_cet
pk_mc["Date_of_publication_cet"] = pk_mc["Date_of_publication_cet"].fillna(
    pk_mc["Date_of_update_cet"]
)

### set Date_of_publication_cet and Date_of_update_cet as CET timezone
pk_mc["Date_of_update_cet"] = pd.to_datetime(
    pk_mc["Date_of_update_cet"]
).dt.tz_localize("Europe/Warsaw", ambiguous="infer", nonexistent="shift_forward")
pk_mc["Date_of_publication_cet"] = pd.to_datetime(
    pk_mc["Date_of_publication_cet"]
).dt.tz_localize("Europe/Warsaw", ambiguous="infer", nonexistent="shift_forward")
# sort values by Date_utc and Date_of_publication_cet
pk_mc.sort_values(
    by=["Date_utc", "Date_of_publication_cet"], ascending=True, inplace=True
)

# %% [markdown]
# ### Baza JWM

# %% [markdown]
# #### Saved at 10:00 history

# %%
### pk5y saved at 10 from jwm
pk5y_10 = downloader_jwm.download_as_dataframe("utc/pk5y_actual_at_10-00.csv")
# drop columns
pk5y_10.drop(columns=["plan_day", "plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_10 = pk5y_10.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_cet",
    }
)
# change to warsaw time
pk5y_10["Date_cet"] = pd.to_datetime(pk5y_10["Date_utc"]).dt.tz_convert("Europe/Warsaw")
# creat 'Date_of_update_cet' as 'Date_cet' - one day and at 10:00
pk5y_10["Date_of_update_cet"] = pd.to_datetime(
    pk5y_10["Date_cet"] - pd.to_timedelta("1 day")
).dt.normalize() + pd.to_timedelta(10, unit="h")
# change 'Date_of_publication_cet' to same as 'Date_of_update_cet' because pse was giving no update time on utc column
pk5y_10["Date_of_publication_cet"] = pk5y_10["Date_of_update_cet"]
# date columns to datetime
pk5y_10["Date_utc"] = pd.to_datetime(pk5y_10["Date_utc"])
pk5y_10["Date_cet"] = pd.to_datetime(pk5y_10["Date_cet"])
pk5y_10["Date_of_update_cet"] = pd.to_datetime(pk5y_10["Date_of_update_cet"])
pk5y_10["Date_of_publication_cet"] = pd.to_datetime(pk5y_10["Date_of_publication_cet"])
# sort columns
pk5y_10 = pk5y_10.reindex(sorted(pk5y_10.columns), axis=1)
# choose history till Date_cet = 2025-07-20
pk5y_10 = pk5y_10[pk5y_10["Date_cet"] < "2025-07-20"].copy()

# %% [markdown]
# #### Saved EOD history

# %%
### pk5y saved eod from jwm
pk5y_eod = downloader_jwm.download_as_dataframe("utc/pk5y_actual_eod.csv")
# drop columns
pk5y_eod.drop(columns=["plan_day", "plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_eod = pk5y_eod.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_cet",
    }
)
# change to warsaw time
pk5y_eod["Date_cet"] = pd.to_datetime(pk5y_eod["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# creat 'Date_of_update_cet' as 'Date_cet'
pk5y_eod["Date_of_update_cet"] = (
    pd.to_datetime(pk5y_eod["Date_cet"] - pd.to_timedelta("1 day")).dt.normalize()
    + pd.to_timedelta(23, unit="h")
    + pd.to_timedelta(59, unit="m")
)
# change 'Date_of_publication_cet' to same as 'Date_of_update_cet' because pse was giving no update time on utc column
pk5y_eod["Date_of_publication_cet"] = pk5y_eod["Date_of_update_cet"]
# date columns to datetime
pk5y_eod["Date_utc"] = pd.to_datetime(pk5y_eod["Date_utc"])
pk5y_eod["Date_cet"] = pd.to_datetime(pk5y_eod["Date_cet"])
pk5y_eod["Date_of_update_cet"] = pd.to_datetime(pk5y_eod["Date_of_update_cet"])
pk5y_eod["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_eod["Date_of_publication_cet"]
)
# sort columns
pk5y_eod = pk5y_eod.reindex(sorted(pk5y_eod.columns), axis=1)
# to datetime
pk5y_eod["Date_utc"] = pd.to_datetime(pk5y_eod["Date_utc"])
# choose history till Date_cet = 2025-08-15
pk5y_eod = pk5y_eod[pk5y_eod["Date_cet"] < "2025-08-15"].copy()

# %% [markdown]
# #### New pk5y on JWM base saved on 7:30, 10:05, 10:10, 10:15, 10:20, 23:59

# %% [markdown]
# ##### pk5y saved at 7:30

# %%
### pk5y saved at 7:30
pk5y_0730n = downloader_jwm.download_as_dataframe("utc/pk5y_forecast_07-30.csv")
# drop columns
pk5y_0730n.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_0730n = pk5y_0730n.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# if Date_of_publication_utc is na fill it with Date_of_update_utc
pk5y_0730n["Date_of_publication_utc"] = pk5y_0730n["Date_of_publication_utc"].fillna(
    pk5y_0730n["Date_of_update_utc"]
)
# create 'Date_cet' from 'Date_utc'
pk5y_0730n["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_0730n["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
pk5y_0730n["Date_of_update_cet"] = pd.to_datetime(
    pk5y_0730n["Date_of_update_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop not needed utc columns
pk5y_0730n.drop(columns=["Date_of_publication_utc", "Date_of_update_utc"], inplace=True)
# create 'Date_cet' from 'Date_utc'
pk5y_0730n["Date_cet"] = pd.to_datetime(pk5y_0730n["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# sort columns
pk5y_0730n = pk5y_0730n.reindex(sorted(pk5y_0730n.columns), axis=1)
# to datetime
pk5y_0730n["Date_utc"] = pd.to_datetime(pk5y_0730n["Date_utc"])

# %% [markdown]
# ##### pk5y saved at 10:05

# %%
### pk5y saved at 10:05
pk5y_1005n = downloader_jwm.download_as_dataframe("utc/pk5y_forecast_10-05.csv")
# drop columns
pk5y_1005n.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_1005n = pk5y_1005n.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# if Date_of_publication_utc is na fill it with Date_of_update_utc
pk5y_1005n["Date_of_publication_utc"] = pk5y_1005n["Date_of_publication_utc"].fillna(
    pk5y_1005n["Date_of_update_utc"]
)
# create 'Date_cet' from 'Date_utc'
pk5y_1005n["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_1005n["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
pk5y_1005n["Date_of_update_cet"] = pd.to_datetime(
    pk5y_1005n["Date_of_update_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop not needed utc columns
pk5y_1005n.drop(columns=["Date_of_publication_utc", "Date_of_update_utc"], inplace=True)
# create 'Date_cet' from 'Date_utc'
pk5y_1005n["Date_cet"] = pd.to_datetime(pk5y_1005n["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# sort columns
pk5y_1005n = pk5y_1005n.reindex(sorted(pk5y_1005n.columns), axis=1)
# to datetime
pk5y_1005n["Date_utc"] = pd.to_datetime(pk5y_1005n["Date_utc"])

# %% [markdown]
# ##### pk5y saved at 10:10

# %%
### pk5y saved at 10:10
pk5y_1010n = downloader_jwm.download_as_dataframe("utc/pk5y_forecast_10-10.csv")
# drop columns
pk5y_1010n.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_1010n = pk5y_1010n.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# if Date_of_publication_utc is na fill it with Date_of_update_utc
pk5y_1010n["Date_of_publication_utc"] = pk5y_1010n["Date_of_publication_utc"].fillna(
    pk5y_1010n["Date_of_update_utc"]
)
# create 'Date_cet' from 'Date_utc'
pk5y_1010n["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_1010n["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
pk5y_1010n["Date_of_update_cet"] = pd.to_datetime(
    pk5y_1010n["Date_of_update_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop not needed utc columns
pk5y_1010n.drop(columns=["Date_of_publication_utc", "Date_of_update_utc"], inplace=True)
# create 'Date_cet' from 'Date_utc'
pk5y_1010n["Date_cet"] = pd.to_datetime(pk5y_1010n["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# sort columns
pk5y_1010n = pk5y_1010n.reindex(sorted(pk5y_1010n.columns), axis=1)
# to datetime
pk5y_1010n["Date_utc"] = pd.to_datetime(pk5y_1010n["Date_utc"])

# %% [markdown]
# ##### pk5y saved at 10:15

# %%
### pk5y saved at 10:15
pk5y_1015n = downloader_jwm.download_as_dataframe("utc/pk5y_forecast_10-15.csv")
# drop columns
pk5y_1015n.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_1015n = pk5y_1015n.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# if Date_of_publication_utc is na fill it with Date_of_update_utc
pk5y_1015n["Date_of_publication_utc"] = pk5y_1015n["Date_of_publication_utc"].fillna(
    pk5y_1015n["Date_of_update_utc"]
)
# create 'Date_cet' from 'Date_utc'
pk5y_1015n["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_1015n["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
pk5y_1015n["Date_of_update_cet"] = pd.to_datetime(
    pk5y_1015n["Date_of_update_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop not needed utc columns
pk5y_1015n.drop(columns=["Date_of_publication_utc", "Date_of_update_utc"], inplace=True)
# create 'Date_cet' from 'Date_utc'
pk5y_1015n["Date_cet"] = pd.to_datetime(pk5y_1015n["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# sort columns
pk5y_1015n = pk5y_1015n.reindex(sorted(pk5y_1015n.columns), axis=1)
# to datetime
pk5y_1015n["Date_utc"] = pd.to_datetime(pk5y_1015n["Date_utc"])

# %% [markdown]
# ##### pk5y saved at 10:20

# %%
### pk5y saved at 10:20
pk5y_1020n = downloader_jwm.download_as_dataframe("utc/pk5y_forecast_10-20.csv")
# drop columns
pk5y_1020n.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_1020n = pk5y_1020n.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# if Date_of_publication_utc is na fill it with Date_of_update_utc
pk5y_1020n["Date_of_publication_utc"] = pk5y_1020n["Date_of_publication_utc"].fillna(
    pk5y_1020n["Date_of_update_utc"]
)
# create 'Date_cet' from 'Date_utc'
pk5y_1020n["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_1020n["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
pk5y_1020n["Date_of_update_cet"] = pd.to_datetime(
    pk5y_1020n["Date_of_update_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop not needed utc columns
pk5y_1020n.drop(columns=["Date_of_publication_utc", "Date_of_update_utc"], inplace=True)
# create 'Date_cet' from 'Date_utc'
pk5y_1020n["Date_cet"] = pd.to_datetime(pk5y_1020n["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# sort columns
pk5y_1020n = pk5y_1020n.reindex(sorted(pk5y_1020n.columns), axis=1)
# to datetime
pk5y_1020n["Date_utc"] = pd.to_datetime(pk5y_1020n["Date_utc"])


# %% [markdown]
# ##### pk5y saved at 23:59

# %%
### pk5y saved at 23:59
pk5y_2359n = downloader_jwm.download_as_dataframe("utc/pk5y_forecast_23-59.csv")
# drop columns
pk5y_2359n.drop(columns=["timeseries_plan_indicator", "delivery_end"], inplace=True)
# rename columns
pk5y_2359n = pk5y_2359n.rename(
    columns={
        "delivery_start": "Date_utc",
        "publication_timestamp": "Date_of_publication_utc",
        "timeseries_plan_created_date": "Date_of_update_utc",
    }
)
# if Date_of_publication_utc is na fill it with Date_of_update_utc
pk5y_2359n["Date_of_publication_utc"] = pk5y_2359n["Date_of_publication_utc"].fillna(
    pk5y_2359n["Date_of_update_utc"]
)
# create 'Date_cet' from 'Date_utc'
pk5y_2359n["Date_of_publication_cet"] = pd.to_datetime(
    pk5y_2359n["Date_of_publication_utc"]
).dt.tz_convert("Europe/Warsaw")
pk5y_2359n["Date_of_update_cet"] = pd.to_datetime(
    pk5y_2359n["Date_of_update_utc"]
).dt.tz_convert("Europe/Warsaw")
# drop not needed utc columns
pk5y_2359n.drop(columns=["Date_of_publication_utc", "Date_of_update_utc"], inplace=True)
# create 'Date_cet' from 'Date_utc'
pk5y_2359n["Date_cet"] = pd.to_datetime(pk5y_2359n["Date_utc"]).dt.tz_convert(
    "Europe/Warsaw"
)
# sort columns
pk5y_2359n = pk5y_2359n.reindex(sorted(pk5y_2359n.columns), axis=1)
# to datetime
pk5y_2359n["Date_utc"] = pd.to_datetime(pk5y_2359n["Date_utc"])

# %% [markdown]
# ##### Join JWM DB data

# %%
# concatenate pk5y_10 and pk5y_eod
pk_jwm = pd.concat(
    [
        pk5y_10,
        pk5y_eod,
        pk5y_0730n,
        pk5y_1005n,
        pk5y_1010n,
        pk5y_1015n,
        pk5y_1020n,
        pk5y_2359n,
    ]
)
# sort values by date_utc and publication date
pk_jwm = pk_jwm.sort_values(by=["Date_utc", "Date_of_publication_cet"], ascending=True)

# %% [markdown]
# ### Join MC and JWM datam

# %%
# join pk_jwm with pk_mc on Date_utc
pk5y_forecast = pd.concat([pk_mc, pk_jwm])
# sort values by Date_utc and Date_of_publication_cet
pk5y_forecast = pk5y_forecast.sort_values(
    by=["Date_utc", "Date_of_publication_cet"], ascending=True
)

# %% [markdown]
# # save to parquet

# %%
# save to parquet
out_path = Path(__file__).parent / "../out"
pk5y_forecast.to_parquet(
    out_path / "pk5y_forecast.parquet",
    index=False,
)
