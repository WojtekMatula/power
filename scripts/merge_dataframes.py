from pathlib import Path
from datetime import timedelta
import pandas as pd

out_path = Path(__file__).parent / "../out"


def front_fill_within_hour(df, column_to_fill):
    # Create hour-level grouping (e.g., 2023-01-01 12:00:00)
    df["hour"] = df.index.strftime("%Y-%m-%d %H:00:00")
    df[column_to_fill] = (
        df[column_to_fill].groupby(df["hour"]).transform(lambda x: x.ffill())
    )
    df = df.drop("hour", axis=1)
    return df


def keep_latest_actual(df):
    df = df.sort_values("Date_of_publication_cet", ascending=False).drop_duplicates(
        subset="Date_cet", keep="first"
    )
    df = df.sort_values("Date_utc", ascending=True)
    return df


def keep_latest_valid_forecast(pk_forecast):
    mask = (
        (
            (pk_forecast["Date_of_publication_cet"].dt.hour == 10)
            & (pk_forecast["Date_of_publication_cet"].dt.minute <= 15)
        )
        | (pk_forecast["Date_of_publication_cet"].dt.hour < 10)
    ) & (
        pk_forecast["Date_cet"].dt.date
        == (pk_forecast["Date_of_publication_cet"].dt.date + timedelta(days=1))
    )
    pk_forecast = pk_forecast.loc[mask]
    pk_forecast = pk_forecast.sort_values(
        "Date_of_publication_cet", ascending=False
    ).drop_duplicates(subset="Date_cet", keep="first")
    return pk_forecast


def merge_pk_actual_and_forecast(pk_actual_rename, pk_forecast_rename) -> pd.DataFrame:
    pk_actual = pd.read_parquet(out_path / "pk5y_actual.parquet")
    pk_forecast = pd.read_parquet(out_path / "pk5y_forecast.parquet")
    pk_forecast = pk_forecast.rename(columns=pk_forecast_rename)
    pk_actual = pk_actual.rename(columns=pk_actual_rename)
    # Keep only latest forecast per Date_utc (published before 10:15)
    pk_forecast = keep_latest_valid_forecast(pk_forecast)
    pk_forecast = pk_forecast.set_index("Date_utc").sort_index()
    pk_actual = pk_actual.set_index("Date_utc").sort_index()
    pk_forecast_columns = list(pk_forecast_rename.values())
    pk_actual_columns = list(pk_actual_rename.values())
    pk_forecast = pk_forecast[pk_forecast_columns]
    pk_actual = pk_actual[pk_actual_columns]
    return pk_forecast.merge(pk_actual, left_index=True, right_index=True, how="outer")


def merge_kse_load_forecast(
    kse_load_forecast_rename, pk_actual_and_forecast
) -> pd.DataFrame:
    kse_load_forecast = pd.read_parquet(out_path / "kse_load_forecast.parquet")
    kse_load_forecast = kse_load_forecast.rename(columns=kse_load_forecast_rename)
    kse_load_forecast = kse_load_forecast.set_index("Date_utc").sort_index()
    kse_load_forecast_columns = list(kse_load_forecast_rename.values())
    kse_load_forecast = kse_load_forecast[kse_load_forecast_columns]
    return pk_actual_and_forecast.merge(
        kse_load_forecast, left_index=True, right_index=True, how="outer"
    )


def merge_peak_hours(df):
    peak_hours_actual = pd.read_parquet(out_path / "peak_hours_actual.parquet")
    peak_hours_actual = peak_hours_actual.set_index("Date_utc").sort_index()
    peak_hours_actual = keep_latest_actual(peak_hours_actual)
    df = df.merge(
        peak_hours_actual[["peak_hours_actual"]],
        left_index=True,
        right_index=True,
        how="outer",
    )
    df = front_fill_within_hour(df, "peak_hours_actual")
    return df


def merge_prices(df):
    fix_price = pd.read_parquet(out_path / "fix_price.parquet")
    rb_price = pd.read_parquet(out_path / "rb_price.parquet")

    fix_price = fix_price.set_index("Date_utc").sort_index()
    rb_price = rb_price.set_index("Date_utc").sort_index()
    df = df.merge(
        fix_price[
            ["fixing1_price", "fixing2_price", "fixing2_volume", "fixing1_volume"]
        ],
        left_index=True,
        right_index=True,
        how="outer",
    )
    df = front_fill_within_hour(df, "fixing1_price")
    df = front_fill_within_hour(df, "fixing1_volume")
    df = front_fill_within_hour(df, "fixing2_price")
    df = front_fill_within_hour(df, "fixing2_volume")
    df = df.merge(
        rb_price[["bilans_price"]],
        left_index=True,
        right_index=True,
        how="outer",
    )
    return df


if __name__ == "__main__":
    pk_actual_rename = {
        "domestic_power_demand": "demand_actual",
        "generation_jgna": "supply_nab_actual",
        "generation_kse": "supply_ab1_actual",
        "generation_photovoltaic": "pv_actual",
        "generation_wind": "wind_actual",
    }

    pk_forecast_rename = {
        "cross_border_balance_forecast": "cb_flow_forecast",
        "pv_total_generation_forecast": "pv_forecast",
        "wind_total_generation_forecast": "wind_forecast",
        "avail_gen_of_gen_unit_and_energy_storage_rb": "supply_ab1_forecast",
        "grid_demand_forecast": "demand_forecast",
        "avail_gen_of_gen_unit_and_energy_storage_non_rb": "supply_nab_forecast",
        "surplus_cap_avail_for_tso": "surplus_capacity_tso",
        "required_power_reserve": "required_power_reserve",
        "surplus_cap_avail_for_tso_over_pow_res": "surplus_capacity_over_reserve",
    }

    kse_load_forecast_rename = {"load_forecast": "demand_kse_forecast"}

    df = merge_pk_actual_and_forecast(pk_actual_rename, pk_forecast_rename)
    df = merge_kse_load_forecast(kse_load_forecast_rename, df)
    df = merge_peak_hours(df)

    df = merge_prices(df)
    df.to_parquet(out_path / "combined.parquet")
    df.to_csv(out_path / "test.csv")

    duplicates_mask = df.index.duplicated(keep=False)
    duplicate_rows = df[duplicates_mask]
    if not duplicate_rows.empty:
        error_msg = (
            f"ERROR: Duplicates found! {len(duplicate_rows)} duplicate rows detected"
        )
        raise ValueError(error_msg)
