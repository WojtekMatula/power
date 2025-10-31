from pathlib import Path
from datetime import timedelta
import pandas as pd
import numpy as np


def load_dataframe():
    out_path = Path(__file__).parent / "../out"
    return pd.read_parquet(out_path / "combined.parquet")


def fix_forecasts(df: pd.DataFrame, forecast_columns) -> pd.DataFrame:
    """
    Fixes forecast columns by applying a weighted average, then resamples,
    and applies interpolation and forward-filling to all relevant columns.
    """
    # Define the columns that need processing in a single list
    # --- Loop 1: Pre-resampling calculations ---
    # Calculate the weighted average for each forecast column
    for col in forecast_columns:
        df[f"{col}_fix"] = df[col] * 10 / 16 + df[col].shift(-4) * 6 / 16

    # Resample the dataframe to 15-minute intervals
    df = df.resample("15min").asfreq()

    # --- Loop 2: Post-resampling operations ---
    # Apply interpolation and forward-filling to all forecast columns
    for col in forecast_columns:
        # Create the 'fix2' column by interpolating the original forecast
        df[f"{col}_interpolate"] = df[col].interpolate(method="index")
        # Forward-fill the 'fix' column
        df[f"{col}_fix"] = df[f"{col}_fix"].ffill()
        # Forward-fill the original forecast column
        df[col] = df[col].ffill()

    return df


def calculate_peak_hours_forecast(df):
    df["generation_clean_actual"] = df["wind_actual"] + df["pv_actual"]
    df["demand-generation-clean_actual"] = (
        df["demand_actual"] - df["generation_clean_actual"]
    )

    df["peak_hours_top_actual"] = (
        df["demand-generation-clean_actual"]
        .rolling(24 * 4 * 14)
        .quantile(0.90)
        .shift(24 * 4 * 3)
    )
    df["peak_hours_bottom_actual"] = (
        df["demand-generation-clean_actual"]
        .rolling(24 * 4 * 14)
        .quantile(0.10)
        .shift(24 * 4 * 3)
    )

    df["peak_hours_actual"] = np.where(
        df["demand-generation-clean_actual"] > df["peak_hours_top_actual"], 2, 1
    )

    df["peak_hours_actual"] = np.where(
        df["demand-generation-clean_actual"] < df["peak_hours_bottom_actual"],
        0,
        df["peak_hours_actual"],
    )

    df["generation_clean_forecast"] = (
        df["wind_forecast_interpolate"] + df["pv_forecast_interpolate"]
    )
    df["demand-generation-clean_forecast"] = (
        df["demand_kse_forecast"] - df["generation_clean_forecast"]
    )

    df["peak_hours_forecast"] = np.where(
        df["demand-generation-clean_forecast"] > df["peak_hours_top_actual"], 2, 1
    )

    df["peak_hours_forecast"] = np.where(
        df["demand-generation-clean_forecast"] < df["peak_hours_bottom_actual"],
        0,
        df["peak_hours_forecast"],
    )
    # Clean up
    df = df.drop(
        columns=[
            "demand-generation-clean_forecast",
            "generation_clean_forecast",
        ]
    )
    return df


def calculate_supply_spikes(df):
    # Calculate rolling quantiles for the last N days
    # For supply_ab1
    df["top_90"] = df["supply_ab1_actual"].rolling("1D").quantile(0.9)
    df["bottom_10"] = df["supply_ab1_actual"].rolling("1D").quantile(0.1)

    # For supply_ab1_forecast
    df["top_90_forecast"] = df["supply_ab1_forecast"].rolling("1D").quantile(0.9)
    df["bottom_10_forecast"] = df["supply_ab1_forecast"].rolling("1D").quantile(0.1)

    # Reset index to restore original structure
    df = df.reset_index(drop=False)

    # Calculate sba columns
    df["supply_top"] = 0.0
    df.loc[df["supply_ab1_actual"] > df["top_90"], "supply_top"] = df[
        "supply_ab1_actual"
    ]

    df["supply_bottom"] = 0.0
    df.loc[df["supply_ab1_actual"] < df["bottom_10"], "supply_bottom"] = df[
        "supply_ab1_actual"
    ]

    df["supply_top_forecast"] = 0.0
    df.loc[
        df["supply_ab1_forecast"] > df["top_90_forecast"],
        "supply_top_forecast",
    ] = df["supply_ab1_forecast"]

    df["supply_bottom_forecast"] = 0.0
    df.loc[
        df["supply_ab1_forecast"] < df["bottom_10_forecast"],
        "supply_bottom_forecast",
    ] = df["supply_ab1_forecast"]
    # Clean up
    df = df.drop(
        columns=["top_90", "top_90_forecast", "bottom_10", "bottom_10_forecast"]
    )
    return df


if __name__ == "__main__":
    out_path = Path(__file__).parent / "../out"
    forecast_columns = [
        "cb_flow_forecast",
        "pv_forecast",
        "wind_forecast",
        "supply_ab1_forecast",
        "demand_forecast",
        "supply_nab_forecast",
        "surplus_capacity_over_reserve",
    ]

    df = load_dataframe()
    df = fix_forecasts(df, forecast_columns)
    df = calculate_peak_hours_forecast(df)
    df = calculate_supply_spikes(df)
    df = df.set_index("Date_utc").sort_index(ascending=True)
    df.to_parquet(out_path / "final.parquet")
    df.to_csv(out_path / "final.csv")
