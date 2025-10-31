import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
from datetime import date
import sys
import webbrowser
from pathlib import Path

# Assuming load_dataframe() is defined elsewhere or replace with your loading logic
# def load_dataframe():
#     # Your code to load the dataframe
#     pass


def load_dataframe():
    out_path = Path(__file__).parent / "../out"
    return pd.read_parquet(out_path / "combined.parquet")


def calculate_peak_hours_forecast(
    df, rolling_window=24 * 4 * 21, bottom_q=0.15, top_q=0.85
):
    df["generation_clean_forecast"] = df["wind_actual"] + df["pv_actual"]
    df["demand-generation-clean_forecast"] = (
        df["demand_kse_forecast"] - df["generation_clean_forecast"]
    )

    df["peak_hours_top_forecast"] = (
        df["demand-generation-clean_forecast"].rolling(rolling_window).quantile(top_q)
    )
    df["peak_hours_bottom_forecast"] = (
        df["demand-generation-clean_forecast"]
        .rolling(rolling_window)
        .quantile(bottom_q)
    )

    df["peak_hours_forecast"] = np.where(
        df["demand-generation-clean_forecast"] > df["peak_hours_top_forecast"], 2, 1
    )

    df["peak_hours_forecast"] = np.where(
        df["demand-generation-clean_forecast"] < df["peak_hours_bottom_forecast"],
        0,
        df["peak_hours_forecast"],
    )
    # Clean up
    df = df.drop(
        columns=[
            "peak_hours_top_forecast",
            "peak_hours_bottom_forecast",
            "demand-generation-clean_forecast",
            "generation_clean_forecast",
        ]
    )
    return df


def calculate_peak_hours_actual(
    df, rolling_window=24 * 4 * 21, bottom_q=0.15, top_q=0.85
):
    df["generation_clean_actual"] = df["wind_actual"] + df["pv_actual"]
    df["demand-generation-clean_actual"] = (
        df["demand_kse_forecast"] - df["generation_clean_actual"]
    )  # Assuming 'demand_kse_actual' is the correct column name; adjust if needed

    df["peak_hours_top_actual"] = (
        df["demand-generation-clean_actual"].rolling(rolling_window).quantile(top_q)
    )
    df["peak_hours_bottom_actual"] = (
        df["demand-generation-clean_actual"].rolling(rolling_window).quantile(bottom_q)
    )

    df["peak_hours_forecast"] = np.where(
        df["demand-generation-clean_actual"] > df["peak_hours_top_actual"], 2, 1
    )

    df["peak_hours_forecast"] = np.where(
        df["demand-generation-clean_actual"] < df["peak_hours_bottom_actual"],
        0,
        df["peak_hours_forecast"],
    )
    # Clean up
    df = df.drop(
        columns=[
            "peak_hours_top_actual",
            "peak_hours_bottom_actual",
            "demand-generation-clean_actual",
            "generation_clean_actual",
        ]
    )
    return df


def get_correlation(df, forecast_col, actual_col):
    analysis = df.dropna(subset=[forecast_col, actual_col])
    if analysis.empty or len(analysis) < 2:
        return np.nan

    forecasts = analysis[forecast_col]
    actuals = analysis[actual_col]

    std_a = np.std(actuals)
    std_f = np.std(forecasts)
    if std_a == 0 or std_f == 0:
        return 0.0  # Or np.nan if you prefer to keep it undefined

    correlation = np.corrcoef(actuals, forecasts)[0, 1]
    return correlation


if __name__ == "__main__":
    # Load the dataframe
    df = load_dataframe()  # Assuming this function exists; adjust as needed

    # Parameter ranges
    days_list = list(range(1, 81, 5))
    quantile_list = list(range(1, 81, 5))
    results = np.zeros((len(days_list), len(quantile_list)))

    for i, days in enumerate(days_list):
        print(f"day {days}")
        rolling_window = 14 * 24 * 4  # Assuming 15-minute intervals (96 per day)
        for j, k in enumerate(quantile_list):
            bottom_q = k / 100.0
            top_q = 1.0 - (days / 100.0)

            # Compute actual
            df_actual = calculate_peak_hours_actual(
                df.copy(), rolling_window, bottom_q, top_q
            )

            # Compute forecast
            df_forecast = calculate_peak_hours_forecast(
                df.copy(), rolling_window, bottom_q, top_q
            )

            # To correlate, we can use one df, but since columns are different, merge or use separate
            # Assuming indices match, we can add forecast to actual df or compute corr directly
            corr = get_correlation(
                pd.concat(
                    [
                        df_actual["peak_hours_actual"],
                        df_forecast["peak_hours_forecast"],
                    ],
                    axis=1,
                ),
                "peak_hours_forecast",
                "peak_hours_actual",
            )
            results[i, j] = corr

    # Create heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=results,
            x=quantile_list,
            y=days_list,
            colorscale="Viridis",
            zmin=0.4,
            zmax=0.55,
        )
    )

    fig.update_layout(
        title="Pearson Correlation Heatmap for Peak Hours Forecast",
        xaxis_title="Quantiles Threshold (1-50 for bottom, top=100 - threshold)",
        yaxis_title="Rolling Window (days)",
    )

    fig.show()
