import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
from datetime import date
import sys
import webbrowser
from pathlib import Path


def validate_forecasts(df: pd.DataFrame, forecast_col: str, actual_col: str) -> None:
    print("\n--- Forecast Validation (Metrics) ---")
    # Ensure required columns exist
    if forecast_col not in df.columns or actual_col not in df.columns:
        print(f"Error: Columns '{forecast_col}' or '{actual_col}' missing.")
        return

    # Drop rows with missing values for the two columns
    analysis = df.dropna(subset=[forecast_col, actual_col])
    if analysis.empty:
        print("No data to analyze after dropping missing values.")
        return

    forecasts = analysis[forecast_col]
    actuals = analysis[actual_col]

    mae = np.mean(np.abs(actuals - forecasts))
    rmse = np.sqrt(np.mean((actuals - forecasts) ** 2))
    mean_abs_actual = np.mean(np.abs(actuals))
    relative_mae = mae / mean_abs_actual * 100 if mean_abs_actual != 0 else np.nan
    bias = np.mean(forecasts - actuals)
    bias_percent = (
        np.abs(bias) / mean_abs_actual * 100 if mean_abs_actual != 0 else np.nan
    )
    correlation = np.corrcoef(actuals, forecasts)[0, 1]

    print(f"Mean Absolute Error (MAE): {mae:.2f} MW")
    print(f"Root Mean Squared Error (RMSE): {rmse:.2f} MW")
    print(f"Relative MAE (% of mean |actual|): {relative_mae:.2f}%")
    print(f"Bias (mean forecast-actual): {bias:.2f} MW")
    print(f"Bias percentage: {bias_percent:.2f}%")
    print(f"Pearson correlation: {correlation:.4f}")
    print("-----------------------------------------")


def visualize_data(
    df: pd.DataFrame,
    forecast_col: str,
    actual_col: str,
    date_range: tuple,
    output_html_file: str = "walidacja_wizualna.html",
    smoothing_window: str = "7D",  # Nowy parametr: domyślnie 1 tydzień (7 dni)
):
    """
    Tworzy interaktywne wykresy i zapisuje je do pliku HTML.
    `date_range` powinien być krotką (data_poczatkowa, data_koncna).
    `smoothing_window` określa okno wygładzania dla średniego błędu (np. '7D' dla 7 dni).
    """
    print(
        f"\n--- Generowanie wykresów wizualnych dla zakresu: {date_range[0]} - {date_range[1]} ---"
    )
    print(f"--- Wykres zostanie zapisany w pliku: {output_html_file} ---")
    print(f"--- Okno wygładzania błędu: {smoothing_window} ---")

    # Sprawdzenie kolumn
    if forecast_col not in df.columns or actual_col not in df.columns:
        print(
            f"Błąd: Brak wymaganych kolumn '{forecast_col}' lub '{actual_col}' w DataFrame."
        )
        return

    # Filtrowanie danych do wizualizacji
    df_viz = df.dropna(subset=[forecast_col, actual_col]).copy()

    # Set Date_utc as index for easier filtering
    df_viz = df_viz.loc[date_range[0] : date_range[1]]

    if df_viz.empty:
        print("Brak danych w podanym zakresie dat.")
        return

    # Obliczenie błędu do wykresu
    df_viz["blad"] = df_viz[actual_col] - df_viz[forecast_col]

    # Tworzenie pod-wykresów (4 zamiast 3)
    fig = make_subplots(
        rows=4,
        cols=1,
        subplot_titles=(
            "Nakładanie się prognozy i danych faktycznych",
            "Wykres rozrzutu (Prognoza vs. Rzeczywistość)",
            "Histogram błędów (linia)",
            "Średni błąd prognozy (wygładzony)",
        ),
        vertical_spacing=0.06,
    )

    # Wykres 1: Nakładanie się liniowe
    fig.add_trace(
        go.Scatter(
            x=df_viz.index,
            y=df_viz[actual_col],
            name=actual_col,
            line=dict(color="blue"),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df_viz.index,
            y=df_viz[forecast_col],
            name=forecast_col,
            line=dict(color="red", dash="dash"),
        ),
        row=1,
        col=1,
    )
    fig.update_yaxes(title_text="Zapotrzebowanie [MW]", row=1, col=1)

    # Wykres 2: Wykres rozrzutu
    fig.add_trace(
        go.Scatter(
            x=df_viz[actual_col],
            y=df_viz[forecast_col],
            mode="markers",
            name="Punkty danych",
            opacity=0.6,
        ),
        row=2,
        col=1,
    )
    # Dodanie linii idealnej (y=x)
    min_val = min(df_viz[actual_col].min(), df_viz[forecast_col].min())
    max_val = max(df_viz[actual_col].max(), df_viz[forecast_col].max())
    fig.add_trace(
        go.Scatter(
            x=[min_val, max_val],
            y=[min_val, max_val],
            mode="lines",
            name="Linia idealna (Prognoza = Rzeczywistość)",
            line=dict(color="black", dash="dot"),
        ),
        row=2,
        col=1,
    )
    fig.update_xaxes(title_text=actual_col, row=2, col=1)
    fig.update_yaxes(title_text=forecast_col, row=2, col=1)

    # Wykres 3: Histogram błędów (linia zamiast słupków)
    hist, bin_edges = np.histogram(df_viz["blad"], bins=50)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2  # Środki binów
    fig.add_trace(
        go.Scatter(
            x=bin_centers,
            y=hist,
            mode="lines",
            name="Rozkład błędu (linia)",
            line=dict(color="purple"),
        ),
        row=3,
        col=1,
    )
    fig.add_vline(x=0, line_width=2, line_dash="dash", line_color="green", row=3, col=1)
    fig.update_xaxes(title_text="Błąd (Faktyczne - Prognoza) [MW]", row=3, col=1)
    fig.update_yaxes(title_text="Liczba wystąpień", row=3, col=1)

    # Wykres 4: Wygładzony błąd w czasie
    df_viz["blad_sredni"] = (
        df_viz["blad"].rolling(window=smoothing_window, min_periods=1).mean()
    )
    fig.add_trace(
        go.Scatter(
            x=df_viz.index,
            y=df_viz["blad_sredni"],
            name="Wygładzony błąd",
            line=dict(color="orange"),
        ),
        row=4,
        col=1,
    )
    fig.add_hline(y=0, line_width=2, line_dash="dash", line_color="green", row=4, col=1)
    fig.update_xaxes(title_text="Data", row=4, col=1)
    fig.update_yaxes(title_text="Średni błąd (Faktyczne - Prognoza) [MW]", row=4, col=1)

    fig.update_layout(
        height=1200,  # Zwiększona wysokość dla 4 wykresów
        title_text="Walidacja Wizualna Prognoz KSE",
        showlegend=True,
    )

    # Zapis do pliku HTML
    fig.write_html(output_html_file)
    print(f"Pomyślnie zapisano wykres w pliku: {output_html_file}")

    webbrowser.open(output_html_file)


def main(forecast_col: str, actual_col: str) -> None:
    out_path = Path(__file__).parent / "../out"
    pk = pd.read_parquet(out_path / "final.parquet")
    validate_forecasts(pk, forecast_col=forecast_col, actual_col=actual_col)

    # Visualization examples
    date_range = ("2024-10-01", "2024-11-01")
    visualize_data(
        pk,
        forecast_col=forecast_col,
        actual_col=actual_col,
        date_range=date_range,
        output_html_file=f"out/html/{actual_col}-OLD.html",
        smoothing_window="1D",
    )

    date_range = ("2025-09-01", "2025-10-01")
    visualize_data(
        pk,
        forecast_col=forecast_col,
        actual_col=actual_col,
        date_range=date_range,
        output_html_file=f"out/html/{actual_col}-NEW.html",
        smoothing_window="1D",
    )

    date_range = ("2024-01-01", date.today().strftime("%Y-%m-%d"))
    visualize_data(
        pk,
        forecast_col=forecast_col,
        actual_col=actual_col,
        date_range=date_range,
        output_html_file=f"out/html/{actual_col}-ALL.html",
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python pk5y_test.py <actual_column> <forecast_column>")
        print("Example: python pk5y_test.py pv_actual pv_forecast")
        sys.exit(1)

    actual_col = sys.argv[1]
    forecast_col = sys.argv[2]
    main(forecast_col, actual_col)
