import pandas as pd
import numpy as np
from pathlib import Path
import argparse

out_path = Path(__file__).parent / "../out"


def print_results(result, charts=False):
    result = result.sort_index()
    # Calculate and print metrics
    results = result.dropna(subset=["model_profit"])
    print("---------------------------------------------------------------")
    print("ACTUALS:")
    print(f"Total trades: {len(results)}")
    print(f"Win rate: {(results['model_profit'] > 0).mean():.2%}")
    print(f"Total PnL: {results['model_profit'].sum():.2f}")
    print(f"PnL per trade: {results['model_profit'].mean():.2f}")
    mae_actual = np.abs(results["error_actual"]).mean()
    rmse_actual = np.sqrt((results["error_actual"] ** 2).mean())
    print(f"MAE: {mae_actual:.2f}")
    print(f"RMSE: {rmse_actual:.2f}")
    cum_pnl_actual = results["model_profit"].cumsum()
    drawdowns_actual = cum_pnl_actual - cum_pnl_actual.cummax()
    max_drawdown_actual = drawdowns_actual.min()
    print(f"Max Drawdown: {max_drawdown_actual:.2f}")

    results = result.dropna(subset=["model_profit_forecast"])
    print("---------------------------------------------------------------")
    print("FORECAST:")
    print(f"Total trades: {len(results)}")
    print(f"Win rate: {(results['model_profit_forecast'] > 0).mean():.2%}")
    print(f"Total PnL: {results['model_profit_forecast'].sum():.2f}")
    print(f"PnL per trade: {results['model_profit_forecast'].mean():.2f}")
    mae_forecast = np.abs(results["error_forecast"]).mean()
    rmse_forecast = np.sqrt((results["error_forecast"] ** 2).mean())
    print(f"MAE: {mae_forecast:.2f}")
    print(f"RMSE: {rmse_forecast:.2f}")
    cum_pnl_forecast = results["model_profit_forecast"].cumsum()
    drawdowns_forecast = cum_pnl_forecast - cum_pnl_forecast.cummax()
    max_drawdown_forecast = drawdowns_forecast.min()
    print(f"Max Drawdown: {max_drawdown_forecast:.2f}")

    if charts:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        import webbrowser

        # Compute daily and rolling RMSE for actual and forecast
        daily_rmse_actual = results.groupby(pd.Grouper(freq="D"))["error_actual"].apply(
            lambda x: np.sqrt(np.mean(x**2)) if not x.empty else np.nan
        )
        rolling_rmse_actual = daily_rmse_actual.rolling(14).mean()

        daily_rmse_forecast = results.groupby(pd.Grouper(freq="D"))[
            "error_forecast"
        ].apply(lambda x: np.sqrt(np.mean(x**2)) if not x.empty else np.nan)
        rolling_rmse_forecast = daily_rmse_forecast.rolling(14).mean()

        # Create a single subplot with secondary y-axis
        fig = make_subplots(
            rows=1,
            cols=1,
            specs=[[{"secondary_y": True}]],
            subplot_titles=["Combined Performance Chart"],
        )

        # Add Cumulative Profit traces to primary y-axis (left, shared scale for profits)
        fig.add_trace(
            go.Scatter(
                x=results.index,
                y=cum_pnl_actual,
                name="Actual Cum PnL",
                line=dict(color="blue", width=2),
                yaxis="y",
            ),
            row=1,
            col=1,
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=results.index,
                y=cum_pnl_forecast,
                name="Forecast Cum PnL",
                line=dict(color="red", width=2),
                yaxis="y",
            ),
            row=1,
            col=1,
            secondary_y=False,
        )

        # Add RMSE traces to secondary y-axis (right, shared scale for errors)
        fig.add_trace(
            go.Scatter(
                x=rolling_rmse_actual.index,
                y=rolling_rmse_actual,
                name="Actual RMSE",
                line=dict(color="blue", width=2, dash="dash"),
                yaxis="y2",
            ),
            row=1,
            col=1,
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=rolling_rmse_forecast.index,
                y=rolling_rmse_forecast,
                name="Forecast RMSE",
                line=dict(color="red", width=2, dash="dash"),
                yaxis="y2",
            ),
            row=1,
            col=1,
            secondary_y=True,
        )

        # Update axes
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(
            title_text="Cumulative PnL (Actual & Forecast)",
            row=1,
            col=1,
            secondary_y=False,
        )
        fig.update_yaxes(
            title_text="Rolling 14-day Mean RMSE (Actual & Forecast)",
            row=1,
            col=1,
            secondary_y=True,
            side="right",
        )

        # Update layout
        fig.update_layout(
            height=800,
            title_text="Interactive Combined Performance Chart",
            showlegend=True,
        )

        # Save to HTML and auto-open in browser
        output_file = "performance_charts.html"
        fig.write_html(output_file)
        webbrowser.open(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate model results.")
    parser.add_argument(
        "--charts", action="store_true", help="Generate and save charts."
    )
    args = parser.parse_args()
    result = pd.read_parquet(out_path / "result.parquet")
    print_results(result, charts=args.charts)
