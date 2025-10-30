import pandas as pd
import numpy as np
import datetime
import statsmodels.api as sm
from pathlib import Path
import argparse
from tqdm import tqdm

out_path = Path(__file__).parent / "../out"


### Function for creating weighted model
def create_weighted_model(
    df, predicted_value, col_x, end_train, train_days, weight_type
):
    """
    Create a weighted linear regression model using data from train_days before end_train.
    Weights are higher for more recent samples, based on weight_type.
    """
    # Calculate start of training period: train_days before end_train
    start_train = end_train - pd.Timedelta(days=train_days)

    # Filter dataframe to training period
    train_mask = (df.index >= start_train) & (df.index <= end_train)
    df_train = df[train_mask].copy()

    # Check if training data is empty
    if df_train.empty:
        return None

    # Calculate number of samples in training period
    n_samples = len(df_train)

    # Calculate weights for training samples
    if weight_type == "exp":
        weights = np.exp(-np.arange(n_samples)[::-1] / n_samples)
    elif weight_type == "linear":
        weights = np.arange(1, n_samples + 1) / n_samples
    elif weight_type == "none":
        weights = np.ones(n_samples)
    else:
        raise ValueError(f"Unknown weight_type: {weight_type}")

    df_train["weight"] = weights

    # Prepare X, y, and weights for the model
    X = sm.add_constant(df_train[col_x])
    y = df_train[predicted_value]
    weights = df_train["weight"]

    try:
        # Fit weighted least squares (WLS) model
        wls_model = sm.WLS(y, X, weights=weights)
        return wls_model.fit()
    except Exception as e:
        print(f"Error during model fitting: {e}")
        return None


def prepare_power_model_dataframe(power_data, predicted_value):
    """
    Create a copy of power_data to store model results.
    If predicted_value is 'spread', compute the spread column.
    """
    power_model = power_data.copy()
    if predicted_value == "spread":
        power_model["spread"] = (
            power_model["bilans_price"] - power_model["fixing1_price"]
        )
    power_model["model_prediction"] = np.nan
    power_model["model_profit"] = np.nan
    power_model["date"] = power_model.index.date
    power_model["hour"] = power_model.index.hour
    power_model = power_model.reset_index()
    power_model = power_model[power_model["date"] > datetime.date(2024, 10, 10)]
    power_model.set_index(["date"], inplace=True)
    return power_model


def process_dates(
    power_model, predicted_value, features_actual_forecast, train_days, weight_type
):
    """
    Process each date: train model, make predictions, calculate profits.
    """
    dates = power_model.index.unique()
    col_x = list(features_actual_forecast.keys())
    for date in tqdm(dates, desc="Processing dates"):
        # Set end of training period: 3 days before the current date
        end_train = date - pd.Timedelta(days=3)

        # Get model results using cleaned data (drop NaNs in features)
        result = create_weighted_model(
            power_model.dropna(subset=col_x),
            predicted_value,
            col_x,
            end_train,
            train_days,
            weight_type,
        )

        if result is not None:
            # Filter test data for the current date
            test_mask = power_model.index == date
            test_df = power_model[test_mask].copy()

            if not test_df.empty:
                # Calculate prediction using model parameters and actual features
                X_test_actual = sm.add_constant(test_df[col_x], has_constant="add")
                test_df["prediction"] = result.predict(X_test_actual)

                # Calculate prediction using forecasted features
                forecast_cols = [features_actual_forecast[f] for f in col_x]
                X_test_forecast = sm.add_constant(
                    test_df[forecast_cols], has_constant="add"
                )
                test_df["prediction_forecast"] = result.predict(X_test_forecast)

                for feature in col_x:
                    coef_col = f"coef_{feature}"
                    power_model.loc[test_mask, coef_col] = result.params[feature]
                power_model.loc[test_mask, "coef_const"] = result.params["const"]

                # Aggregate predictions by date and hour (mean)
                test_df["prediction"] = test_df.groupby(["date", "hour"])[
                    "prediction"
                ].transform("mean")
                test_df["prediction_forecast"] = test_df.groupby(["date", "hour"])[
                    "prediction_forecast"
                ].transform("mean")

                # Assign back to power_model
                power_model.loc[test_mask, "prediction"] = test_df["prediction"]
                power_model.loc[test_mask, "prediction_forecast"] = test_df[
                    "prediction_forecast"
                ]

    return power_model


def train(predicted_value, features_actual_forecast, train_days=90, weight_type="exp"):
    """
    Main function to run the entire process.
    """
    power_data = pd.read_parquet(out_path / "final.parquet")
    power_model = prepare_power_model_dataframe(power_data, predicted_value)
    power_model = process_dates(
        power_model, predicted_value, features_actual_forecast, train_days, weight_type
    )
    power_model = power_model.reset_index()
    power_model.set_index(["Date_utc"], inplace=True)
    return power_model


def calculate_stats(power_model, predicted_value):
    if predicted_value == "bilans_price":
        long_cond = power_model["prediction"] > power_model["fixing1_price"]
        long_cond_forecast = (
            power_model["prediction_forecast"] > power_model["fixing1_price"]
        )
    elif predicted_value == "spread":
        long_cond = power_model["prediction"] >= 0
        long_cond_forecast = power_model["prediction_forecast"] >= 0
    else:
        raise ValueError(f"Unknown predicted_value: {predicted_value}")

    # Calculate profit based on trading strategy
    power_model["profit"] = np.where(
        long_cond,
        power_model["bilans_price"] - power_model["fixing1_price"],  # Long position
        power_model["fixing1_price"] - power_model["bilans_price"],  # Short position
    )

    # Calculate profit for forecast-based prediction
    power_model["profit_forecast"] = np.where(
        long_cond_forecast,
        power_model["bilans_price"] - power_model["fixing1_price"],  # Long position
        power_model["fixing1_price"] - power_model["bilans_price"],  # Short position
    )

    # Assign predictions and profits back to power_model (profit divided by 4)
    power_model["model_prediction"] = power_model["prediction"]
    power_model["model_profit"] = power_model["profit"] / 4
    power_model["model_prediction_forecast"] = power_model["prediction_forecast"]
    power_model["model_profit_forecast"] = power_model["profit_forecast"] / 4

    power_model["error_actual"] = (
        power_model["model_prediction"] - power_model[predicted_value]
    )
    power_model["error_forecast"] = (
        power_model["model_prediction_forecast"] - power_model[predicted_value]
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run power model training and analysis."
    )
    parser.add_argument(
        "--train_days", type=int, default=90, help="Number of days for training window."
    )
    parser.add_argument(
        "--weight_type",
        choices=["linear", "exp", "none"],
        default="exp",
        help="Type of weighting for samples: linear, exp, or none.",
    )
    parser.add_argument(
        "--target",
        choices=["spread", "bilans_price"],
        default="bilans_price",
        help="Target to predict: spread or bilans_price.",
    )
    args = parser.parse_args()

    # Read features map from CSV in parent directory
    script_dir = Path(__file__).parent
    features_map_path = script_dir.parent / "features_map.csv"
    features_df = pd.read_csv(features_map_path)
    features_actual_forecast = dict(zip(features_df["actual"], features_df["forecast"]))

    predicted_value = args.target
    result = train(
        predicted_value,
        features_actual_forecast,
        train_days=args.train_days,
        weight_type=args.weight_type,
    )
    calculate_stats(result, predicted_value)
    result.to_parquet(
        out_path / "result.parquet",
        index=True,  # Preserve index assuming it's meaningful (e.g., datetime)
    )
