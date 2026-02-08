import pandas as pd

def transform_finance_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate financial market data
    """

    df = df.copy()

    # Required Columns
    required_cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
    df = df[required_cols]

    # Datetime Standardization
    df["date"] = pd.to_datetime(df["date"])

    # Positive Price Validation
    df = df[
        (df["open"] > 0) &
        (df["high"] > 0) &
        (df["low"] > 0) &
        (df["close"] > 0)
    ]

    # Volume Validation
    df = df[df["volume"] >= 0]

    # Market Hours Check (Weekdays only)
    df = df[df["date"].dt.dayofweek < 5]

    # Standardize Ticker
    df["ticker"] = df["ticker"].str.upper().str.strip()

    # Remove Duplicates
    df = df.drop_duplicates(subset=["date", "ticker"])

    return df


def get_clean_finance_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prefect-compatible wrapper for finance transformation
    """
    return transform_finance_data(df)
