
import pandas as pd

def weather_market_correlation(weather_df: pd.DataFrame, finance_df: pd.DataFrame):
    """
    Correlation between weather temperature and stock closing prices
    """

    merged = weather_df.merge(
        finance_df,
        on="date",
        how="inner"
    )

    return merged[["temperature", "close"]].corr()
