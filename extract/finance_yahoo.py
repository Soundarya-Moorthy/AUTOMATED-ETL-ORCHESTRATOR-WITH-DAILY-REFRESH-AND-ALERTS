
import yfinance as yf
import pandas as pd
from config.config import STOCK_TICKERS

def extract_yahoo_finance(
    tickers=["AAPL", "MSFT"],
    start="2025-01-01",
    end=None
):
    """
    Extract stock OHLCV data from Yahoo Finance
    """

    data = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        group_by="ticker",
        auto_adjust=False
    )

    df = data.stack(level=0, future_stack=True).reset_index()
    df.columns = [
        "date", "ticker",
        "open", "high", "low", "close",
        "adj_close", "volume"
    ]

    return df

