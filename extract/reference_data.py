
import pandas as pd

def extract_holidays():
    return pd.read_csv("data/holidays.csv")

def extract_tickers():
    return pd.read_csv("data/tickers.csv")
