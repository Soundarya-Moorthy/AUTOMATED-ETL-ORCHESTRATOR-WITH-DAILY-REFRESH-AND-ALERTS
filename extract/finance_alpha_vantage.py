import requests
import pandas as pd
from config.config import ALPHA_VANTAGE_API_KEY, ALPHA_VANTAGE_BASE_URL

def extract_alpha_vantage(symbol, function="RSI", interval="daily"):
    params = {
        "function": function,
        "symbol": symbol,
        "interval": interval,
        "time_period": 14,
        "series_type": "close",
        "apikey": ALPHA_VANTAGE_API_KEY
    }

    response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params)
    data = response.json()
  
    if "Note" in data:
        raise Exception("Alpha Vantage API limit exceeded. Try after 1 minute.")

    if "Error Message" in data:
        raise Exception(f"Alpha Vantage error: {data['Error Message']}")

    tech_key = f"Technical Analysis: {function}"

    if tech_key not in data:
        raise Exception("Expected technical indicator not found in response")

    df = pd.DataFrame.from_dict(data[tech_key], orient="index")
    df.index = pd.to_datetime(df.index)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    df["symbol"] = symbol

    return df
