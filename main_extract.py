
from extract.weather_openweather import extract_openweather
from extract.weather_noaa import extract_noaa_weather
from extract.finance_yahoo import extract_yahoo_finance
from extract.finance_alpha_vantage import extract_alpha_vantage
from config.config import CITIES
import pandas as pd

if __name__ == "__main__":
    print("Starting Weather Extraction")
    all_weather = []
    for city in CITIES:
        print(f"Fetching weather for {city}")
        df = extract_openweather(city)
        all_weather.append(df)

    final_weather_df = pd.concat(all_weather, ignore_index=True)

    print("\n All Cities Weather Output:")
    print(final_weather_df)

    weather_hist = extract_noaa_weather()
    yahoo_data = extract_yahoo_finance()
    alpha_data = extract_alpha_vantage("AAPL")

    print("Weather (NOAA):", weather_hist.head())
    print("Yahoo Finance:", yahoo_data.head())
    print("Alpha Vantage:", alpha_data.head())
