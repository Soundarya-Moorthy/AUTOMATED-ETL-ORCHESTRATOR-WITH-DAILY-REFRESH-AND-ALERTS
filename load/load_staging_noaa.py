
import pandas as pd
from extract.weather_noaa import extract_noaa_weather
from config.db_config import engine

def load_staging_noaa():
    df = extract_noaa_weather()

    if df.empty:
        print("NOAA dataframe is empty")
        return

    df.to_sql(
        "weather_noaa",
        engine,
        schema="staging",
        if_exists="replace",
        index=False
    )

    print("staging.weather_noaa loaded successfully")


if __name__ == "__main__":
    load_staging_noaa()
