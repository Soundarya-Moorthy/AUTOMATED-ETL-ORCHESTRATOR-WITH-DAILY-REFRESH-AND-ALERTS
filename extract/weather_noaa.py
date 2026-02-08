import pandas as pd
from config.config import NOAA_DATA_PATH

def extract_noaa_weather():
    """
    Extract historical weather data from NOAA CSV file
    """

    try:
        df = pd.read_csv(NOAA_DATA_PATH)

        # Standardize column names
        df.columns = df.columns.str.lower()

        # Rename columns to match pipeline expectations
        df = df.rename(columns={
            "name": "station_name",
            "prcp": "precipitation",
            "tmax": "tmax",
            "tmin": "tmin",
            "tavg": "temperature"
        })

        # Convert date
        df["date"] = pd.to_datetime(df["date"])

        # Keep only required columns
        df = df[
            [
                "date",
                "station",
                "station_name",
                "latitude",
                "longitude",
                "temperature",
                "tmax",
                "tmin",
                "precipitation"
            ]
        ]

        return df

    except Exception as e:
        raise Exception(f"NOAA Extract Failed: {str(e)}")
