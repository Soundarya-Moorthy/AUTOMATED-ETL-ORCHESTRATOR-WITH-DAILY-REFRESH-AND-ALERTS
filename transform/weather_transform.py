
import pandas as pd

def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize weather data
    """

    df = df.copy()

    # Timestamp Standardization 
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.date

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date

    # Temperature Validation 
    if "temperature" in df.columns:
        df = df[df["temperature"].between(-50, 60)]
    elif "tavg" in df.columns:
        df = df[df["tavg"].between(-50, 60)]

    # Precipitation 
    if "prcp" in df.columns:
        df["prcp"] = df["prcp"].fillna(0)

    if "precipitation" in df.columns:
        df["precipitation"] = df["precipitation"].fillna(0)

    # Humidity 
    if "humidity" in df.columns:
        df["humidity"] = df["humidity"].fillna(method="ffill")

    # Wind 
    if "wind_speed" in df.columns:
        df["wind_speed"] = df["wind_speed"].fillna(0)

    # Remove Duplicates 
    df = df.drop_duplicates()

    return df
