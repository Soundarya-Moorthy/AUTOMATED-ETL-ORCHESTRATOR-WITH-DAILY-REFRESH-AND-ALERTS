
import pandas as pd
from config.db_config import get_engine

def load_staging_weather(df: pd.DataFrame):
    engine = get_engine()

    df.to_sql(
        name="weather_live",
        schema="staging",
        con=engine,
        if_exists="replace",
        index=False
    )

    print("staging.weather_live loaded successfully")
