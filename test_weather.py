
from extract.weather_openweather import extract_openweather
from config.config import CITIES
import pandas as pd

all_weather = []

for city in CITIES:
    print(f"Fetching weather for {city}")
    df = extract_openweather(city)
    all_weather.append(df)

final_weather_df = pd.concat(all_weather, ignore_index=True)

print("\n All Cities Weather Output:")
print(final_weather_df)
