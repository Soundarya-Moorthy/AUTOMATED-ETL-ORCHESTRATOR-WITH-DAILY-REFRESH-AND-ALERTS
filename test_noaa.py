from extract.weather_noaa import extract_noaa_weather

df = extract_noaa_weather()

print("NOAA Historical Weather Data:")
print(df.head())
print("\nColumns:", df.columns)
