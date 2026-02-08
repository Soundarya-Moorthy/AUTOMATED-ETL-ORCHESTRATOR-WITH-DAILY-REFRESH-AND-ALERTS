
from extract.weather_openweather import extract_openweather
from extract.weather_noaa import extract_noaa_weather
from extract.finance_yahoo import extract_yahoo_finance

from transform.weather_transform import transform_weather_data
from transform.finance_transform import transform_finance_data
from transform.quality_checks import check_duplicates, check_nulls

# WEATHER
raw_weather = extract_noaa_weather()
clean_weather = transform_weather_data(raw_weather)

print("Weather Nulls:\n", check_nulls(clean_weather))
print("Weather Duplicates:", check_duplicates(clean_weather))

# FINANCE
raw_finance = extract_yahoo_finance(["AAPL"])
clean_finance = transform_finance_data(raw_finance)

print("Finance Nulls:\n", check_nulls(clean_finance))
print("Finance Duplicates:", check_duplicates(clean_finance))
