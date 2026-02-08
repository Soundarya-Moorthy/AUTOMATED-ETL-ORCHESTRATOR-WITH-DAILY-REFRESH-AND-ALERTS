from utils.retry import retry_request
from config.config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL
import pandas as pd
from datetime import datetime

def extract_openweather(city):
    params = {
        "q": city,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }

    data = retry_request(OPENWEATHER_BASE_URL, params)

    precipitation = 0.0
    if "rain" in data:
        precipitation = data["rain"].get("1h", 0.0)

    weather_record = {
        "city": data["name"],
        "country": data["sys"]["country"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "precipitation": precipitation,
        "timestamp": datetime.utcfromtimestamp(data["dt"])
    }

    return pd.DataFrame([weather_record])
