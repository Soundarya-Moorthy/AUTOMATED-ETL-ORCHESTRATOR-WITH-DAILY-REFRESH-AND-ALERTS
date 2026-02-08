

import os
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

CITIES = [
    "Bangalore,IN",
    "Tiruchirappalli,IN",
    "Delhi,IN",
    "Mumbai,IN",
    "Hyderabad,IN",
    "Kolkata,IN",
    "Chennai,IN",
    "New York,US",
    "London,GB"
]

NOAA_DATA_PATH = "data/noaa_weather.csv"


ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

STOCK_TICKERS = ["AAPL", "MSFT", "GOOGL"]
