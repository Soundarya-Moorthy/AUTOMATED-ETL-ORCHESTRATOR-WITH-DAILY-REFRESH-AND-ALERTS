
import pandas as pd
from config.db_config import engine

def load_dim_date():
    query = """
        INSERT INTO warehouse.dim_date (date_id, year, month, day, weekday)
        SELECT DISTINCT
            d::date,
            EXTRACT(YEAR FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(DAY FROM d),
            TO_CHAR(d, 'Day')
        FROM (
            SELECT date AS d FROM staging.weather_noaa
            UNION
            SELECT timestamp::date FROM staging.weather_live
            UNION
            SELECT date FROM staging.finance_prices
        ) dates
        ON CONFLICT (date_id) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(query)


def load_dim_city():
    query = """
        INSERT INTO warehouse.dim_city (city_name, country)
        SELECT DISTINCT
            SPLIT_PART(city, ',', 1),
            SPLIT_PART(city, ',', 2)
        FROM staging.weather_live
        ON CONFLICT DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(query)


def load_dim_stock():
    query = """
        INSERT INTO warehouse.dim_stock (ticker)
        SELECT DISTINCT ticker
        FROM staging.finance_prices
        ON CONFLICT (ticker) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(query)
