import pandas as pd
from sqlalchemy import text
from config.db_config import get_engine


def load_dim_date():
    engine = get_engine()
    from sqlalchemy import text
from config.db_config import get_engine

def load_dim_date():
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.dim_date (date_id, year, month, day, weekday)
            SELECT DISTINCT
                date::DATE                           AS date_id,
                EXTRACT(YEAR FROM date)::INT         AS year,
                EXTRACT(MONTH FROM date)::INT        AS month,
                EXTRACT(DAY FROM date)::INT          AS day,
                EXTRACT(DOW FROM date)::INT          AS weekday
            FROM staging.weather_live
            ON CONFLICT (date_id) DO NOTHING;
        """))

    print("dim_date loaded successfully")

def load_dim_city():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO warehouse.dim_city (city_name, country)
        SELECT DISTINCT
            city As city_name,
            'UNKNOWN' AS country
        FROM staging.weather_live
        WHERE city IS NOT NULL
        ON CONFLICT (city_name, country) DO NOTHING;
    """))
    print("dim_city loaded successfully.")

def load_dim_stock():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO warehouse.dim_stock (ticker)
        SELECT DISTINCT ticker
        FROM staging.finance_prices
        ON CONFLICT (ticker) DO NOTHING;
    """))
    print("dim_stock loaded successfully.")
