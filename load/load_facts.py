
from sqlalchemy import text
from db.engine import engine

def load_fact_weather():
    query = text("""
        INSERT INTO warehouse.fact_weather (
            date_id,
            city_id,
            temperature,
            humidity,
            wind_speed,
            precipitation
        )
        SELECT
            wl.timestamp::date AS date_id,
            dc.city_id,
            wl.temperature,
            wl.humidity,
            wl.wind_speed,
            wl.precipitation
        FROM staging.weather_live wl
        JOIN warehouse.dim_city dc
            ON SPLIT_PART(wl.city, ',', 1) = dc.city_name
        ON CONFLICT DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    print("weather loaded successfully")

def load_fact_finance():
    query = text("""
        INSERT INTO warehouse.fact_finance (
            date_id,
            stock_id,
            open,
            high,
            low,
            close,
            volume
        )
        SELECT
            fl.date::date AS date_id,
            ds.stock_id,
            fl.open,
            fl.high,
            fl.low,
            fl.close,
            fl.volume
        FROM staging.finance_prices fl
        JOIN warehouse.dim_stock ds
          ON UPPER(TRIM(fl.ticker)) = UPPER(TRIM(ds.ticker))
        ON CONFLICT (date_id, stock_id) DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(query)

    print("finance loaded successfully")
