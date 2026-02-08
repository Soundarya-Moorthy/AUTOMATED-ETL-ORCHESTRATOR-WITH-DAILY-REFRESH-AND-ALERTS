
import pandas as pd
from config.db_config import get_engine
from transform.finance_transform import get_clean_finance_data
from sqlalchemy import text

def load_staging_finance(df: pd.DataFrame):
    engine = get_engine()
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.finance_prices"))

    df.to_sql(
        name="finance_prices",
        schema="staging",
        con=engine,
        if_exists="append",
        index=False
    )

    print("staging.finance_prices loaded successfully")

