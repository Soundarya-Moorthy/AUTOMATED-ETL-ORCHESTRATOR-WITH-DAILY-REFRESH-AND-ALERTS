from prefect import flow, task
from datetime import datetime

# ---------- IMPORT YOUR EXISTING FUNCTIONS ----------
from extract.weather_openweather import extract_openweather
from extract.weather_noaa import extract_noaa_weather
from extract.finance_yahoo import extract_yahoo_finance

from transform.weather_transform import transform_weather_data
from transform.finance_transform import transform_finance_data
from transform.quality_checks import (
    check_data_freshness,
    check_duplicates,
    record_count_reconciliation
)

from load.load_staging_weather import load_staging_weather
from load.load_staging_finance import load_staging_finance
from load.run_load import run_warehouse_load

from transform.correlation import weather_market_correlation


# ---------- TASKS ----------

@task
def extract_weather_task():
    print("Extracting weather data...")
    noaa_df = extract_noaa_weather()
    return noaa_df


@task
def extract_finance_task():
    print("Extracting finance data...")
    return extract_yahoo_finance()


@task
def transform_weather_task(df):
    print("Transforming weather data...")
    return transform_weather_data(df)


@task
def transform_finance_task(df):
    print("Transforming finance data...")
    return transform_finance_data(df)


@task
def quality_checks_task(raw_df, clean_df, timestamp_col):
    print("Running data quality checks...")
    return {
        "fresh": check_data_freshness(clean_df, timestamp_col),
        "duplicates": check_duplicates(clean_df),
        "counts": record_count_reconciliation(raw_df, clean_df)
    }


@task
def load_staging_task(weather_df, finance_df):
    print("Loading staging tables...")
    load_staging_weather(weather_df)
    load_staging_finance(finance_df)


@task
def load_warehouse_task():
    print("Loading warehouse tables...")
    run_warehouse_load()


@task
def correlation_task(weather_df, finance_df):
    print("Running correlation analysis...")
    corr = weather_market_correlation(weather_df, finance_df)
    print(corr)
    return corr


# ---------- MAIN DAG / FLOW ----------

@flow(name="Automated ETL Orchestrator")
def etl_pipeline():
    print(f"ETL Pipeline started at {datetime.utcnow()}")

    # Extract
    raw_weather = extract_weather_task()
    raw_finance = extract_finance_task()

    # Transform
    clean_weather = transform_weather_task(raw_weather)
    clean_finance = transform_finance_task(raw_finance)

    # Quality Checks
    weather_qc = quality_checks_task(raw_weather, clean_weather, "date")
    finance_qc = quality_checks_task(raw_finance, clean_finance, "date")

    print("Weather QC:", weather_qc)
    print("Finance QC:", finance_qc)

    # Load
    load_staging_task(clean_weather, clean_finance)
    load_warehouse_task()

    # Correlation
    correlation_task(clean_weather, clean_finance)

    print("ETL Pipeline completed successfully")


if __name__ == "__main__":
    etl_pipeline()
