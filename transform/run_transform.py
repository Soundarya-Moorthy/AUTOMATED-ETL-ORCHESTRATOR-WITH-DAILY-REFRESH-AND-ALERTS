
from transform.finance_transform import transform_finance_data
from transform.weather_transform import transform_weather_data
from transform.quality_checks import (
    check_data_freshness,
    check_nulls,
    check_duplicates,
    record_count_reconciliation
)

def run_finance_transform(raw_df):
    clean_df = transform_finance_data(raw_df)

    print("Finance Quality Checks:")
    print("Nulls:\n", check_nulls(clean_df))
    print("Duplicates:", check_duplicates(clean_df))
    print("Counts:", record_count_reconciliation(raw_df, clean_df))

    return clean_df


def run_weather_transform(raw_df, timestamp_col="timestamp"):
    clean_df = transform_weather_data(raw_df)

    print("Weather Quality Checks:")
    print("Nulls:\n", check_nulls(clean_df))
    print("Duplicates:", check_duplicates(clean_df))
    print("Freshness:", check_data_freshness(clean_df, timestamp_col))
    print("Counts:", record_count_reconciliation(raw_df, clean_df))

    return clean_df
