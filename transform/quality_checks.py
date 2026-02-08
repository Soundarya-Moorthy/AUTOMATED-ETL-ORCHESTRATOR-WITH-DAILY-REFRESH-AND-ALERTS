
from datetime import datetime, timedelta
import pandas as pd

# Freshness Check 
def check_data_freshness(df, timestamp_col):
    latest = df[timestamp_col].max()

    if isinstance(latest, datetime):
        latest_dt = latest
    else:
        latest_dt = datetime.combine(latest, datetime.min.time())

    return (datetime.utcnow() - latest_dt) < timedelta(hours=24)


# Null Check 
def check_nulls(df: pd.DataFrame) -> pd.Series:
    """
    Returns null counts per column
    """
    return df.isnull().sum()


# Duplicate Check 
def check_duplicates(df: pd.DataFrame) -> int:
    """
    Returns duplicate record count
    """
    return df.duplicated().sum()


# Record Count Reconciliation 
def record_count_reconciliation(raw_df: pd.DataFrame, clean_df: pd.DataFrame) -> dict:
    """
    Compare source vs transformed row counts
    """
    return {
        "raw_count": len(raw_df),
        "clean_count": len(clean_df),
        "dropped_rows": len(raw_df) - len(clean_df)
    }


# Referential Integrity (Pre-load) 
def check_referential_integrity(fact_df, dim_df, fact_key, dim_key):
    """
    Ensure fact keys exist in dimension table
    """
    missing = fact_df[~fact_df[fact_key].isin(dim_df[dim_key])]
    return len(missing)
