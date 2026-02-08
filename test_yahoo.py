
from extract.finance_yahoo import extract_yahoo_finance

df = extract_yahoo_finance(
    tickers=["AAPL", "^GSPC", "^IXIC"],
    start="2025-01-01"
)

print(df.head())
