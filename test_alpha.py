
from extract.finance_alpha_vantage import extract_alpha_vantage

df = extract_alpha_vantage("AAPL", function="RSI")
print(df.head())
