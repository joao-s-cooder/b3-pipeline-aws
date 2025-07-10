from core.extract import get_bovespa_data

df = get_bovespa_data()
print(df.head())