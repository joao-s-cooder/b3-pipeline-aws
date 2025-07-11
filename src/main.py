from src.core.extract_b3 import B3DataExtractor
from src.core.extract_bitcoin import bicoinDataExtractor
from src.core.data_processor import DataProcessor

import pandas as pd
import os

def run_b3_extractor():
    """
    Função para executar a extração de dados da B3.
    """
    print("Iniciando a extração de dados da B3...")
    b3_extractor = B3DataExtractor()
    df_b3 = b3_extractor.get_data()

    if not df_b3.empty:
        print(f"Dados da B3 Coletados. Total de linhas: {len(df_b3)}")
        df_b3_cleaned = DataProcessor.clean_b3_data(df_b3)

        os.makedirs('data', exist_ok=True)
        output_path = os.path.join("data", f"b3_portifoliio_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet")
        df_b3_cleaned.to_parquet(output_path, index=False, compression='snappy')
        print(f"Dados da B3 limpos e salvos em '{output_path}'")
    else:
        print("Nenhum dado coletado da B3 ou ocorreu erro.")

def run_bitcoin_extractor(minutes: int = 1):
    """
    Executa a extração de dados do Bitcoin.
    """
    print(f"\nIniciando a extração de dados do Bitcoin por {minutes} minuto(s)...")
    bitcoin_extractor = bicoinDataExtractor()
    df_bitcoin = bitcoin_extractor.stream_data(minutes)

    if not df_bitcoin.empty:
        print(f"Dados do Bitcoin coletados. Total de linhas: {len(df_bitcoin)}")
        df_bitcoin_cleaned = DataProcessor.clean_bitcoin_data(df_bitcoin)

        os.makedirs('data', exist_ok=True)
        output_path = os.path.join(f"data", f"bitcoin_data_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet")
        df_bitcoin_cleaned.to_parquet(output_path, index=False, compression='snappy')
        print(f"Dados do Bitcoin limpos e salvos em '{output_path}'")
    else:
        print("Nenhum dado coletado do Bitcoin ou ocorreu erro.")

if __name__ == "__main__":
    run_b3_extractor()
    run_bitcoin_extractor(minutes=3)