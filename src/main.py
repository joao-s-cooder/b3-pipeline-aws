from src.core.extract_b3 import B3DataExtractor
from src.core.extract_bitcoin import bicoinDataExtractor
from src.core.data_processor import DataProcessor
from src.utils.s3_uploader import S3UpLoader
from src.config.s3_config import S3_BUCKET_NAME, S3_REGION_NAME, S3_UPLOAD_PREFIX_B3, S3_UPLOAD_PREFIX_BITCOIN
import pandas as pd
import os
from datetime import datetime

s3_uploader = S3UpLoader(bucket_name=S3_BUCKET_NAME, region_name=S3_REGION_NAME)

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

        # Gerar o nome do arquivo
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"b3_portifoliio_{timestamp}.parquet"
        output_path = os.path.join("data", file_name)

        os.makedirs('data', exist_ok=True)
        df_b3_cleaned.to_parquet(output_path, index=False, compression='snappy')
        print(f"Dados da B3 extraidos e salvos em '{output_path}'")

        # Enviar para o S3
        s3_object_name = f"{S3_UPLOAD_PREFIX_B3}{file_name}"
        s3_uploader.upload_file(output_path, s3_object_name)
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

        #Gerar o nome do arquivo
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"bitcoin_data_{timestamp}.parquet"
        output_path = os.path.join("data", file_name)

        os.makedirs('data', exist_ok=True)
        df_bitcoin_cleaned.to_parquet(output_path, index=False, compression='snappy')
        print(f"Dados do Bitcoin limpos e salvos em '{output_path}'")

        # Enviar para o S3
        s3_object_name = f"{S3_UPLOAD_PREFIX_BITCOIN}{file_name}"
        s3_uploader.upload_file(output_path, s3_object_name)
    else:
        print("Nenhum dado coletado do Bitcoin ou ocorreu erro.")



if __name__ == "__main__":
    run_b3_extractor()
    run_bitcoin_extractor(minutes=3)