import pandas as pd

class DataProcessor:
    """
    Classe para processar e limpar os dados coletados pelos extratores.
    """
    @staticmethod
    def clean_b3_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Função para limpar dados da B3.
        """
        if df.empty:
            return df
        #Renomear colunas, converter tipos
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        return df

    @staticmethod
    def clean_bitcoin_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Função para limpar dados do Bitcoin.
        """
        if df.empty:
            return df
        #Garantir que 'value' é numérico
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df.dropna(subset=['value'], inplace=True)
        return df