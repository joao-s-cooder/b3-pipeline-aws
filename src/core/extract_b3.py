import requests
import json
import base64
import pandas as pd
from src.config.settings import B3_BASE_URL, B3_DEFAULT_PAYLOAD

class B3DataExtractor:
    """
    Scraper para coletar dados da B3 (Bolsa de Valores do Brasil).
    """
    def __init__(self, payload: dict = None):
        self.payload = payload if payload else B3_DEFAULT_PAYLOAD
        self.base_url = B3_BASE_URL

    def _build_url(self) -> str:
        """
        Constrói a URL com os parâmetros codificados em Base64.
        """
        json_str = json.dumps(self.payload, separators=(',', ':'))
        encoded_params = base64.b64encode(json_str.encode()).decode()
        return f"{self.base_url}{encoded_params}"
    
    def get_data(self) -> pd.DataFrame:
        """
        Faz a requisição para a B3 e retorna os dados em um DataFrame.
        """
        url = self._build_url()
        try:
            response = requests.get(url, timeout=10) # Define timeout para evitar bloqueios
            response.raise_for_status()  # Lança um erro para códigos de status HTTP 4xx/5xx
            data = response.json()
            results = data.get("results")
            if results:
                return pd.DataFrame(results)
            else:
                print("Aviso: 'results' não encontrado na resposta.")
                return pd.DataFrame() # Retorna DataFrame vazio se não houver resultados
        except requests.RequestException as e:
            print(f"Erro ao acessar B3: {e}")
            return pd.DataFrame()
        