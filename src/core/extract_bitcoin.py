import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import pytz
from src.config.settings import GOOGLE_FINANCE_URL, HEADERS


class bicoinDataExtractor:
    """
    Scraper para coletar dados do Bitcoin no Google Finance.
    """

    def __init__(self, url: str = GOOGLE_FINANCE_URL, HEADERS: dict = HEADERS):
        self.url = url
        self.headers = HEADERS
        self.timezone = pytz.timezone("America/Sao_Paulo") # Fuso horário de São Paulo

    def _get_current_price(self) -> dict:
        """
        Extrai o preço atual do Bitcoin da página do google finance.
        """
        try:
            response = requests.get(self.url, headers=self.headers, timeout=10)
            response.raise_for_status()  # Lança um erro para códigos de status HTTP 4xx/5xx
            soup = BeautifulSoup(response.text, "html.parser")

            price_tag = soup.find("div", class_="YMlKec fxKbKc")
            if price_tag:
                value_str = price_tag.text.replace("R$", "").replace(",", ".").strip()
                value = float(value_str)
                return {
                    "tagValue": "BRL",
                    "value": value,
                    "dateTimeExtract" : datetime.now(self.timezone)
                }
            else:
                print("Aviso: Preço do Bitcoin não encontrado na página.")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            return None
        except ValueError as e:
            print(f"Erro ao converter o valor do preço: {e}")
            return None
        
    def stream_data(self, minutes: int = 1) -> pd.DataFrame:
        """
        Coleta dados do Bitcoin a cada 'Minutes' minutos e retorna um DataFrame.
        """
        if not isinstance(minutes, int) or minutes <= 0:
            raise ValueError("O parâmetro 'minutes' deve ser um inteiro positivo.")
        
        dados = []
        print(f"Iniciando coleta de dados do Bitcoin a cada {minutes} minutos...")
        for i in range(minutes):
            data_point = self._get_current_price()
            if data_point:
                dados.append(data_point)
                print(f"coleta {i+1}/{minutes}: BTC/BRL = {data_point['value']:.2f} em {data_point['dateTimeExtract'].strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print(f"coleta {i+1}/{minutes}: Falha ao obter dados.")

            if i < minutes - 1: # Não espera o último no loop
                time.sleep(60) # Aguarda 1 Minuto para próxima extração
        
        return pd.DataFrame(dados) if dados else pd.DataFrame(columns=["tagValue", "value", "dateTimeExtract"])


        