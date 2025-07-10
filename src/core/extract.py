import requests
from bs4 import BeautifulSoup
import json
import base64
import pandas as pd
from datetime import datetime
import time
import pytz

# A B3 usa um endpoint REST onde os parâmetros são passados como um objeto JSON codificado em Base64 na própria URL

def get_bovespa_data():
  # Parâmetros da requisição
  payload = {
      "language": "pt-br",
      "pageNumber": 1,
      "pageSize": 200,
      "index": "IBOV",
      "segment": "1"
  }

  # Codificando o JSON em base64
  json_str = json.dumps(payload, separators=(',', ':'))  # remove espaços extras
  encoded_params = base64.b64encode(json_str.encode()).decode()

  # URL final
  url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encoded_params}"

  # Fazendo a requisição
  response = requests.get(url)

  # Verificando o resultado
  if response.status_code == 200:
      data = response.json()
      results = data["results"]
      df = pd.DataFrame(results)

  else:
      print("Erro:", response.status_code)

  return df


def get_stream_bitcoin_data(minutes):
  dados = []
  url = "https://www.google.com/finance/quote/BTC-BRL"
  headers = {"User-Agent": "Mozilla/5.0"}

  # Define o fuso horário  (BRT)
  fuso = pytz.timezone('America/Sao_Paulo')

  for i in range(minutes):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # Buscando o elemento que contém o preço
    price_tag = soup.find("div", class_="YMlKec fxKbKc")  # classes específicas da página

    # Definindo resultados para o df
    if price_tag:
        tagValue = 'BRL'
        value = float(price_tag.text.replace("R$", "").replace(",", "").strip())
        dateTimeExtract = datetime.now(fuso)

        # adiciona os dados na lista
        dados.append({
            "tagValue": tagValue,
            "value": value,
            "dateTimeExtract": dateTimeExtract
            })
    else:
      print("Não foi possível encontrar o preço na página.")

    time.sleep(60) # Aguarda 1 Minuto para próxima extração

  df = pd.DataFrame(dados)

  return df
