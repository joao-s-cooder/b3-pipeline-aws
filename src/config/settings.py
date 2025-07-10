# Cofigurações para extrair dados da B3
B3_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
B3_DEFAULT_PAYLOAD = {
    "language": "pt-br",
    "pageNumber": 1,
    "pageSize": 200,
    "index": "IBOV",
    "segment": "1"
}

# Configurações para o scraper do Google Finance (Bitcoin)
GOOGLE_FINANCE_URL = "https://www.google.com/finance/quote/BTC-BRL"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}