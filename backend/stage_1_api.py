import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Configurações iniciais
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
DATA_DIR = "../data/raw"


def get_last_three_quarters():
    """Identifica as URLs dos últimos 3 trimestres disponíveis."""
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, 'html.parser')  
    # Encontra todos os anos (links que terminam com / e tem 4 dígitos)
    years = sorted(
        [
            link.get('href') for link in soup.find_all('a') 
            if link.get('href').rstrip('/').isdigit()
        ],
        reverse=True
    )
    quarters_urls = []
    for year in years:
        year_url = urljoin(BASE_URL, year)
        res_year = requests.get(year_url)
        soup_year = BeautifulSoup(res_year.text, 'html.parser')  
        # Encontra trimestres (1T, 2T, etc)
        quarters = sorted(
            [
                link.get('href') for link in soup_year.find_all('a') 
                if 'T' in link.get('href').upper()
            ],
            reverse=True
        )
        for q in quarters:
            quarters_urls.append(urljoin(year_url, q))
            if len(quarters_urls) == 3:
                return quarters_urls
    return quarters_urls


def download_files(urls):
    """Baixa os arquivos ZIP de cada trimestre identificado."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    for url in urls:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        zip_files = [
            link.get('href') for link in soup.find_all('a')
            if link.get('href').endswith('.zip')
        ]
        for file_name in zip_files:
            file_url = urljoin(url, file_name)
            print(f"Baixando: {file_name}...")        
            file_path = os.path.join(DATA_DIR, file_name)
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)


if __name__ == "__main__":
    print("Iniciando busca pelos trimestres...")
    targets = get_last_three_quarters()
    print(f"Trimestres encontrados: {targets}")
    download_files(targets)
    print("Download concluído com sucesso!")